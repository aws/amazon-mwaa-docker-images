import pytest
from unittest.mock import patch, MagicMock
from mwaa.celery.sqs_broker import Channel, CELERY_WORKER_TASK_LIMIT


class TestChannelBasicPublish:

    @pytest.fixture
    def mock_channel(self):
        mock_connection = MagicMock()

        mock_connection.channel_max = 65535
        mock_connection._used_channel_ids = []
        mock_connection.channels = []
        mock_connection.state = MagicMock()

        mock_connection.client.transport_options = {
            'predefined_queues': {},
            'queue_name_prefix': '',
            'visibility_timeout': 1800,
            'wait_time_seconds': 10,
            'region': 'us-east-1'
        }
        mock_connection.client.virtual_host = '/'

        with patch('mwaa.celery.sqs_broker.boto3'), \
             patch('mwaa.celery.sqs_broker.shared_memory.SharedMemory') as mock_shared_mem, \
             patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env, \
             patch('mwaa.celery.sqs_broker.get_event_loop'):

            mock_env.side_effect = lambda key, default=None: {
                'MWAA__CORE__TASK_MONITORING_ENABLED': 'false',
                'AIRFLOW_ENV_ID': 'test',
                'AIRFLOW__CELERY__WORKER_AUTOSCALE': '20,20',
                'AIRFLOW__MWAA__TEST_ABANDONED_SQS_MESSAGE_SCENARIOS': 'false',
                'AIRFLOW__MWAA__TEST_UNDEAD_PROCESS_SCENARIOS': 'false'
            }.get(key, default)

            mock_shared_mem.return_value = MagicMock()

            channel = Channel(connection=mock_connection)

            with patch.object(channel.__class__.__bases__[0], 'basic_publish') as mock_super_publish:
                channel._super_basic_publish = mock_super_publish
                yield channel

    @pytest.mark.parametrize("env_value", ['false', None])
    @patch('mwaa.celery.sqs_broker.Stats')
    def test_health_monitoring_disabled_calls_super(self, mock_stats, mock_channel, env_value):
        """When health monitoring is disabled or not set, should always call super."""
        message = {'test': 'message'}
        kwargs = {'extra': 'args'}
        exchange = 'celeryev'
        routing_key = 'worker.heartbeat'

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = env_value

            mock_channel.basic_publish(message, exchange, routing_key, **kwargs)

            mock_channel._super_basic_publish.assert_called_once_with(
                message, exchange, routing_key, **kwargs
            )

            mock_stats.gauge.assert_not_called()

    @pytest.mark.parametrize("routing_key", ['worker.heartbeat', 'other.routing.key'])
    @patch('mwaa.celery.sqs_broker.Stats')
    def test_celeryev_exchange_blocks_super(self, mock_stats, mock_channel, routing_key):
        """When health monitoring is enabled and exchange is celeryev, should not call super."""
        message = {'test': 'message'}
        kwargs = {'extra': 'args'}
        exchange = 'celeryev'

        mock_channel._get_tasks_from_state = MagicMock(return_value=[{'task': 'test'}] * 15)
        mock_channel._is_task_consumption_paused = MagicMock(return_value=False)
        mock_channel.idle_worker_monitoring_enabled = True

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = 'true'

            mock_channel.basic_publish(message, exchange, routing_key, **kwargs)

            mock_channel._super_basic_publish.assert_not_called()

            if routing_key == 'worker.heartbeat':
                mock_stats.gauge.assert_any_call("mwaa.celery.process.heartbeat", 1)
                mock_stats.gauge.assert_any_call("mwaa.celery.at_max_concurrency", 15)
                assert not any(call[0][0] == "mwaa.celery.sqs.consumption_paused"
                              for call in mock_stats.gauge.call_args_list)
            else:
                # For other routing keys, no metrics should be called
                mock_stats.gauge.assert_not_called()

    @pytest.mark.parametrize("exchange,routing_key", [
        ('other_exchange', 'worker.heartbeat'),
        ('task_exchange', 'task.routing.key')
    ])
    def test_non_celeryev_exchange_calls_super(self, mock_channel, exchange, routing_key):
        """When health monitoring is enabled but exchange is not celeryev, should call super."""
        message = {'test': 'message'}
        kwargs = {'extra': 'args'}

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = 'true'

            mock_channel.basic_publish(message, exchange, routing_key, **kwargs)

            mock_channel._super_basic_publish.assert_called_once_with(
                message, exchange, routing_key, **kwargs
            )

    def test_celery_worker_task_limit_constant_parsing(self):
        """Test CELERY_WORKER_TASK_LIMIT correctly parses environment variable."""
        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = '20,10'

            # Re-import to trigger constant re-evaluation
            import importlib
            import mwaa.celery.sqs_broker
            importlib.reload(mwaa.celery.sqs_broker)

            from mwaa.celery.sqs_broker import CELERY_WORKER_TASK_LIMIT
            assert CELERY_WORKER_TASK_LIMIT == 20

    @pytest.mark.parametrize("monitoring_enabled,expected_tasks", [
        (True, 15),   # When monitoring enabled, use actual task count
        (False, 20),  # When monitoring disabled, use CELERY_WORKER_TASK_LIMIT
    ])
    @patch('mwaa.celery.sqs_broker.Stats')
    def test_worker_heartbeat_active_tasks_calculation(self, mock_stats, mock_channel, monitoring_enabled, expected_tasks):
        """Test num_active_tasks calculation logic."""
        message = {'test': 'message'}
        exchange = 'celeryev'
        routing_key = 'worker.heartbeat'

        mock_channel._get_tasks_from_state = MagicMock(return_value=[{'task': 'test'}] * 15)
        mock_channel._is_task_consumption_paused = MagicMock(return_value=False)
        mock_channel.idle_worker_monitoring_enabled = monitoring_enabled

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = 'true'

            mock_channel.basic_publish(message, exchange, routing_key)

            if expected_tasks >= 20:  # CELERY_WORKER_TASK_LIMIT
                mock_stats.gauge.assert_any_call("mwaa.celery.at_max_concurrency", expected_tasks)
            else:
                assert not any(call[0][0] == "mwaa.celery.at_max_concurrency"
                              for call in mock_stats.gauge.call_args_list)

    @pytest.mark.parametrize("num_tasks,is_paused,expect_max_concurrency,expect_consumption_paused", [
        (25, False, True, False),   # Above limit, not paused
        (15, True, False, True),    # Below limit, paused
        (20, False, True, False),   # At limit, not paused
        (10, False, False, False),  # Below limit, not paused
    ])
    @patch('mwaa.celery.sqs_broker.Stats')
    def test_worker_heartbeat_conditional_metrics(self, mock_stats, mock_channel, num_tasks, is_paused, expect_max_concurrency, expect_consumption_paused):
        """Test conditional metric emission logic."""
        message = {'test': 'message'}
        exchange = 'celeryev'
        routing_key = 'worker.heartbeat'

        mock_channel._get_tasks_from_state = MagicMock(return_value=[{'task': 'test'}] * num_tasks)
        mock_channel._is_task_consumption_paused = MagicMock(return_value=is_paused)
        mock_channel.idle_worker_monitoring_enabled = True

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = 'true'

            mock_channel.basic_publish(message, exchange, routing_key)

            mock_stats.gauge.assert_any_call("mwaa.celery.process.heartbeat", 1)

            if expect_max_concurrency:
                mock_stats.gauge.assert_any_call("mwaa.celery.at_max_concurrency", num_tasks)
            else:
                assert not any(call[0][0] == "mwaa.celery.at_max_concurrency"
                              for call in mock_stats.gauge.call_args_list)

            if expect_consumption_paused:
                mock_stats.gauge.assert_any_call("mwaa.celery.sqs.consumption_paused", 1)
            else:
                assert not any(call[0][0] == "mwaa.celery.sqs.consumption_paused"
                              for call in mock_stats.gauge.call_args_list)
