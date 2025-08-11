import pytest
from unittest.mock import patch, MagicMock
from mwaa.celery.sqs_broker import Channel


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
    def test_health_monitoring_disabled_calls_super(self, mock_channel, env_value):
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

    @pytest.mark.parametrize("routing_key", ['worker.heartbeat', 'other.routing.key'])
    def test_celeryev_exchange_blocks_super(self, mock_channel, routing_key):
        """When health monitoring is enabled and exchange is celeryev, should not call super."""
        message = {'test': 'message'}
        kwargs = {'extra': 'args'}
        exchange = 'celeryev'

        with patch('mwaa.celery.sqs_broker.os.environ.get') as mock_env_get:
            mock_env_get.return_value = 'true'

            mock_channel.basic_publish(message, exchange, routing_key, **kwargs)

            mock_channel._super_basic_publish.assert_not_called()

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

    def test_size_with_attributes(self, mock_channel):
        """Test _size returns message count when Attributes exist."""
        queue = 'test-queue'
        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.return_value = {
            'Attributes': {'ApproximateNumberOfMessages': '5'}
        }
        
        with patch.object(mock_channel, '_new_queue', return_value='queue-url'), \
             patch.object(mock_channel, 'canonical_queue_name', return_value=queue), \
             patch.object(mock_channel, 'sqs', return_value=mock_sqs):
            
            result = mock_channel._size(queue)
            
            assert result == 5
            mock_sqs.get_queue_attributes.assert_called_once_with(
                QueueUrl='queue-url', AttributeNames=['ApproximateNumberOfMessages']
            )

    def test_size_without_attributes(self, mock_channel):
        """Test _size raises exception when Attributes missing."""
        queue = 'test-queue'
        mock_sqs = MagicMock()
        mock_sqs.get_queue_attributes.return_value = {}
        
        with patch.object(mock_channel, '_new_queue', return_value='queue-url'), \
             patch.object(mock_channel, 'canonical_queue_name', return_value=queue), \
             patch.object(mock_channel, 'sqs', return_value=mock_sqs), \
             patch('mwaa.celery.sqs_broker.logger') as mock_logger:
            
            with pytest.raises(KeyError):
                mock_channel._size(queue)
            
            mock_logger.error.assert_called_once()