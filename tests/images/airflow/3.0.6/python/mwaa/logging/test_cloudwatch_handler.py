import json
import logging
import pytest
import os
import importlib
import types
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock, MagicMock, ANY

from mwaa.config.setup_environment import (
    setup_environment_variables,
    _execute_startup_script,
    _export_env_variables,
    _is_protected_os_environ,
)
from mwaa.subprocess.subprocess import Subprocess
from mwaa.logging.cloudwatch_handlers import (
    BaseLogHandler,
    CloudWatchRemoteTaskLogger,
    SubprocessLogHandler,
    DagProcessorManagerLogHandler,
    DagProcessingLogHandler
)

print(BaseLogHandler.__init__.__code__.co_varnames)

@pytest.fixture
def mock_handler():
    return Mock()

@pytest.fixture
def mock_boto3_client():
    with patch('boto3.client') as mock:
        yield mock

@pytest.fixture
def mock_watchtower():
    with patch('mwaa.logging.cloudwatch_handlers.watchtower.CloudWatchLogHandler') as mock:
        yield mock

@pytest.fixture
def mock_fluent():
    with patch('mwaa.logging.fork_safe_handler.ForkSafeFluentHandler') as mock:
        yield mock

@pytest.fixture(autouse=True)
def reload_module():
    import importlib
    import mwaa.logging.cloudwatch_handlers
    importlib.reload(mwaa.logging.cloudwatch_handlers)

@pytest.fixture
def base_logger(mock_handler):
    logger = BaseLogHandler(
        log_group_arn="arn:aws:logs:region:account:log-group:test",
        kms_key_arn="arn:aws:kms:region:account:key/test",
        enabled=True
    )
    logger.handler = mock_handler
    logger.stats = Mock()
    return logger



def test_emit_skips_deprecated_metric_message(base_logger):  # updated parameter name
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="The basic metric validator will be deprecated",
        args=(),
        exc_info=None
    )

    base_logger.emit(record)
    base_logger.handler.emit.assert_not_called()


def test_emit_handles_normal_message(base_logger):  # updated parameter name
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Normal message",
        args=(),
        exc_info=None
    )

    with patch.dict(os.environ, {'MWAA__LOGGING__AIRFLOW_TEST_LOG_LEVEL': 'INFO'}):
        base_logger.emit(record)
        base_logger.handler.emit.assert_called_once_with(record)


def test_emit_respects_log_level(base_logger):
    record = logging.LogRecord(
        name="test",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="Debug message",
        args=(),
        exc_info=None
    )

    with patch.dict(os.environ, {'MWAA__LOGGING__AIRFLOW_TEST_LOG_LEVEL': 'INFO'}):
        base_logger.emit(record)
        base_logger.handler.emit.assert_not_called()


def test_emit_with_no_handler():
    logger = BaseLogHandler(
        log_group_arn="arn:aws:logs:region:account:log-group:test",
        kms_key_arn="arn:aws:kms:region:account:key/test",
        enabled=True
    )
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None
    )

    try:
        logger.emit(record)
        assert True  # Test passes if no exception is raised
    except Exception as e:
        assert False, f"emit() raised an exception {e} when handler is not set"

def test_emit_handles_exception(base_logger):
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None
    )

    base_logger.handler.emit.side_effect = Exception("Simulated error")

    base_logger.logs_source = "TEST"  # Make sure this is set

    with patch.object(BaseLogHandler, '_report_logging_error') as mock_report_error:
        base_logger.emit(record)

        # Verify that stats.incr was called with the correct error metric
        base_logger.stats.incr.assert_called_once_with("mwaa.logging.TEST.emit_error", 1)

        # Verify that _report_logging_error was called with the correct message
        mock_report_error.assert_called_once_with("Failed to emit log record.")

@pytest.mark.parametrize("use_non_critical_logging, expected_handler, unexpected_handler", [
    ('false', 'watchtower', 'fluent'),
    ('true', 'fluent', 'watchtower')
])
def test_log_handler_creation(mock_boto3_client, mock_watchtower, mock_fluent, use_non_critical_logging, expected_handler, unexpected_handler):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': use_non_critical_logging}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = BaseLogHandler('arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)
        handler.create_cloudwatch_handler('test_stream', 'test_source')

        assert handler.handler is not None, "No handler was created"

        if expected_handler == 'watchtower':
            assert isinstance(handler.handler, mock_watchtower.return_value.__class__), "Created handler should be a Watchtower handler"
            assert mock_watchtower.called, f"{expected_handler} handler should be created"
            assert not mock_fluent.called, f"{unexpected_handler} handler should not be created"
            mock_watchtower.assert_called_once_with(
                log_group_name=handler.log_group_name,
                log_stream_name='test_stream',
                boto3_client=mock_boto3_client.return_value,
                use_queues=True,
                send_interval=10,
                create_log_group=False
            )
        else:
            assert isinstance(handler.handler, mock_fluent.return_value.__class__), "Created handler should be a Fluent handler"
            assert mock_fluent.called, f"{expected_handler} handler should be created"
            assert not mock_watchtower.called, f"{unexpected_handler} handler should not be created"
            mock_fluent.assert_called_once_with(
                'customer.logs',
                host=ANY,
                port=24224,
                queue_maxsize=50000,
                queue_circular=True,
            )

def test_cloudwatch_remote_task_logger_initialization(mock_boto3_client):
    """Test CloudWatchRemoteTaskLogger initialization."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    assert logger.enabled is True
    assert logger.log_level == logging.INFO
    assert logger.log_group_name == 'test-Task'
    assert logger.region_name == 'us-west-2'
    assert logger.handler is None  # Handler is lazy-initialized


def test_cloudwatch_remote_task_logger_get_handler(mock_boto3_client, mock_watchtower):
    """Test CloudWatchRemoteTaskLogger handler initialization."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    handler = logger.get_handler()
    
    assert handler is not None
    assert mock_watchtower.called
    mock_watchtower.assert_called_once_with(
        log_group_name='test-Task',
        boto3_client=mock_boto3_client.return_value,
        use_queues=True,
        create_log_group=False,
    )
    
    # Should return same handler on subsequent calls (cached)
    handler2 = logger.get_handler()
    assert handler is handler2


def test_cloudwatch_remote_task_logger_processors_property(mock_boto3_client, mock_watchtower):
    """Test CloudWatchRemoteTaskLogger processors property returns tuple."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    processors = logger.processors
    
    assert isinstance(processors, tuple)
    assert len(processors) == 1
    assert callable(processors[0])


def test_cloudwatch_remote_task_logger_emit_is_noop():
    """Test that emit() is a no-op for CloudWatchRemoteTaskLogger."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None
    )
    
    # Should not raise any exception
    result = logger.emit(record)
    assert result is None


def test_cloudwatch_remote_task_logger_upload_is_noop():
    """Test that upload() is a no-op for CloudWatchRemoteTaskLogger."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    logger.handler = Mock()
    
    ti = MagicMock()
    
    # Should not raise any exception
    result = logger.upload('/tmp/test.log', ti)
    assert result is None
    logger.handler.flush.assert_called_once()


def test_cloudwatch_remote_task_logger_read(mock_boto3_client):
    """Test CloudWatchRemoteTaskLogger read() method."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        mock_hook = Mock()
        mock_hook_class.return_value = mock_hook
        
        # Mock log events
        mock_hook.get_log_events.return_value = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "test message", "level": "info"}'
            }
        ]
        
        logger = CloudWatchRemoteTaskLogger(
            log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
            kms_key_arn=None,
            enabled=True,
            log_level='INFO'
        )
        
        # Mock task instance
        ti = MagicMock()
        ti.dag_id = 'test_dag'
        ti.task_id = 'test_task'
        ti.run_id = 'test_run'
        ti.try_number = 1
        ti.end_date = None
        
        # Mock get_dagrun
        mock_dag_run = MagicMock()
        mock_dag_run.logical_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_dag_run.run_after = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_dag_run.data_interval_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_dag_run.data_interval_end = datetime(2024, 1, 2, tzinfo=timezone.utc)
        
        # Mock get_log_template
        mock_log_template = MagicMock()
        mock_log_template.filename = "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
        mock_dag_run.get_log_template.return_value = mock_log_template
        
        ti.get_dagrun.return_value = mock_dag_run
        
        messages, metadata = logger.read(ti, 1)
        
        assert len(messages) >= 1
        assert any('Reading remote log from Cloudwatch' in msg for msg in messages if isinstance(msg, str))


def test_cloudwatch_remote_task_logger_event_to_dict_with_json():
    """Test _event_to_dict with JSON message."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    event = {
        'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
        'message': '{"event": "test", "level": "info"}'
    }
    
    result = logger._event_to_dict(event)
    
    assert 'timestamp' in result
    assert result['event'] == 'test'
    assert result['level'] == 'info'


def test_cloudwatch_remote_task_logger_event_to_dict_with_plain_text():
    """Test _event_to_dict with plain text message."""
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    
    event = {
        'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
        'message': 'plain text message'
    }
    
    result = logger._event_to_dict(event)
    
    assert 'timestamp' in result
    assert result['event'] == 'plain text message'


def test_cloudwatch_remote_task_logger_ignored_patterns():
    """Test that IGNORED_PATTERNS filters out dag_processor logs."""
    import re
    
    # Test dag_processor pattern
    dag_processor_stream = "dag_processor/2025-01-01/dags-folder/dag.py.log"
    assert any(re.match(p, dag_processor_stream) for p in CloudWatchRemoteTaskLogger.IGNORED_PATTERNS)
    
    # Test that regular task log stream is not filtered
    task_stream = "dag_id=test/run_id=test/task_id=test/attempt=1.log"
    assert not any(re.match(p, task_stream) for p in CloudWatchRemoteTaskLogger.IGNORED_PATTERNS)


def test_subprocess_log_handler_with_fluent(mock_boto3_client, mock_fluent):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = SubprocessLogHandler(
            'arn:aws:logs:us-west-2:123456789012:log-group:test',
            None,
            'test_prefix',
            'test_source',
            True,
            log_formatter=logging.Formatter('%(message)s')
        )

        assert mock_fluent.called
        assert mock_fluent.call_args.args[0] == 'customer.logs'
        assert mock_fluent.call_args.kwargs == {
            'host': ANY,
            'port': 24224,
            'queue_maxsize': 50000,
            'queue_circular': True,
        }

def test_dag_processor_manager_log_handler(mock_boto3_client, mock_fluent, mock_watchtower):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = DagProcessorManagerLogHandler(
            'arn:aws:logs:us-west-2:123456789012:log-group:test',
            None,
            'test_stream',
            True
        )

        assert mock_fluent.called
        assert mock_fluent.call_args.args[0] == 'customer.logs'
        assert mock_fluent.call_args.kwargs == {
            'host': ANY,
            'port': 24224,
            'queue_maxsize': 50000,
            'queue_circular': True,
        }

def test_dag_processing_log_handler(mock_boto3_client, mock_fluent, mock_watchtower):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = DagProcessingLogHandler(
            'arn:aws:logs:us-west-2:123456789012:log-group:test',
            None,
            'test_stream_template',
            True
        )

        handler.set_context('test_dag.py')
        
        assert mock_fluent.called
        assert mock_fluent.call_args.args[0] == 'customer.logs'
        assert mock_fluent.call_args.kwargs == {
            'host': ANY,
            'port': 24224,
            'queue_maxsize': 50000,
            'queue_circular': True,
        }


def test_cloudwatch_remote_task_logger_always_uses_watchtower_not_fluent(mock_boto3_client, mock_fluent, mock_watchtower):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        logger = CloudWatchRemoteTaskLogger(
            log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
            kms_key_arn=None,
            enabled=True,
            log_level='INFO'
        )

        # Trigger handler initialisation (lazy)
        logger.get_handler()

        assert logger.handler is not None
        assert mock_watchtower.called, "CloudWatchRemoteTaskLogger must use watchtower"
        assert not mock_fluent.called, (
            "CloudWatchRemoteTaskLogger must NOT use Fluent, "
            "even with USE_NON_CRITICAL_LOGGING=true"
        )
        mock_watchtower.assert_called_once_with(
            log_group_name='test-Task',
            boto3_client=mock_boto3_client.return_value,
            use_queues=True,
            create_log_group=False,
        )


def _make_task_instance_mock():
    ti = MagicMock()
    ti.dag_id = 'test_dag'
    ti.task_id = 'test_task'
    ti.run_id = 'test_run'
    ti.try_number = 1
    ti.end_date = None

    mock_dag_run = MagicMock()
    mock_dag_run.logical_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_dag_run.run_after = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_dag_run.data_interval_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mock_dag_run.data_interval_end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    mock_log_template = MagicMock()
    mock_log_template.filename = "dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    mock_dag_run.get_log_template.return_value = mock_log_template

    ti.get_dagrun.return_value = mock_dag_run
    return ti


def _make_logger_with_hook(mock_hook_class, log_events):
    mock_hook = Mock()
    mock_hook_class.return_value = mock_hook
    mock_hook.get_log_events.return_value = log_events

    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=True,
        log_level='INFO'
    )
    return logger


def _get_event_text(msg):
    if isinstance(msg, str):
        return msg
    return getattr(msg, 'event', str(msg))


def test_read_with_no_triggerer_streams(mock_boto3_client):
    """Test read() with no triggerer streams returns only task logs."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {'logStreams': []}
        ti = _make_task_instance_mock()

        messages, metadata = logger.read(ti, 1)

        for msg in messages:
            assert "triggerer" not in _get_event_text(msg).lower()


def test_read_with_one_triggerer_stream(mock_boto3_client):
    """Test read() with one triggerer stream returns task logs + header + triggerer logs."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        triggerer_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "triggerer log line", "level": "info"}'
            }
        ]
        logger = _make_logger_with_hook(mock_hook_class, task_events)

        triggerer_stream = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.42.log'
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [{'logStreamName': triggerer_stream}]
        }
        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            if '.trigger.' in kwargs.get('log_stream_name', ''):
                return triggerer_events
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        events_text = [_get_event_text(msg) for msg in messages]
        assert any(f"Reading triggerer logs from: {triggerer_stream}" in e for e in events_text)
        assert any("triggerer log line" in e for e in events_text)


def test_read_with_multiple_triggerer_streams(mock_boto3_client):
    """Test read() with multiple triggerer streams returns all streams' events."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        stream1 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.1.log'
        stream2 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.2.log'

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [{'logStreamName': stream1}, {'logStreamName': stream2}]
        }
        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            stream = kwargs.get('log_stream_name', '')
            if stream == stream1:
                return [{'timestamp': int(datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                         'message': '{"event": "stream1_event", "level": "info"}'}]
            elif stream == stream2:
                return [{'timestamp': int(datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc).timestamp() * 1000),
                         'message': '{"event": "stream2_event", "level": "info"}'}]
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        events_text = [_get_event_text(msg) for msg in messages]
        assert any("stream1_event" in e for e in events_text)
        assert any("stream2_event" in e for e in events_text)


def test_read_describe_log_streams_failure(mock_boto3_client):
    """Test describe_log_streams failure still returns task logs."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.side_effect = Exception("CloudWatch API error")

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        events_text = [_get_event_text(msg) for msg in messages]
        assert any("task log line" in e for e in events_text)


def test_read_get_log_events_failure_for_triggerer_stream(mock_boto3_client):
    """Test get_log_events failure for one triggerer stream skips it and continues."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        stream_fail = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.10.log'
        stream_ok = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.20.log'

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [{'logStreamName': stream_fail}, {'logStreamName': stream_ok}]
        }
        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            stream = kwargs.get('log_stream_name', '')
            if stream == stream_fail:
                raise Exception("Stream read error")
            elif stream == stream_ok:
                return [{'timestamp': int(datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc).timestamp() * 1000),
                         'message': '{"event": "ok_triggerer_event", "level": "info"}'}]
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        events_text = [_get_event_text(msg) for msg in messages]
        assert any("ok_triggerer_event" in e for e in events_text)
        assert any("Failed to read triggerer logs from" in e for e in events_text)


def test_read_excludes_non_matching_triggerer_streams(mock_boto3_client):
    """Test streams not matching regex pattern are excluded."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        bad_stream = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.abc.log'

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [{'logStreamName': bad_stream}]
        }

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        events_text = [_get_event_text(msg) for msg in messages]
        assert not any("Reading triggerer logs from:" in e for e in events_text)


@pytest.mark.parametrize("stream_suffix", [
    ".trigger.1.log",
    ".trigger.123.log",
    ".trigger.999999.log",
])
def test_triggerer_pattern_matches_valid_streams(stream_suffix):
    """Valid triggerer stream suffixes must be matched by TRIGGERER_STREAM_PATTERN."""
    assert CloudWatchRemoteTaskLogger.TRIGGERER_STREAM_PATTERN.search(stream_suffix) is not None


@pytest.mark.parametrize("stream_suffix", [
    ".trigger.abc.log",
    ".trigger..log",
    ".triggerX.1.log",
    ".trigger.1.txt",
    ".trigger.log",
])
def test_triggerer_pattern_rejects_invalid_streams(stream_suffix):
    """Invalid triggerer stream suffixes must NOT be matched by TRIGGERER_STREAM_PATTERN."""
    assert CloudWatchRemoteTaskLogger.TRIGGERER_STREAM_PATTERN.search(stream_suffix) is None


def test_discover_triggerer_streams_follows_pagination(mock_boto3_client):
    """_discover_triggerer_streams must follow nextToken pagination."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        logger = _make_logger_with_hook(mock_hook_class, [])

        stream_page1 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.1.log'
        stream_page2 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.2.log'

        logger.hook.conn.describe_log_streams.side_effect = [
            {'logStreams': [{'logStreamName': stream_page1}], 'nextToken': 'token123'},
            {'logStreams': [{'logStreamName': stream_page2}]},
        ]

        base_stream = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log'
        streams = logger._discover_triggerer_streams(base_stream)

        assert stream_page1 in streams
        assert stream_page2 in streams
        assert len(streams) == 2
        assert logger.hook.conn.describe_log_streams.call_count == 2


# =============================================================================
# Tests for _split_oversize_event
# =============================================================================

class TestSplitOversizeEvent:
    """Comprehensive tests for CloudWatchRemoteTaskLogger._split_oversize_event."""

    @pytest.fixture
    def logger(self):
        """Create a CloudWatchRemoteTaskLogger instance for testing."""
        return CloudWatchRemoteTaskLogger(
            log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
            kms_key_arn=None,
            enabled=True,
            log_level='INFO'
        )

    # --- Basic behavior: no split needed ---

    def test_small_message_returns_single_item_list(self, logger):
        """A message under the size limit should be returned as-is in a single-item list."""
        msg = {"event": "short log line", "level": "info", "timestamp": "2024-01-01T00:00:00"}
        result = logger._split_oversize_event(msg)
        assert result == [msg]
        assert len(result) == 1

    def test_empty_event_returns_single_item(self, logger):
        """A message with an empty event field should not be split."""
        msg = {"event": "", "level": "info", "timestamp": "2024-01-01T00:00:00"}
        result = logger._split_oversize_event(msg)
        assert result == [msg]

    def test_message_exactly_at_limit_returns_single_item(self, logger):
        """A message exactly at _MAX_EVENT_BYTES should not be split."""
        metadata = {"level": "info", "timestamp": "2024-01-01T00:00:00"}
        # Calculate how much space we have for the event
        envelope = json.dumps({**metadata, "event": ""}, default=str).encode("utf-8")
        available = logger._MAX_EVENT_BYTES - len(envelope)
        # Create an event that, when JSON-serialized, fills exactly to the limit
        # Use 'a' chars which don't get escaped in JSON
        event_str = "a" * (available - 2)  # -2 for the quotes around the string in JSON
        msg = {**metadata, "event": event_str}
        serialized_size = len(json.dumps(msg, default=str).encode("utf-8"))
        # Ensure we're at or under the limit
        assert serialized_size <= logger._MAX_EVENT_BYTES
        result = logger._split_oversize_event(msg)
        assert len(result) == 1
        assert result[0]["event"] == event_str

    # --- Splitting behavior ---

    def test_oversize_message_is_split_into_multiple_chunks(self, logger):
        """A message exceeding _MAX_EVENT_BYTES should be split into multiple chunks."""
        # Create a message well over the limit
        large_event = "x" * (logger._MAX_EVENT_BYTES * 2)
        msg = {"event": large_event, "level": "info", "timestamp": "2024-01-01T00:00:00"}
        result = logger._split_oversize_event(msg)
        assert len(result) > 1

    def test_split_chunks_concatenate_to_original_event(self, logger):
        """Concatenating all chunk events should reproduce the original event string."""
        large_event = "Hello world! " * 30000  # ~390KB, well over 260KB limit
        msg = {"event": large_event, "level": "info", "timestamp": "2024-01-01T00:00:00"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_each_chunk_is_within_size_limit(self, logger):
        """Each chunk, when JSON-serialized, must be within _MAX_EVENT_BYTES."""
        large_event = "a" * (logger._MAX_EVENT_BYTES * 3)
        msg = {"event": large_event, "level": "info", "timestamp": "2024-01-01T00:00:00"}
        result = logger._split_oversize_event(msg)
        for i, chunk in enumerate(result):
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES, (
                f"Chunk {i} is {chunk_size} bytes, exceeds limit of {logger._MAX_EVENT_BYTES}"
            )

    def test_metadata_preserved_in_all_chunks(self, logger):
        """All metadata keys (everything except 'event') must be present in every chunk."""
        metadata = {
            "level": "error",
            "timestamp": "2024-01-01T12:00:00",
            "logger_name": "airflow.task",
            "task_id": "my_task",
        }
        large_event = "log line " * 50000
        msg = {**metadata, "event": large_event}
        result = logger._split_oversize_event(msg)
        assert len(result) > 1
        for chunk in result:
            for key, value in metadata.items():
                assert chunk[key] == value, f"Metadata key '{key}' missing or wrong in chunk"

    # --- JSON escape handling ---

    def test_newlines_in_event_are_handled_correctly(self, logger):
        """Events with newline characters (which become \\n in JSON) should split correctly."""
        # Newlines become 2-char sequences in JSON, so this tests escape-aware splitting
        large_event = "line\n" * 80000  # lots of newlines
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    def test_tabs_in_event_are_handled_correctly(self, logger):
        """Events with tab characters (which become \\t in JSON) should split correctly."""
        large_event = "col1\tcol2\tcol3\n" * 30000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_backslashes_in_event_are_handled_correctly(self, logger):
        """Events with backslashes (which become \\\\ in JSON) should split correctly."""
        large_event = "path\\to\\file\n" * 30000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    def test_quotes_in_event_are_handled_correctly(self, logger):
        """Events with double quotes (which become \\" in JSON) should split correctly."""
        large_event = 'key="value" ' * 30000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_mixed_escape_sequences(self, logger):
        """Events with a mix of escape-worthy characters should split correctly."""
        # Mix of \n, \t, \\, " — all become 2-char sequences in JSON
        line = 'ERROR\t"file\\path"\nstack trace line\r\n'
        large_event = line * 15000
        msg = {"event": large_event, "level": "error"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    # --- Multi-byte UTF-8 / Unicode handling ---
    # With ensure_ascii=False, non-ASCII characters stay as raw UTF-8 bytes
    # and the existing multi-byte walk-back logic handles them correctly.

    def test_ascii_only_large_event_splits_correctly(self, logger):
        """Pure ASCII events should always split and reassemble."""
        large_event = "ABCDEFGHIJ" * 30000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_multibyte_utf8_characters_not_split_mid_char(self, logger):
        """Multi-byte UTF-8 characters must not be split in the middle."""
        # Japanese characters are 3 bytes each in UTF-8
        large_event = "日本語テスト" * 20000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, ensure_ascii=False, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    def test_emoji_characters_not_split(self, logger):
        """4-byte emoji characters must not be split in the middle."""
        large_event = "🚀🎉🔥" * 25000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_mixed_ascii_and_multibyte(self, logger):
        """Mix of ASCII and multi-byte characters should split correctly."""
        large_event = "Hello 世界! " * 30000
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    # --- Edge cases ---

    def test_event_field_missing_returns_single_item(self, logger):
        """If 'event' key is missing, the message should still be handled."""
        # Create a large message without 'event' key — but it's under the limit
        msg = {"level": "info", "data": "x" * 100}
        result = logger._split_oversize_event(msg)
        # Without 'event', it should just return as-is if under limit
        assert len(result) == 1

    def test_non_string_event_is_converted(self, logger):
        """If event is not a string (e.g., dict or int), it should be str()-converted."""
        large_dict_str = str({"key": "v" * logger._MAX_EVENT_BYTES})
        msg = {"event": large_dict_str, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_dict_str

    def test_event_with_only_escape_characters(self, logger):
        """An event consisting entirely of characters that get escaped in JSON."""
        # Each \n becomes \\n in JSON (2 bytes), so effective size doubles
        large_event = "\n" * (logger._MAX_EVENT_BYTES)
        msg = {"event": large_event, "level": "info"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    def test_single_character_over_limit(self, logger):
        """Message just barely over the limit should split into exactly 2 chunks."""
        metadata = {"level": "info"}
        envelope = json.dumps({**metadata, "event": ""}, default=str).encode("utf-8")
        available = logger._MAX_EVENT_BYTES - len(envelope)
        # Create event that exceeds the limit. The envelope includes "event": "" which
        # accounts for the key and empty quotes. Each 'a' adds 1 byte to the JSON.
        # We need the total serialized msg to exceed _MAX_EVENT_BYTES.
        # available = max - envelope_size, so event of length (available + 1) will exceed.
        event_str = "a" * (available + 1)
        msg = {**metadata, "event": event_str}
        serialized_size = len(json.dumps(msg, default=str).encode("utf-8"))
        assert serialized_size > logger._MAX_EVENT_BYTES, "Test setup: msg must exceed limit"
        result = logger._split_oversize_event(msg)
        assert len(result) == 2
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == event_str

    def test_very_large_metadata_reduces_chunk_capacity(self, logger):
        """Large metadata should reduce the available space for event chunks."""
        # Large metadata means less room for event per chunk
        large_metadata = {
            "level": "info",
            "logger_name": "a" * 1000,
            "extra_field": "b" * 1000,
        }
        large_event = "x" * (logger._MAX_EVENT_BYTES * 2)
        msg = {**large_metadata, "event": large_event}
        result = logger._split_oversize_event(msg)
        # Should produce more chunks than without large metadata
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        for chunk in result:
            chunk_size = len(json.dumps(chunk, default=str).encode("utf-8"))
            assert chunk_size <= logger._MAX_EVENT_BYTES

    def test_returns_original_msg_if_empty_chunks(self, logger):
        """If splitting somehow produces no chunks, should return [msg] as fallback."""
        # This tests the `return chunks if chunks else [msg]` fallback
        msg = {"event": "small", "level": "info"}
        result = logger._split_oversize_event(msg)
        assert result == [msg]

    # --- Realistic scenarios ---

    def test_realistic_stack_trace(self, logger):
        """A realistic large Python stack trace should split and reassemble correctly."""
        frame = (
            '  File "/usr/local/lib/python3.12/site-packages/airflow/models/taskinstance.py", '
            'line 1234, in _run_raw_task\n'
            '    result = execute_callable(context=context)\n'
        )
        # Make it large enough to require splitting
        large_event = "Traceback (most recent call last):\n" + frame * 2000 + "RuntimeError: something broke\n"
        msg = {"event": large_event, "level": "error", "timestamp": "2024-06-15T10:30:00"}
        result = logger._split_oversize_event(msg)
        assert len(result) > 1
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event

    def test_realistic_json_log_payload(self, logger):
        """A large JSON-structured log event should split and reassemble correctly."""
        import json as json_mod
        records = [{"id": i, "status": "processed", "data": "x" * 200} for i in range(1000)]
        large_event = json_mod.dumps(records)
        msg = {"event": large_event, "level": "info", "timestamp": "2024-06-15T10:30:00"}
        result = logger._split_oversize_event(msg)
        reconstructed = "".join(chunk["event"] for chunk in result)
        assert reconstructed == large_event
        # Verify the reconstructed JSON is still valid
        parsed = json_mod.loads(reconstructed)
        assert len(parsed) == 1000

    # --- Resilience / fallback behavior ---

    def test_split_error_does_not_raise(self, logger):
        """If the inner split logic throws, the outer method should not propagate the exception."""
        from unittest.mock import patch

        msg = {"event": "test", "level": "info"}
        with patch.object(logger, '_split_oversize_event_inner', side_effect=RuntimeError("simulated failure")):
            # Should not raise — falls back gracefully
            result = logger._split_oversize_event(msg)
            assert len(result) == 1
            assert "event" in result[0]

    def test_split_error_returns_truncated_for_large_event(self, logger):
        """On split failure with a large event, should return a truncated version."""
        from unittest.mock import patch

        large_event = "x" * (logger._MAX_EVENT_BYTES * 2)
        msg = {"event": large_event, "level": "error"}
        with patch.object(logger, '_split_oversize_event_inner', side_effect=ValueError("bad split")):
            result = logger._split_oversize_event(msg)
            assert len(result) == 1
            assert "TRUNCATED" in result[0]["event"]
            assert result[0]["level"] == "error"

    def test_split_error_increments_metric(self, logger):
        """On split failure, a metric should be emitted."""
        from unittest.mock import patch, Mock

        logger.stats = Mock()
        msg = {"event": "test", "level": "info"}
        with patch.object(logger, '_split_oversize_event_inner', side_effect=RuntimeError("fail")):
            logger._split_oversize_event(msg)
            logger.stats.incr.assert_called_once_with("mwaa.logging.task.split_error", 1)

    # --- Watchtower compatibility ---

    def test_chunks_fit_watchtower_limit_with_non_ascii(self, logger):
        """Chunks must not exceed watchtower's max_message_size (262144 bytes) when
        serialized with ensure_ascii=True (watchtower's default behavior).

        This is a regression test for a bug where the split logic measured chunk sizes
        using ensure_ascii=False (non-ASCII as raw UTF-8, e.g. 田=3 bytes) but watchtower
        serializes with ensure_ascii=True (田 becomes \\u7530 = 6 bytes), causing chunks
        to exceed watchtower's limit and get silently truncated.
        """
        WATCHTOWER_MAX_MESSAGE_SIZE = 262144
        WATCHTOWER_EXTRA_PAYLOAD = 26
        watchtower_effective_limit = WATCHTOWER_MAX_MESSAGE_SIZE - WATCHTOWER_EXTRA_PAYLOAD

        # Build a message with heavy non-ASCII content (CJK, accented, emojis)
        pattern = (
            'ERROR: Failed for customer: \u7530\u4e2d\u592a\u90ce (order_id=98765)\n'
            'WARNING: Donn\u00e9es invalides pour Jos\u00e9 Garc\u00eda \u2014 r\u00e9essayer\n'
            'DEBUG: /data/uploads/\u5ba2\u6237\u6570\u636e_2024\u5e746\u6708.csv\n'
            'INFO: \u2705 batch_1 ok, \u274c batch_2 fail, \U0001f504 batch_3 retry\n'
        )
        # Make it ~512KB raw to force splitting
        event = pattern * (512000 // len(pattern.encode('utf-8')))
        msg = {"logger": "airflow.task", "level": "info", "event": event}

        result = logger._split_oversize_event(msg)

        # Must produce multiple chunks
        assert len(result) > 1, "Expected message to be split into multiple chunks"

        # Each chunk, when serialized the way watchtower does it (ensure_ascii=True),
        # must fit within watchtower's effective limit
        for i, chunk in enumerate(result):
            # Watchtower uses json.dumps with default serializer (ensure_ascii=True)
            watchtower_serialized = json.dumps(chunk, default=str)
            watchtower_size = len(watchtower_serialized.encode("utf-8"))
            assert watchtower_size <= watchtower_effective_limit, (
                f"Chunk {i} exceeds watchtower limit: {watchtower_size} > {watchtower_effective_limit}. "
                f"This means watchtower will silently truncate the chunk, causing data loss."
            )

        # Verify no data loss: concatenated events must equal original
        reassembled = "".join(chunk["event"] for chunk in result)
        assert reassembled == event, "Data loss detected: reassembled chunks don't match original event"
