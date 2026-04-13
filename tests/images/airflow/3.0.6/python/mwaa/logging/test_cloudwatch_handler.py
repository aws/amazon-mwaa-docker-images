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
    with patch('mwaa.logging.cloudwatch_handlers.fluent_handler.FluentHandler') as mock:
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
                port=24224
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
            'port': 24224
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
            'port': 24224
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
            'port': 24224
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
        assert any(f"*** Reading triggerer logs from: {triggerer_stream}" in e for e in events_text)
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
        assert not any("*** Reading triggerer logs from:" in e for e in events_text)


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
