import logging
import time
import pytest
import os
import importlib
import types
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, Mock, MagicMock, ANY, call

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


def test_emit_skips_deprecated_metric_message(base_logger):
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


def test_emit_handles_normal_message(base_logger):
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
        assert True
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
                queue_overflow_handler=ANY,
                buffer_overflow_handler=ANY,
                nanosecond_precision=True,
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
    assert logger.handler is None


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

    result = logger.emit(record)
    assert result is None


def _write_af3_task_log(base, dag_id="d", run_id="r", task_id="t", attempt=1):
    """Create a nested Airflow 3 task-attempt log file under base; return its path."""
    log_dir = os.path.join(base, f"dag_id={dag_id}", f"run_id={run_id}", f"task_id={task_id}")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"attempt={attempt}.log")
    with open(log_file, "w") as f:
        f.write("a task log line\n")
    return log_file


def _make_task_logger(enabled=True):
    logger = CloudWatchRemoteTaskLogger(
        log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
        kms_key_arn=None,
        enabled=enabled,
        log_level='INFO'
    )
    logger.handler = Mock()
    return logger


def test_cloudwatch_remote_task_logger_upload_deletes_local_log(tmp_path):
    """upload() flushes, then deletes the local task-log attempt directory.

    Replaces the old *_upload_is_noop test: that no-op was the disk-accumulation
    bug. Logs are served from CloudWatch (read() has no local fallback), so
    removing the local copy loses nothing customer-visible.
    """
    base = str(tmp_path)
    log_file = _write_af3_task_log(base)
    parent = os.path.dirname(log_file)
    logger = _make_task_logger(enabled=True)
    ti = MagicMock()

    # The supervisor passes the path relative to base_log_folder.
    rel = os.path.relpath(log_file, base)
    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload(rel, ti)

    assert result is None
    logger.handler.flush.assert_called_once()
    assert not os.path.exists(log_file)
    assert not os.path.exists(parent)


def test_cloudwatch_remote_task_logger_upload_disabled_keeps_local_log(tmp_path):
    """When the handler is disabled, logs were not streamed to CloudWatch, so the
    local copy must NOT be deleted."""
    base = str(tmp_path)
    log_file = _write_af3_task_log(base)
    logger = _make_task_logger(enabled=False)
    ti = MagicMock()

    rel = os.path.relpath(log_file, base)
    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload(rel, ti)

    assert result is None
    logger.handler.flush.assert_called_once()
    assert os.path.exists(log_file)


def test_cloudwatch_remote_task_logger_upload_missing_dir_is_noop(tmp_path):
    """upload() does not raise if the attempt directory is already gone."""
    base = str(tmp_path)
    logger = _make_task_logger(enabled=True)
    ti = MagicMock()

    # A well-formed relative path whose parent was never created.
    rel = os.path.join("dag_id=d", "run_id=r", "task_id=t", "attempt=1.log")
    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload(rel, ti)

    assert result is None


def test_cloudwatch_remote_task_logger_upload_skips_path_outside_base(tmp_path):
    """A path resolving outside base_log_folder is not deleted (traversal guard)."""
    base = os.path.join(str(tmp_path), "logs")
    os.makedirs(base, exist_ok=True)
    # Sentinel file outside the base that must survive.
    outside = os.path.join(str(tmp_path), "outside.log")
    with open(outside, "w") as f:
        f.write("do not delete\n")

    logger = _make_task_logger(enabled=True)
    ti = MagicMock()

    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload(os.path.join("..", "outside.log"), ti)

    assert result is None
    assert os.path.exists(outside)


def test_cloudwatch_remote_task_logger_upload_refuses_to_delete_base(tmp_path):
    """A log resolving directly under base_log_folder must NOT delete base itself.

    parent == base would otherwise rmtree every task's local logs on the worker.
    The relative_to guard passes in this case (the file IS inside base), so this
    equality check is the last line of defense against wiping base_log_folder.
    """
    base = str(tmp_path)
    # A sibling file standing in for another task's logs under base; must survive.
    sentinel = os.path.join(base, "other_task.log")
    with open(sentinel, "w") as f:
        f.write("do not delete\n")
    # Target log resolves directly under base -> parent == base.
    log_file = os.path.join(base, "attempt=1.log")
    with open(log_file, "w") as f:
        f.write("log\n")

    logger = _make_task_logger(enabled=True)
    logger.stats = Mock()
    ti = MagicMock()

    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload("attempt=1.log", ti)

    assert result is None
    # base and the sibling task's logs are untouched (catastrophe prevented).
    assert os.path.isdir(base)
    assert os.path.exists(sentinel)
    # A clean refusal, not an error path: the error metric must not fire.
    logger.stats.incr.assert_not_called()


def test_cloudwatch_remote_task_logger_upload_handles_absolute_path(tmp_path):
    """An absolute path under the base is resolved and its parent deleted.

    Defensive-branch coverage: the real supervisor always passes a relative path.
    """
    base = str(tmp_path)
    log_file = _write_af3_task_log(base)
    parent = os.path.dirname(log_file)
    logger = _make_task_logger(enabled=True)
    ti = MagicMock()

    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.return_value = base
        result = logger.upload(log_file, ti)  # absolute path

    assert result is None
    assert not os.path.exists(parent)


def test_cloudwatch_remote_task_logger_upload_delete_error_is_non_fatal(tmp_path):
    """A silent rmtree failure (dir remains) increments the metric and is non-fatal.

    rmtree(ignore_errors=True) does not raise on OSError, so the real failure
    signal is 'parent still exists after rmtree', not an exception. Mock rmtree
    as a no-op that leaves the directory in place to exercise that path.
    """
    base = str(tmp_path)
    log_file = _write_af3_task_log(base)
    parent = os.path.dirname(log_file)
    logger = _make_task_logger(enabled=True)
    logger.stats = Mock()
    ti = MagicMock()

    rel = os.path.relpath(log_file, base)
    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf, \
         patch('mwaa.logging.cloudwatch_handlers.shutil.rmtree') as mock_rmtree:
        mock_conf.get.return_value = base
        result = logger.upload(rel, ti)  # rmtree is a no-op: dir remains

    assert result is None
    mock_rmtree.assert_called_once()
    assert os.path.exists(parent)  # simulated silent failure: dir not removed
    logger.stats.incr.assert_called_once()
    assert 'local_log_delete_error' in logger.stats.incr.call_args[0][0]


def test_cloudwatch_remote_task_logger_upload_unexpected_error_is_non_fatal(tmp_path):
    """An unexpected error before the delete (e.g. conf resolution) hits the outer
    except path: metric incremented, non-fatal."""
    base = str(tmp_path)
    log_file = _write_af3_task_log(base)
    logger = _make_task_logger(enabled=True)
    logger.stats = Mock()
    ti = MagicMock()

    rel = os.path.relpath(log_file, base)
    with patch('mwaa.logging.cloudwatch_handlers.conf') as mock_conf:
        mock_conf.get.side_effect = RuntimeError("conf boom")
        result = logger.upload(rel, ti)

    assert result is None
    logger.stats.incr.assert_called_once()
    assert 'local_log_delete_error' in logger.stats.incr.call_args[0][0]


def _make_task_instance_mock():
    """Helper to create a mock TaskInstance with standard test values."""
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
    """Helper to create a CloudWatchRemoteTaskLogger with a mocked AwsLogsHook."""
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


def test_cloudwatch_remote_task_logger_read(mock_boto3_client):
    """Test CloudWatchRemoteTaskLogger read() returns StructuredLogMessage objects."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        logger = _make_logger_with_hook(mock_hook_class, [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "test message", "level": "info"}'
            }
        ])
        ti = _make_task_instance_mock()

        messages, metadata = logger.read(ti, 1)

        assert len(messages) >= 2  # at least 1 info message + 1 log entry
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )


def test_read_returns_end_of_log_true_in_metadata(mock_boto3_client):
    """Test that read() always sets end_of_log=True in metadata.

    This is critical for Airflow 3.2.0 where the UI sends Accept: application/x-ndjson,
    causing the server to use read_log_stream() which loops until end_of_log is True.
    Without this, the stream loops forever.
    """
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        logger = _make_logger_with_hook(mock_hook_class, [])
        ti = _make_task_instance_mock()

        # Test with no initial metadata
        _, metadata = logger.read(ti, 1)
        assert metadata["end_of_log"] is True

        # Test with existing metadata dict (should not lose other keys)
        _, metadata = logger.read(ti, 1, metadata={"offset": 42})
        assert metadata["end_of_log"] is True
        assert metadata["offset"] == 42


def test_read_messages_are_ndjson_serializable(mock_boto3_client):
    """Test that all messages from read() can be serialized via model_dump_json().

    The NDJSON streaming path calls .model_dump_json() on every item returned by read().
    If any item is a plain string instead of StructuredLogMessage, it crashes.
    """
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        logger = _make_logger_with_hook(mock_hook_class, [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "line one", "level": "info"}'
            },
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc).timestamp() * 1000),
                'message': 'plain text line'
            },
        ])
        ti = _make_task_instance_mock()

        messages, _ = logger.read(ti, 1)

        for msg in messages:
            # This is exactly what the NDJSON streaming path does
            json_str = msg.model_dump_json()
            assert isinstance(json_str, str)
            assert len(json_str) > 0


def test_read_remote_logs_error_returns_structured_messages(mock_boto3_client):
    """Test that _read_remote_logs wraps error messages as StructuredLogMessage too."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        mock_hook = Mock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_log_events.side_effect = Exception("CloudWatch unavailable")

        logger = CloudWatchRemoteTaskLogger(
            log_group_arn='arn:aws:logs:us-west-2:123456789012:log-group:test-Task',
            kms_key_arn=None,
            enabled=True,
            log_level='INFO'
        )
        ti = _make_task_instance_mock()

        messages, metadata = logger.read(ti, 1)

        assert metadata["end_of_log"] is True
        assert len(messages) >= 2  # info message + error message
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage)
        # The error message should contain the exception text
        assert any("CloudWatch unavailable" in msg.event for msg in messages if hasattr(msg, 'event'))


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

    dag_processor_stream = "dag_processor/2025-01-01/dags-folder/dag.py.log"
    assert any(re.match(p, dag_processor_stream) for p in CloudWatchRemoteTaskLogger.IGNORED_PATTERNS)

    task_stream = "dag_id=test/run_id=test/task_id=test/attempt=1.log"
    assert not any(re.match(p, task_stream) for p in CloudWatchRemoteTaskLogger.IGNORED_PATTERNS)


def test_subprocess_log_handler_with_fluent(mock_boto3_client, mock_fluent):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
        }

def test_dag_processor_manager_log_handler(mock_boto3_client, mock_fluent, mock_watchtower):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
        }

def test_dag_processing_log_handler(mock_boto3_client, mock_fluent, mock_watchtower):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
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


def test_read_with_no_triggerer_streams(mock_boto3_client):
    """Test read() with no triggerer streams returns only task logs (no error, no header)."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        logger = _make_logger_with_hook(mock_hook_class, task_events)
        # Mock describe_log_streams to return no triggerer streams
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': []
        }
        ti = _make_task_instance_mock()

        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        # No triggerer text should appear
        for msg in messages:
            assert "triggerer" not in msg.event.lower(), (
                f"Unexpected triggerer text in output: {msg.event}"
            )


def test_read_with_one_triggerer_stream(mock_boto3_client):
    """Test read() with one triggerer stream returns task logs + header + triggerer logs."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

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
        # Return different events for task stream vs triggerer stream
        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            if '.trigger.' in kwargs.get('log_stream_name', ''):
                return triggerer_events
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        # Check header is present
        header_found = any(
            f"Reading triggerer logs from: {triggerer_stream}" in msg.event
            for msg in messages
        )
        assert header_found, "Expected triggerer header not found in output"
        # Check triggerer event is present
        triggerer_event_found = any(
            "triggerer log line" in msg.event for msg in messages
        )
        assert triggerer_event_found, "Expected triggerer log event not found in output"


def test_read_with_multiple_triggerer_streams(mock_boto3_client):
    """Test read() with multiple triggerer streams returns all streams' events."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        stream1 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.1.log'
        stream2 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.2.log'
        stream3 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.3.log'

        triggerer_events_1 = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "triggerer_stream_1_event", "level": "info"}'
            }
        ]
        triggerer_events_2 = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "triggerer_stream_2_event", "level": "info"}'
            }
        ]
        triggerer_events_3 = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "triggerer_stream_3_event", "level": "info"}'
            }
        ]

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [
                {'logStreamName': stream1},
                {'logStreamName': stream2},
                {'logStreamName': stream3},
            ]
        }

        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            stream = kwargs.get('log_stream_name', '')
            if stream == stream1:
                return triggerer_events_1
            elif stream == stream2:
                return triggerer_events_2
            elif stream == stream3:
                return triggerer_events_3
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        # All three streams' events must appear
        events_text = [msg.event for msg in messages]
        assert any("triggerer_stream_1_event" in e for e in events_text), "Stream 1 events missing"
        assert any("triggerer_stream_2_event" in e for e in events_text), "Stream 2 events missing"
        assert any("triggerer_stream_3_event" in e for e in events_text), "Stream 3 events missing"


def test_read_describe_log_streams_failure(mock_boto3_client):
    """Test describe_log_streams failure still returns task logs + error message."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        logger = _make_logger_with_hook(mock_hook_class, task_events)
        # Make describe_log_streams raise an exception
        logger.hook.conn.describe_log_streams.side_effect = Exception("CloudWatch API error")

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        # Task logs should still be present
        task_log_found = any("task log line" in msg.event for msg in messages)
        assert task_log_found, "Task logs should still be returned on describe_log_streams failure"


def test_read_get_log_events_failure_for_triggerer_stream(mock_boto3_client):
    """Test get_log_events failure for one triggerer stream skips it and continues with others."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        stream_fail = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.10.log'
        stream_ok = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.20.log'

        triggerer_ok_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 2, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "ok_triggerer_event", "level": "info"}'
            }
        ]

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [
                {'logStreamName': stream_fail},
                {'logStreamName': stream_ok},
            ]
        }

        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            stream = kwargs.get('log_stream_name', '')
            if stream == stream_fail:
                raise Exception("Stream read error")
            elif stream == stream_ok:
                return triggerer_ok_events
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        events_text = [msg.event for msg in messages]
        # The second stream's events should be present
        assert any("ok_triggerer_event" in e for e in events_text), (
            "Second triggerer stream events should be present"
        )
        # An error message for the failed stream should be present
        assert any("Failed to read triggerer logs from" in e and stream_fail in e for e in events_text), (
            "Error message for failed triggerer stream should be present"
        )


def test_read_excludes_non_matching_triggerer_streams(mock_boto3_client):
    """Test streams not matching regex pattern are excluded."""
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        task_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "task log line", "level": "info"}'
            }
        ]
        # These streams do NOT match the .trigger.{digits}.log pattern
        bad_stream_1 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.abc.log'
        bad_stream_2 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger..log'

        bad_events = [
            {
                'timestamp': int(datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'message': '{"event": "bad_stream_event", "level": "info"}'
            }
        ]

        logger = _make_logger_with_hook(mock_hook_class, task_events)
        logger.hook.conn.describe_log_streams.return_value = {
            'logStreams': [
                {'logStreamName': bad_stream_1},
                {'logStreamName': bad_stream_2},
            ]
        }

        mock_hook = mock_hook_class.return_value
        def get_log_events_side_effect(**kwargs):
            stream = kwargs.get('log_stream_name', '')
            if stream in (bad_stream_1, bad_stream_2):
                return bad_events
            return task_events
        mock_hook.get_log_events.side_effect = get_log_events_side_effect

        ti = _make_task_instance_mock()
        messages, metadata = logger.read(ti, 1)

        # All items must be StructuredLogMessage
        for msg in messages:
            assert isinstance(msg, StructuredLogMessage), (
                f"Expected StructuredLogMessage, got {type(msg).__name__}: {msg}"
            )
        # Non-matching stream events should NOT appear
        events_text = [msg.event for msg in messages]
        assert not any("bad_stream_event" in e for e in events_text), (
            "Events from non-matching streams should not appear in output"
        )
        # No triggerer header should appear since all streams were filtered out
        assert not any("Reading triggerer logs from:" in e for e in events_text), (
            "No triggerer header should appear when all streams are filtered out"
        )

@pytest.mark.parametrize("stream_suffix", [
    ".trigger.1.log",
    ".trigger.123.log",
    ".trigger.999999.log",
])
def test_triggerer_pattern_matches_valid_streams(stream_suffix):
    """Valid triggerer stream suffixes must be matched by TRIGGERER_STREAM_PATTERN."""
    assert CloudWatchRemoteTaskLogger.TRIGGERER_STREAM_PATTERN.search(stream_suffix) is not None, (
        f"Expected pattern to match '{stream_suffix}'"
    )


@pytest.mark.parametrize("stream_suffix", [
    ".trigger.abc.log",
    ".trigger..log",
    ".triggerX.1.log",
    ".trigger.1.txt",
    ".trigger.log",
])
def test_triggerer_pattern_rejects_invalid_streams(stream_suffix):
    """Invalid triggerer stream suffixes must NOT be matched by TRIGGERER_STREAM_PATTERN."""
    assert CloudWatchRemoteTaskLogger.TRIGGERER_STREAM_PATTERN.search(stream_suffix) is None, (
        f"Expected pattern to NOT match '{stream_suffix}'"
    )


def test_discover_triggerer_streams_follows_pagination(mock_boto3_client):
    """_discover_triggerer_streams must follow nextToken pagination to collect all streams."""
    with patch('mwaa.logging.cloudwatch_handlers.AwsLogsHook') as mock_hook_class:
        logger = _make_logger_with_hook(mock_hook_class, [])

        stream_page1 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.1.log'
        stream_page2 = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log.trigger.2.log'

        logger.hook.conn.describe_log_streams.side_effect = [
            {
                'logStreams': [{'logStreamName': stream_page1}],
                'nextToken': 'token123',
            },
            {
                'logStreams': [{'logStreamName': stream_page2}],
            },
        ]

        base_stream = 'dag_id=test_dag/run_id=test_run/task_id=test_task/attempt=1.log'
        streams = logger._discover_triggerer_streams(base_stream)

        assert stream_page1 in streams
        assert stream_page2 in streams
        assert len(streams) == 2
        assert logger.hook.conn.describe_log_streams.call_count == 2

def test_cloudwatch_remote_task_logger_uses_fluent_when_non_critical(mock_boto3_client, mock_fluent, mock_watchtower):
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

        # Access processors to trigger the fluent handler creation
        _ = logger.processors

        assert mock_fluent.called, (
            "CloudWatchRemoteTaskLogger must use Fluent "
            "when USE_NON_CRITICAL_LOGGING=true"
        )
        mock_fluent.assert_called_once_with(
            'customer.task.logs',
            host='localhost',
            port=24224,
            queue_maxsize=50000,
            queue_circular=True,
            queue_overflow_handler=ANY,
            buffer_overflow_handler=ANY,
            nanosecond_precision=True,
        )
        assert not mock_watchtower.called, (
            "CloudWatchRemoteTaskLogger must NOT use watchtower "
            "when USE_NON_CRITICAL_LOGGING=true"
        )


# --- Tests for NCL observability metrics ---

def test_queue_overflow_handler_emits_metric(mock_boto3_client):
    """Verify that queue_overflow_handler fires the queue_evicted StatsD metric."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import _make_overflow_handlers

        with patch('mwaa.logging.cloudwatch_handlers.get_statsd') as mock_get_statsd:
            mock_stats = MagicMock()
            mock_get_statsd.return_value = mock_stats

            queue_handler, _ = _make_overflow_handlers("scheduler")
            queue_handler(b'\x00' * 128)

            mock_stats.incr.assert_called_once_with("mwaa.logging.scheduler.ncl_queue_evicted_records", 1)


def test_buffer_overflow_handler_emits_metrics(mock_boto3_client):
    """Verify that buffer_overflow_handler fires both buffer_overflow and buffer_overflow_bytes metrics."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import _make_overflow_handlers

        with patch('mwaa.logging.cloudwatch_handlers.get_statsd') as mock_get_statsd:
            mock_stats = MagicMock()
            mock_get_statsd.return_value = mock_stats

            _, buffer_handler = _make_overflow_handlers("worker")
            fake_pendings = b'\x00' * 2048
            buffer_handler(fake_pendings)

            mock_stats.incr.assert_any_call("mwaa.logging.worker.ncl_buffer_overflow", 1)
            mock_stats.incr.assert_any_call("mwaa.logging.worker.ncl_buffer_overflow_bytes", 2048)


def test_emitted_counter_batches_at_threshold(mock_boto3_client, mock_fluent):
    """Verify that the emitted metric is batched: fires every _EMITTED_BATCH_SIZE records."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import BaseLogHandler, _EMITTED_BATCH_SIZE

        handler = BaseLogHandler('arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)
        handler.create_cloudwatch_handler('test_stream', 'scheduler')

        mock_stats = MagicMock()
        handler.stats = mock_stats

        # Emit _EMITTED_BATCH_SIZE - 1 records: no emitted metric yet
        record = logging.LogRecord('test', logging.INFO, '', 0, 'msg', (), None)
        for _ in range(_EMITTED_BATCH_SIZE - 1):
            handler.emit(record)

        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 0, "Should not have flushed emitted yet"

        # One more emit crosses the threshold
        handler.emit(record)

        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 1
        assert emitted_calls[0] == call(f"mwaa.logging.scheduler.ncl_emitted_records", _EMITTED_BATCH_SIZE)


def test_emitted_counter_flushes_remainder_on_close(mock_boto3_client, mock_fluent):
    """Verify that close() flushes any remaining emitted count."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import BaseLogHandler

        handler = BaseLogHandler('arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)
        handler.create_cloudwatch_handler('test_stream', 'worker')

        mock_stats = MagicMock()
        handler.stats = mock_stats

        # Emit 5 records (below threshold)
        record = logging.LogRecord('test', logging.INFO, '', 0, 'msg', (), None)
        for _ in range(5):
            handler.emit(record)

        # Close should flush the remainder
        handler.close()

        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 1
        assert emitted_calls[0] == call("mwaa.logging.worker.ncl_emitted_records", 5)


def test_emitted_counter_flushes_remainder_on_flush(mock_boto3_client, mock_fluent):
    """Verify that flush() flushes any remaining emitted count."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import BaseLogHandler

        handler = BaseLogHandler('arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)
        handler.create_cloudwatch_handler('test_stream', 'scheduler')

        mock_stats = MagicMock()
        handler.stats = mock_stats

        # Emit 3 records (below threshold)
        record = logging.LogRecord('test', logging.INFO, '', 0, 'msg', (), None)
        for _ in range(3):
            handler.emit(record)

        handler.flush()

        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 1
        assert emitted_calls[0] == call("mwaa.logging.scheduler.ncl_emitted_records", 3)


def test_emitted_counter_flushes_on_time_interval(mock_boto3_client, mock_fluent):
    """Verify that the emitted counter flushes after _EMITTED_FLUSH_INTERVAL_SECONDS even below batch threshold."""
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)
        from mwaa.logging.cloudwatch_handlers import BaseLogHandler, _EMITTED_FLUSH_INTERVAL_SECONDS

        handler = BaseLogHandler('arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)
        handler.create_cloudwatch_handler('test_stream', 'scheduler')

        mock_stats = MagicMock()
        handler.stats = mock_stats

        # Emit 5 records (well below batch threshold of 100)
        record = logging.LogRecord('test', logging.INFO, '', 0, 'msg', (), None)
        for _ in range(5):
            handler.emit(record)

        # No flush yet — below threshold and within time window
        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 0

        # Simulate time passing beyond the flush interval
        handler._last_emitted_flush = time.time() - _EMITTED_FLUSH_INTERVAL_SECONDS - 1

        # Next emit should trigger a time-based flush
        handler.emit(record)

        emitted_calls = [c for c in mock_stats.incr.call_args_list if 'emitted' in str(c)]
        assert len(emitted_calls) == 1
        # 5 from before + 1 that triggered the flush = 6
        assert emitted_calls[0] == call("mwaa.logging.scheduler.ncl_emitted_records", 6)
