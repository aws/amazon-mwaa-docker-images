import logging
import pytest
import os
import time
import importlib
from unittest.mock import patch, Mock, MagicMock, ANY, call
from airflow.models.taskinstance import TaskInstance
from mwaa.config.setup_environment import (
    setup_environment_variables,
    _execute_startup_script,
    _export_env_variables,
    _is_protected_os_environ,
)
from mwaa.subprocess.subprocess import Subprocess
from mwaa.logging.cloudwatch_handlers import (
    BaseLogHandler,
    TaskLogHandler,
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

@pytest.fixture(autouse=True)
def reload_module():
    import importlib
    import mwaa.logging.cloudwatch_handlers
    importlib.reload(mwaa.logging.cloudwatch_handlers)

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
                queue_overflow_handler=ANY,
                buffer_overflow_handler=ANY,
                nanosecond_precision=True,
            )

def test_task_log_handler_with_fluent(mock_boto3_client, mock_fluent):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = TaskLogHandler('', 'arn:aws:logs:us-west-2:123456789012:log-group:test', None, True)

        ti = MagicMock(spec=TaskInstance)
        ti.try_number = 1
        ti.dag_id = 'test_dag'
        ti.task_id = 'test_task'
        ti.execution_date = '2023-01-01'

        handler.set_context(ti)

        assert mock_fluent.called
        assert mock_fluent.call_args.args[0] == 'customer.task.logs'
        assert mock_fluent.call_args.kwargs == {
            'host': ANY,
            'port': 24224,
            'queue_maxsize': 50000,
            'queue_circular': True,
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
        }

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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
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
            'queue_overflow_handler': ANY,
            'buffer_overflow_handler': ANY,
            'nanosecond_precision': True,
        }


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
