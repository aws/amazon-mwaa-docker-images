import logging
import pytest
import os
import importlib
from unittest.mock import patch, Mock, MagicMock, ANY
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

        handler = BaseLogHandler('arn:aws:logs:us-east-1:123456789012:log-group:test', None, True)
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

def test_task_log_handler_with_fluent(mock_boto3_client, mock_fluent):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = TaskLogHandler('', 'arn:aws:logs:us-east-1:123456789012:log-group:test', None, True)

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
            'queue_maxsize': 50000
        }

def test_subprocess_log_handler_with_fluent(mock_boto3_client, mock_fluent):
    with patch.dict(os.environ, {'USE_NON_CRITICAL_LOGGING': 'true'}, clear=True):
        # Force reload of the module
        import importlib
        import mwaa.logging.cloudwatch_handlers
        importlib.reload(mwaa.logging.cloudwatch_handlers)

        handler = SubprocessLogHandler(
            'arn:aws:logs:us-east-1:123456789012:log-group:test',
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
            'arn:aws:logs:us-east-1:123456789012:log-group:test',
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
            'arn:aws:logs:us-east-1:123456789012:log-group:test',
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
