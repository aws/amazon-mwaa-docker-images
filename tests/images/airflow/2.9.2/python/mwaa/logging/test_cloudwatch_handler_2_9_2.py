import logging
import pytest
import os
from unittest.mock import patch, Mock
from mwaa.config.setup_environment import (
    setup_environment_variables,
    _execute_startup_script,
    _export_env_variables,
    _is_protected_os_environ,
)
from mwaa.subprocess.subprocess import Subprocess
from mwaa.logging.cloudwatch_handlers import BaseLogHandler

print(BaseLogHandler.__init__.__code__.co_varnames)

@pytest.fixture
def mock_handler():
    return Mock()


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
