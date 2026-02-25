import pytest
from unittest.mock import patch, Mock, call
import os
from io import BytesIO
from subprocess import Popen
from mwaa.subprocess.subprocess import Subprocess, _SUBPROCESS_LOG_POLL_IDLE_SLEEP_INTERVAL
import time

@pytest.fixture
def subprocess_instance():
    """Fixture to create a Subprocess instance with mock command"""
    return Subprocess(cmd="test_command")

@pytest.fixture
def mock_process(mocker):
    """Fixture to create a mock process"""
    process = mocker.Mock(spec=Popen)
    process.poll.side_effect = [None, None, 0]  # Process runs for 2 iterations then ends
    process.stdout = mocker.Mock()
    process.stdout.readline.return_value = b'test output\n'
    return process

@pytest.fixture
def mock_logger(mocker):
    """Fixture to create a mock logger"""
    return mocker.Mock()


def test_read_subprocess_log_stream_empty_stream(subprocess_instance):
    """Test behavior when stream is None"""
    # Create mock process with None stdout
    mock_process = Mock(spec=Popen)
    mock_process.stdout = None

    subprocess_instance.process_logger = Mock()
    subprocess_instance._read_subprocess_log_stream(mock_process)

    # Assert no logging calls were made
    subprocess_instance.process_logger.info.assert_not_called()
    subprocess_instance.process_logger.error.assert_not_called()
    subprocess_instance.process_logger.warning.assert_not_called()


def test_read_subprocess_log_stream_process_running(mocker, subprocess_instance, mock_process, mock_logger):
    """Test that read_subprocess_log_stream correctly handles a running process"""
    # Setup
    subprocess_instance.process_logger = mock_logger
    mock_process.stdout.closed = False

    # Simulate the following sequence:
    # 1. Read output line, process running
    # 2. Read empty line, process running (triggers sleep)
    # 3. Read empty line, process finished (breaks loop)
    mock_process.poll.side_effect = [None, None, 0]
    mock_process.stdout.readline.side_effect = [
        b'test output\n',
        b'',
        b'',
        b''
    ]

    # Act
    with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'INFO'}):
        subprocess_instance._read_subprocess_log_stream(mock_process)

    # Assert
    assert mock_process.poll.call_count == 3
    mock_logger.info.assert_called_once_with('test output')


def test_read_subprocess_log_stream_closed_stream(subprocess_instance):
    """Test behavior when stream is closed"""
    # Create and configure mock process
    mock_process = Mock(spec=Popen)
    mock_process.stdout = Mock()
    mock_process.stdout.closed = True

    subprocess_instance.process_logger = Mock()
    subprocess_instance._read_subprocess_log_stream(mock_process)

    subprocess_instance.process_logger.info.assert_not_called()


def test_read_subprocess_log_stream_process_terminated(subprocess_instance):
    """Test behavior when process has terminated"""
    # Create and configure mock process
    mock_process = Mock(spec=Popen)
    mock_process.stdout = BytesIO(b"final message\n")
    mock_process.poll.return_value = 0

    subprocess_instance.process_logger = Mock()

    with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'INFO'}):
        subprocess_instance._read_subprocess_log_stream(mock_process)

    subprocess_instance.process_logger.info.assert_called_once_with("final message")


def test_read_subprocess_log_stream_process_error(mocker, subprocess_instance, mock_process, mock_logger):
    """Test that read_subprocess_log_stream correctly handles error log level"""
    # Setup
    subprocess_instance.process_logger = mock_logger
    mock_process.stdout.closed = False

    # Simulate the following sequence:
    # 1. Read output line, process running
    # 2. Read empty line, process running (triggers sleep)
    # 3. Read empty line, process finished (breaks loop)
    mock_process.poll.side_effect = [None, None, 0]
    mock_process.stdout.readline.side_effect = [
        b'test output\n',
        b'',
        b'',
        b''
    ]

    # Act
    with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'ERROR'}):
        subprocess_instance._read_subprocess_log_stream(mock_process)

    # Assert
    assert mock_process.poll.call_count == 3
    mock_logger.error.assert_called_once_with('test output')

def test_read_subprocess_log_stream_process_warning(mocker, subprocess_instance, mock_process, mock_logger):
    """Test that read_subprocess_log_stream correctly handles warning log level"""
    # Setup
    subprocess_instance.process_logger = mock_logger
    mock_process.stdout.closed = False

    # Simulate the following sequence:
    # 1. Read output line, process running
    # 2. Read empty line, process running (triggers sleep)
    # 3. Read empty line, process finished (breaks loop)
    mock_process.poll.side_effect = [None, None, 0]
    mock_process.stdout.readline.side_effect = [
        b'test output\n',
        b'',
        b'',
        b''
    ]

    # Act
    with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'WARNING'}):
        subprocess_instance._read_subprocess_log_stream(mock_process)

    # Assert
    assert mock_process.poll.call_count == 3
    mock_logger.warning.assert_called_once_with('test output')