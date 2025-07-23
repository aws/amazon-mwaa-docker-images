import pytest
from unittest.mock import patch, Mock, call
import os
from io import BytesIO
from subprocess import Popen
from mwaa.subprocess.subprocess import Subprocess
import time

@pytest.fixture
def subprocess_instance():
    """Fixture to create a Subprocess instance with mock command"""
    return Subprocess(cmd="test_command")


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


def test_read_subprocess_log_stream_process_running(self, mocker, mock_process, mock_logger):
    """
    Test that read_subprocess_log_stream correctly handles a running process
    """
    # Arrange
    mock_sleep = mocker.patch('time.sleep')
    expected_calls = [call(1)]  # Expect sleep to be called with 1 second delay

    # Act
    read_subprocess_log_stream(
        process=mock_process,
        logger=mock_logger
    )

    # Assert
    mock_sleep.assert_called_once()
    assert mock_sleep.call_args_list == expected_calls
    assert mock_process.poll.call_count == 3  # Called until process ends
    mock_logger.info.assert_called_with('test output')


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

    subprocess_instance.process_logger.info.assert_called_once_with("final message\n")

def read_subprocess_log_stream(process, logger):
    """
    Read and log output from a subprocess stream

    Args:
        process: subprocess.Popen instance
        logger: logging.Logger instance
    """
    try:
        while process.poll() is None:
            output = process.stdout.readline()
            if output:
                # Decode bytes to string and strip whitespace
                log_line = output.decode('utf-8').strip()
                if log_line:
                    logger.info(log_line)
            else:
                time.sleep(1)
    except Exception as e:
        logger.error(f"Error reading subprocess output: {str(e)}")
        raise