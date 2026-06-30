import pytest
import os
from unittest.mock import patch, MagicMock, mock_open

# Set CONTAINER_START_TIME before importing the module
TEST_CONTAINER_START_TIME = 1234567890.0
with patch.dict('os.environ', {'CONTAINER_START_TIME': str(TEST_CONTAINER_START_TIME)}):
    from mwaa.execute_command import (
        execute_command,
        HYBRID_WORKER_SIGTERM_PATIENCE_INTERVAL_DEFAULT
    )


# ------------------------
# Fixtures
# ------------------------
@pytest.fixture
def mock_environ():
    """Basic environment variables fixture"""
    return {
        "AIRFLOW_HOME": "/usr/local/airflow",
        "MWAA__CORE__TASK_MONITORING_ENABLED": "false",
        "MWAA__CORE__TERMINATE_IF_IDLE": "false",
        "MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED": "false",
        "MWAA__HEALTH_MONITORING__ENABLE_SIDECAR_HEALTH_MONITORING": "false",
        "MWAA__HYBRID_CONTAINER__SIGTERM_PATIENCE_INTERVAL": "150",
        "MWAA__DB__POSTGRES_HOST": "localhost",
        "MWAA__DB__POSTGRES_PORT": "5432",
        "MWAA__DB__POSTGRES_DB": "airflow",
        "MWAA__DB__POSTGRES_SSLMODE": "disable"
    }


@pytest.fixture
def mock_subprocess():
    """Mock Subprocess"""
    process = MagicMock()
    process.start.return_value = 0
    process.process = MagicMock()
    process.process.returncode = 0

    with patch('mwaa.subprocess.subprocess.Subprocess', return_value=process) as mock:
        yield mock


@pytest.fixture
def mock_run_subprocesses():
    """Mock run_subprocesses"""
    with patch('mwaa.execute_command.run_subprocesses') as mock:
        yield mock

def remove_celery_state():
    try:
        os.remove("/celery_state_")
    except FileNotFoundError:
        pass


@pytest.fixture
def mock_file_operations():
    """Mock file-related operations"""
    with patch("os.path.exists", return_value=False), \
         patch("builtins.open", mock_open()), \
         patch("os.remove"):
        yield


# ------------------------
# Test Cases
# ------------------------

def test_execute_command_shell(mock_environ):
    """Test shell command execution"""
    with patch('os.execlpe') as mock_exec:
        execute_command("shell", mock_environ, TEST_CONTAINER_START_TIME)
        mock_exec.assert_called_once_with("/bin/bash", "/bin/bash", mock_environ)


def test_execute_command_spy(mock_environ):
    """Test spy command execution"""
    with patch('time.sleep', side_effect=KeyboardInterrupt) as mock_sleep:
        with pytest.raises(KeyboardInterrupt):
            execute_command("spy", mock_environ, TEST_CONTAINER_START_TIME)
        mock_sleep.assert_called_once_with(1)


@pytest.mark.parametrize("command,expected_subprocesses", [
    ("scheduler", 3),  # scheduler, dag-processor, triggerer
    ("worker", 1),  # celery worker
    ("webserver", 1),  # webserver
    ("hybrid", 4),  # scheduler, dag-processor, triggerer, worker
])
def test_execute_command_airflow_components(command, expected_subprocesses, mock_environ, mock_run_subprocesses):
    """Test execution of different Airflow components"""
    execute_command(command, mock_environ, TEST_CONTAINER_START_TIME)
    assert mock_run_subprocesses.called
    subprocess_calls = mock_run_subprocesses.call_args[0][0]
    assert len(subprocess_calls) == expected_subprocesses


def test_execute_command_invalid(mock_environ):
    """Test invalid command execution"""
    with pytest.raises(ValueError, match="Invalid command"):
        execute_command("invalid", mock_environ, TEST_CONTAINER_START_TIME)


def test_execute_command_worker_idle_termination(mock_environ, mock_run_subprocesses):
    """Test worker with idle termination enabled"""
    with patch.dict(os.environ, {"MWAA__CORE__TERMINATE_IF_IDLE": "true"}):
        execute_command("worker", mock_environ, TEST_CONTAINER_START_TIME)
    mock_run_subprocesses.assert_called()


def test_execute_command_worker_signal_handling(mock_environ, mock_run_subprocesses):
    """Test worker with signal handling enabled"""
    with patch.dict(os.environ, {"MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED": "true"}):
        execute_command("worker", mock_environ, TEST_CONTAINER_START_TIME)
    mock_run_subprocesses.assert_called()


def test_execute_command_container_start_time(mock_environ, mock_run_subprocesses):
    """Test container start time is properly set"""
    test_time = 9876543210.0
    execute_command("scheduler", mock_environ, test_time)
    from mwaa.execute_command import CONTAINER_START_TIME
    assert CONTAINER_START_TIME == test_time


@patch('mwaa.celery.task_monitor.WorkerTaskMonitor')
@patch('multiprocessing.shared_memory.SharedMemory')
def test_execute_command_task_monitoring_enabled(mock_shared_memory, mock_worker_monitor, mock_environ,
                                                 mock_run_subprocesses):
    """Test execute_command when task monitoring is enabled in environment variables"""
    # Setup the mocks
    mock_worker_instance = MagicMock()
    mock_worker_monitor.return_value = mock_worker_instance

    mock_shared_memory_instance = MagicMock()
    mock_shared_memory.return_value = mock_shared_memory_instance

    with patch.dict(os.environ, {"MWAA__CORE__TASK_MONITORING_ENABLED": "true"}, clear=True):
        execute_command("worker", mock_environ, TEST_CONTAINER_START_TIME)

    # Verify run_subprocesses was called
    mock_run_subprocesses.assert_called()



