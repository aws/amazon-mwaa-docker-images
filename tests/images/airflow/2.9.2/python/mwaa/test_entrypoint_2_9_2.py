import os
import pytest
import mwaa.entrypoint as entrypoint

from unittest.mock import patch, mock_open, MagicMock

# # ------------------------
# # Fixtures
# # ------------------------
@pytest.fixture
def mock_environ():
    return {
        "MWAA__CORE__STARTUP_SCRIPT_PATH": "../../2.9.2/startup/startup.sh",
    }

# ------------------------
# Test Cases
# ------------------------
def test_no_startup_script_path():
    """Test when MWAA__CORE__STARTUP_SCRIPT_PATH is not set"""
    with patch.dict(os.environ, {}, clear=True):
        result = entrypoint.execute_startup_script("worker", {})
        assert result == {}


def test_startup_script_not_found():
    """Test when startup script file doesn't exist"""
    with patch.dict(os.environ, {"MWAA__CORE__STARTUP_SCRIPT_PATH": "/nonexistent/path"}), \
            patch('os.path.isfile', return_value=False):
        result = entrypoint.execute_startup_script("worker", {})
        assert result == {}


def test_uses_mocked_env():
    """Ensure environment variables are correctly set"""
    assert "true" in os.environ["TESTING_MODE"]


@pytest.mark.parametrize("cmd,expected_logger", [
    ("worker", "worker_startup"),
    ("scheduler", "scheduler_startup"),
    ("hybrid", "worker_startup")
])
def test_startup_script_execution(cmd, expected_logger, mock_environ):
    """Test successful startup script execution with different commands"""
    customer_env = {"CUSTOM_VAR": "value"}

    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.entrypoint.Subprocess') as mock_subprocess, \
            patch('builtins.open', mock_open(read_data='{"CUSTOM_VAR": "value"}')):
        mock_process = MagicMock()
        mock_subprocess.return_value = mock_process

        result = entrypoint.execute_startup_script(cmd, mock_environ)
        assert result == customer_env


def test_failed_env_vars_reading(mock_environ):
    """Test when reading customer environment variables fails"""
    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.entrypoint.Subprocess') as mock_subprocess, \
            patch('builtins.open', mock_open()) as mock_file:
        mock_file.side_effect = Exception("Failed to read file")
        mock_process = MagicMock()
        mock_subprocess.return_value = mock_process

        with pytest.raises(Exception) as exc_info:
            entrypoint.execute_startup_script("worker", mock_environ)

        assert "Failed to read customer's environment variables" in str(exc_info.value)


def test_missing_customer_env_vars_file(mock_environ):
    """Test when customer environment variables file is not created"""
    with patch('os.path.isfile') as mock_isfile, \
            patch('mwaa.entrypoint.Subprocess') as mock_subprocess:
        mock_isfile.side_effect = [True, False]
        mock_process = MagicMock()
        mock_subprocess.return_value = mock_process

        result = entrypoint.execute_startup_script("worker", mock_environ)
        assert result == {}


@pytest.mark.parametrize("subprocess_error", [
    Exception("Process failed"),
    TimeoutError("Process timed out")
])
def test_subprocess_execution_errors(subprocess_error, mock_environ):
    """Test handling of subprocess execution errors"""
    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.entrypoint.Subprocess') as mock_subprocess:
        mock_process = MagicMock()
        mock_process.start.side_effect = subprocess_error
        mock_subprocess.return_value = mock_process

        with pytest.raises(Exception):
            entrypoint.execute_startup_script("worker", mock_environ)
