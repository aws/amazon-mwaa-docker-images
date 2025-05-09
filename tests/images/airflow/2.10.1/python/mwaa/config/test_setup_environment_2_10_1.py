# test_setup_environment.py
import pytest
import json
import os
from unittest.mock import patch, mock_open, MagicMock
from datetime import timedelta
from mwaa.config.setup_environment import (
    setup_environment_variables,
    _execute_startup_script,
    _export_env_variables,
    _is_protected_os_environ,
)
from mwaa.subprocess.subprocess import Subprocess  # Add this import


@pytest.fixture
def mock_environ(monkeypatch):
    """Fixture for mocking os.environ"""
    test_environ = {
        "AIRFLOW_ENV_ID": "test-env-id",
        "AIRFLOW_ENV_NAME": "test-env",
        "AIRFLOW_HOME": "/usr/local/airflow",
        "AWS_DEFAULT_REGION": "us-east-1",
        "PYTHONPATH": "/usr/local/airflow/dags",
        "MWAA__CORE__TEST": "test-value",
        "MWAA__CORE__STARTUP_SCRIPT_PATH": "../../2.10.1/startup/startup.sh",
        "MWAA__CORE__TESTING_MODE": "true"
    }
    monkeypatch.setattr(os, "environ", test_environ)
    return test_environ


@pytest.fixture
def mock_config_functions(monkeypatch):
    """Fixture for mocking all configuration related functions"""
    mock_functions = {
        "get_essential_airflow_config": MagicMock(return_value={"ESSENTIAL_CONFIG": "value1"}),
        "get_opinionated_airflow_config": MagicMock(return_value={"OP_CONFIG": "value2"}),
        "get_essential_environ": MagicMock(return_value={"ESSENTIAL_ENV": "value3"}),
        "get_opinionated_environ": MagicMock(return_value={"OP_ENV": "value4"}),
        "get_user_airflow_config": MagicMock(return_value={"USER_CONFIG": "value5"})
    }

    for func_name, mock_func in mock_functions.items():
        monkeypatch.setattr(f"mwaa.config.setup_environment.{func_name}", mock_func)

    return mock_functions


@pytest.fixture
def temp_home_dir(tmp_path):
    """Fixture for creating temporary home directory with .bashrc and .bash_profile"""
    bashrc = tmp_path / ".bashrc"
    bash_profile = tmp_path / ".bash_profile"
    bashrc.touch()
    bash_profile.touch()
    return tmp_path


# ------------------------
# Environment Configuration Tests
# ------------------------

@pytest.mark.parametrize(
    "key,expected",
    [
        ("MWAA__CORE__TEST", True),
        ("AIRFLOW_ENV_ID", True),
        ("AWS_DEFAULT_REGION", True),
        ("CUSTOM_VAR", False),
        ("PYTHONPATH", True),
        ("RANDOM_VAR", False),
        ("MWAA__CUSTOM__VAR", True),
    ]
)
def test_is_protected_os_environ(key, expected):
    """Test protected environment variable checking"""
    assert _is_protected_os_environ(key) == expected


def test_export_env_variables(temp_home_dir):
    """Test environment variable export to shell files"""
    test_environ = {
        "TEST_VAR1": "value1",
        "TEST_VAR2": "value2 with space",
        "TEST_VAR3": "value3!@#$"
    }

    with patch("os.path.expanduser", return_value=str(temp_home_dir)):
        _export_env_variables(test_environ)

    # Verify both files
    for filename in [".bashrc", ".bash_profile"]:
        file_path = temp_home_dir / filename
        content = file_path.read_text()

        assert 'export TEST_VAR1=value1\n' in content
        assert "export TEST_VAR2='value2 with space'\n" in content
        assert "export TEST_VAR3='value3!@#$'\n" in content


def test_setup_environment_variables(mock_environ, mock_config_functions):
    """Test complete environment variable setup"""
    with patch("mwaa.config.setup_environment._execute_startup_script") as mock_execute_script, \
            patch("mwaa.config.setup_environment._export_env_variables") as mock_export:
        mock_execute_script.return_value = {"STARTUP_VAR": "value6"}

        result = setup_environment_variables("worker", "Local")

        # Verify all configurations are present and correctly merged
        assert result["ESSENTIAL_CONFIG"] == "value1"
        assert result["OP_CONFIG"] == "value2"
        assert result["ESSENTIAL_ENV"] == "value3"
        assert result["OP_ENV"] == "value4"
        assert result["USER_CONFIG"] == "value5"
        assert result["STARTUP_VAR"] == "value6"

        # Verify protected variables are preserved
        assert result["AIRFLOW_ENV_ID"] == "test-env-id"
        assert result["AIRFLOW_HOME"] == "/usr/local/airflow"
        assert result["AWS_DEFAULT_REGION"] == "us-east-1"

        # Verify export was called
        mock_export.assert_called_once_with(result)


# ------------------------
# Startup Script Tests
# ------------------------

def test_no_startup_script_path():
    """Test when MWAA__CORE__STARTUP_SCRIPT_PATH is not set"""
    with patch.dict(os.environ, {}, clear=True):
        result = _execute_startup_script("worker", {})
        assert result == {}


def test_startup_script_not_found():
    """Test when startup script file doesn't exist"""
    with patch.dict(os.environ, {"MWAA__CORE__STARTUP_SCRIPT_PATH": "/nonexistent/path"}), \
            patch('os.path.isfile', return_value=False):
        result = _execute_startup_script("worker", {})
        assert result == {}


def test_uses_mocked_env(mock_environ):
    """Ensure environment variables are correctly set"""
    assert mock_environ["MWAA__CORE__TESTING_MODE"] == "true"


@pytest.mark.parametrize("cmd,expected_logger", [
    ("worker", "worker_startup"),
    ("scheduler", "scheduler_startup"),
    ("hybrid", "worker_startup")
])
def test_startup_script_execution(cmd, expected_logger, mock_environ):
    """Test successful startup script execution with different commands"""
    customer_env = {"CUSTOM_VAR": "value"}

    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.config.setup_environment.Subprocess') as MockSubprocess:
        # Configure mock subprocess
        mock_process = MagicMock()
        mock_process.start.return_value = 0
        MockSubprocess.return_value = mock_process

        with patch('builtins.open', mock_open(read_data='{"CUSTOM_VAR": "value"}')):
            result = _execute_startup_script(cmd, mock_environ)

            assert result == customer_env
            # Verify Subprocess was instantiated twice (for script and verification)
            assert MockSubprocess.call_count == 2
            # Verify start was called on each instance
            assert mock_process.start.call_count == 2


def test_failed_env_vars_reading(mock_environ):
    """Test when reading customer environment variables fails"""
    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.config.setup_environment.Subprocess') as MockSubprocess, \
            patch('builtins.open') as mock_file:
        # Configure mock subprocess
        mock_process = MagicMock()
        mock_process.start.return_value = 0
        MockSubprocess.return_value = mock_process

        # Configure file mock to raise exception
        mock_file.side_effect = Exception("Failed to read file")

        with pytest.raises(Exception) as exc_info:
            _execute_startup_script("worker", mock_environ)

        assert ("Failed to read customer's environment variables from startup script: "
                "Failed to read file") in str(exc_info.value)



def test_missing_customer_env_vars_file(mock_environ):
    """Test when customer environment variables file is not created"""
    with patch('os.path.isfile') as mock_isfile, \
            patch('mwaa.config.setup_environment.Subprocess') as MockSubprocess:
        # Configure mock subprocess
        mock_process = MagicMock()
        mock_process.start.return_value = 0
        MockSubprocess.return_value = mock_process

        # First True for startup script, second False for env vars file
        mock_isfile.side_effect = [True, False]

        with pytest.raises(Exception) as exc_info:
            _execute_startup_script("worker", mock_environ)

        assert ("Failed to access customer environment variables file: Service was unable "
                "to create or locate /tmp/customer_env_vars.json") in str(exc_info.value)
        assert MockSubprocess.call_count == 2


@pytest.mark.parametrize("subprocess_error", [
    Exception("Process failed"),
    TimeoutError("Process timed out")
])
def test_subprocess_execution_errors(subprocess_error, mock_environ):
    """Test handling of subprocess execution errors"""
    with patch('os.path.isfile', return_value=True), \
            patch('mwaa.config.setup_environment.Subprocess') as MockSubprocess:
        # Configure mock subprocess to raise error
        mock_process = MagicMock()
        mock_process.start.side_effect = subprocess_error
        MockSubprocess.return_value = mock_process

        with pytest.raises(type(subprocess_error)) as exc_info:
            _execute_startup_script("worker", mock_environ)

        assert str(subprocess_error) == str(exc_info.value)


@pytest.mark.parametrize(
    "command,executor_type",
    [
        ("worker", "Local"),
        ("scheduler", "Celery"),
        ("webserver", "Local"),
        ("hybrid", "Celery")
    ]
)
def test_setup_environment_variables_different_commands(
        command,
        executor_type,
        mock_environ,
        mock_config_functions
):
    """Test environment setup with different commands and executor types"""
    with patch("mwaa.config.setup_environment._execute_startup_script") as mock_execute_script:
        mock_execute_script.return_value = {}

        result = setup_environment_variables(command, executor_type)

        # Verify essential configurations are present
        assert "ESSENTIAL_CONFIG" in result
        assert "ESSENTIAL_ENV" in result

        # Verify the execute_startup_script was called correctly
        mock_execute_script.assert_called_once()
        args, _ = mock_execute_script.call_args
        assert args[0] == command
        # Verify the environment dict was passed (without checking exact contents)
        assert isinstance(args[1], dict)
