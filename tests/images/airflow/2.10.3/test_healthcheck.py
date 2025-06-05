import os
import sys
import pytest
from unittest.mock import patch, mock_open
import subprocess

# Add the directory containing healthcheck.py to the Python path
AIRFLOW_VERSION = "2.10.3"
HEALTHCHECK_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "images",
        "airflow",
        AIRFLOW_VERSION
    )
)
sys.path.insert(0, HEALTHCHECK_DIR)

from healthcheck import ExitStatus, is_process_running, get_airflow_process_command, exit_with_status

# Mock process output for different scenarios
MOCK_PS_OUTPUT_WITH_SCHEDULER = """
USER       PID  %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
airflow    123   0.0  0.2 /usr/local/airflow/.local/bin/airflow scheduler
root       456   0.0  0.1 /usr/sbin/sshd
"""

MOCK_PS_OUTPUT_WITH_WORKER = """
USER       PID  %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
airflow    789   0.0  0.2 MainProcess] -active- (celery worker)
root       456   0.0  0.1 /usr/sbin/sshd
"""

MOCK_PS_OUTPUT_EMPTY = """
USER       PID  %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root       456   0.0  0.1 /usr/sbin/sshd
"""

@pytest.mark.parametrize("airflow_component,expected_cmd", [
    ("SCHEDULER", "/usr/local/airflow/.local/bin/airflow scheduler"),
    ("WORKER", "MainProcess] -active- (celery worker)"),
    ("STATIC_ADDITIONAL_WORKER", "MainProcess] -active- (celery worker)"),
    ("DYNAMIC_ADDITIONAL_WORKER", "MainProcess] -active- (celery worker)"),
    ("ADDITIONAL_WEBSERVER", "/usr/local/airflow/.local/bin/airflow webserver"),
    ("WEB_SERVER", "/usr/local/airflow/.local/bin/airflow webserver"),
])
def test_get_airflow_process_command_valid(airflow_component, expected_cmd):
    assert get_airflow_process_command(airflow_component) == expected_cmd

def test_get_airflow_process_command_invalid():
    with pytest.raises(SystemExit) as exc_info:
        get_airflow_process_command("INVALID_COMPONENT")
    assert exc_info.value.code == ExitStatus.INVALID_AIRFLOW_COMPONENT.value

@pytest.mark.parametrize("ps_output,cmd_substring,expected_result", [
    (MOCK_PS_OUTPUT_WITH_SCHEDULER, "/usr/local/airflow/.local/bin/airflow scheduler", True),
    (MOCK_PS_OUTPUT_WITH_WORKER, "MainProcess] -active- (celery worker)", True),
    (MOCK_PS_OUTPUT_EMPTY, "/usr/local/airflow/.local/bin/airflow scheduler", False),
    (MOCK_PS_OUTPUT_EMPTY, "MainProcess] -active- (celery worker)", False),
])
def test_is_process_running(ps_output, cmd_substring, expected_result):
    with patch('subprocess.check_output') as mock_check_output:
        mock_check_output.return_value = ps_output.encode()
        assert is_process_running(cmd_substring) == expected_result

def test_is_process_running_subprocess_error():
    with patch('subprocess.check_output') as mock_check_output:
        mock_check_output.side_effect = subprocess.SubprocessError()
        assert not is_process_running("any_command")

@pytest.mark.parametrize("exit_status", [
    ExitStatus.SUCCESS,
    ExitStatus.INSUFFICIENT_ARGUMENTS,
    ExitStatus.INVALID_AIRFLOW_COMPONENT,
    ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY,
])
def test_exit_with_status(exit_status):
    with pytest.raises(SystemExit) as exc_info:
        exit_with_status(exit_status)
    assert exc_info.value.code == exit_status.value

def run_healthcheck(argv, container_unhealthy=False, ps_output=MOCK_PS_OUTPUT_EMPTY):
    """Helper function to run the main healthcheck logic"""
    with patch('sys.argv', argv), \
         patch('os.path.exists') as mock_exists, \
         patch('subprocess.check_output') as mock_check_output:
        
        mock_exists.return_value = container_unhealthy
        mock_check_output.return_value = ps_output.encode()
        
        # Import the module to get access to the main block code
        import healthcheck
        
        # Create a copy of the main block code
        if container_unhealthy:
            return ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY
            
        if len(argv) != 2:
            return ExitStatus.INSUFFICIENT_ARGUMENTS
            
        airflow_component = argv[1]
        airflow_cmd_substring = get_airflow_process_command(airflow_component)
        
        if is_process_running(airflow_cmd_substring):
            return ExitStatus.SUCCESS
        else:
            return ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY

@pytest.mark.parametrize("argv,container_unhealthy,ps_output,expected_exit_code", [
    (["script.py", "SCHEDULER"], False, MOCK_PS_OUTPUT_WITH_SCHEDULER, ExitStatus.SUCCESS),
    (["script.py", "WORKER"], False, MOCK_PS_OUTPUT_WITH_WORKER, ExitStatus.SUCCESS),
    (["script.py", "SCHEDULER"], False, MOCK_PS_OUTPUT_EMPTY, ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY),
    (["script.py"], False, MOCK_PS_OUTPUT_WITH_SCHEDULER, ExitStatus.INSUFFICIENT_ARGUMENTS),
    (["script.py", "SCHEDULER"], True, MOCK_PS_OUTPUT_WITH_SCHEDULER, ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY),
])
def test_main_flow(argv, container_unhealthy, ps_output, expected_exit_code):
    result = run_healthcheck(argv, container_unhealthy, ps_output)
    assert result == expected_exit_code
