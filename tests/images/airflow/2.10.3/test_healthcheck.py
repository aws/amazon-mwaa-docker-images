import os
import sys
import pytest
from unittest.mock import patch

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

from healthcheck import main, ExitStatus

# Simple mock process outputs
MOCK_PS_OUTPUT_WITH_PROCESS = "airflow    123   0.0  0.2 /usr/local/airflow/.local/bin/airflow scheduler"
MOCK_PS_OUTPUT_EMPTY = "root       456   0.0  0.1 /usr/sbin/sshd"

def test_main_success():
    """Test main function success path"""
    with patch('sys.argv', ['healthcheck.py', 'SCHEDULER']), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_WITH_PROCESS.encode()), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.SUCCESS.value

def test_main_unhealthy_container():
    """Test when container is marked unhealthy"""
    with patch('sys.argv', ['healthcheck.py', 'SCHEDULER']), \
         patch('os.path.exists', return_value=True), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY.value

def test_main_process_not_found():
    """Test when process is not found"""
    with patch('sys.argv', ['healthcheck.py', 'SCHEDULER']), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_EMPTY.encode()), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY.value

def test_main_invalid_args():
    """Test with invalid number of arguments"""
    with patch('sys.argv', ['healthcheck.py']), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.INSUFFICIENT_ARGUMENTS.value

def test_main_invalid_component():
    """Test with invalid component name"""
    with patch('sys.argv', ['healthcheck.py', 'INVALID']), \
         patch('os.path.exists', return_value=False), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.INVALID_AIRFLOW_COMPONENT.value
