import os
import sys
import pytest
from unittest.mock import patch

# Add the directory containing healthcheck.py to the Python path
AIRFLOW_VERSION = "3.0.6"
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
MOCK_PS_OUTPUT_WITH_SCHEDULER = "airflow    123   0.0  0.2 /usr/local/airflow/.local/bin/airflow scheduler"
# 3.x uses api_server instead of the webserver
MOCK_PS_OUTPUT_WITH_WEBSERVER = "airflow    124   0.0  0.2 airflow api_server"
MOCK_PS_OUTPUT_WITH_WORKER = "airflow     36  4.1  2.2 506068 176924 ?       Ss   09:51   0:09 [celeryd: celery@9011bf25155e:MainProcess] -active- (celery worker)"
MOCK_PS_OUTPUT_WITH_WORKER_FALLBACK_TITLE = "airflow     53 68.6  2.1 506084 176492 ?       Ss   09:58   0:05 /usr/local/bin/python3.11 /usr/local/airflow/.local/bin/airflow celery worker"
MOCK_PS_OUTPUT_EMPTY = "root       456   0.0  0.1 /usr/sbin/sshd"

def test_main_success_scheduler():
    """Test main function success path for scheduler"""
    with patch('sys.argv', ['healthcheck.py', 'SCHEDULER']), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_WITH_SCHEDULER.encode()), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.SUCCESS.value

@pytest.mark.parametrize("webserver_component", [
    "WEB_SERVER",
    "ADDITIONAL_WEBSERVER"
])
def test_main_success_webserver(webserver_component):
    """Test main function success path for webserver components"""
    with patch('sys.argv', ['healthcheck.py', webserver_component]), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_WITH_WEBSERVER.encode()), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.SUCCESS.value

@pytest.mark.parametrize("worker_component", [
    "WORKER",
    "STATIC_ADDITIONAL_WORKER",
    "DYNAMIC_ADDITIONAL_WORKER"
])
def test_main_success_worker_with_mainprocess(worker_component):
    """Test main function success path for worker with MainProcess string"""
    with patch('sys.argv', ['healthcheck.py', worker_component]), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_WITH_WORKER.encode()), \
         pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == ExitStatus.SUCCESS.value

@pytest.mark.parametrize("worker_component", [
    "WORKER",
    "STATIC_ADDITIONAL_WORKER",
    "DYNAMIC_ADDITIONAL_WORKER"
])
def test_main_success_worker_with_fallback_title(worker_component):
    """Test main function success path for worker with fallback title"""
    with patch('sys.argv', ['healthcheck.py', worker_component]), \
         patch('os.path.exists', return_value=False), \
         patch('subprocess.check_output', return_value=MOCK_PS_OUTPUT_WITH_WORKER_FALLBACK_TITLE.encode()), \
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

@pytest.mark.parametrize("component", [
    "SCHEDULER",
    "WORKER",
    "STATIC_ADDITIONAL_WORKER",
    "DYNAMIC_ADDITIONAL_WORKER",
    "WEB_SERVER",
    "ADDITIONAL_WEBSERVER"
])
def test_main_process_not_found(component):
    """Test when process is not found"""
    with patch('sys.argv', ['healthcheck.py', component]), \
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
