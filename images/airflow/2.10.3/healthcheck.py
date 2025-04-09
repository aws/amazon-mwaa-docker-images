"""
Module for running healthcheck on airflow processes.
"""

import subprocess
import sys
from enum import Enum

class ExitStatus(Enum):
    """
    Enum to map failure reason to exit codes.
    """
    SUCCESS=0
    INSUFFICIENT_ARGUMENTS=1
    INVALID_AIRFLOW_COMPONENT=2
    AIRFLOW_COMPONENT_UNHEALTHY=3

def exit_with_status(exit_status: ExitStatus):
    """
    Exit script with provided exit status.
    """
    print(f"Exiting with status: {exit_status.name}")
    sys.exit(exit_status.value)

def is_process_running(cmd_substring: str) -> bool:
    """
    Check if a process containing the given command substring is running.

    :param cmd_substring: Substring of the command used to launch the process.
    :return: True if a matching process is found, False otherwise.
    """
    try:
        procs = subprocess.check_output(['ps', 'uaxw']).decode().splitlines()
        for proc in procs:
            if cmd_substring in proc:
                print(f"Process found: {proc}")
                return True
    except subprocess.SubprocessError as e:
        print(f"Error checking processes: {e}")
    return False

def get_airflow_process_command(airflow_component: str):
    """
    Get airflow command substring for a given airflow component.

    :param airflow_component: Airflow component running on host. 
    :return: Airflow command substring.
    """
    if airflow_component == "SCHEDULER":
        return "/usr/local/airflow/.local/bin/airflow scheduler"

    if airflow_component == "WORKER":
        return "MainProcess] -active- (celery worker)"

    if airflow_component == "STATIC_ADDITIONAL_WORKER":
        return "MainProcess] -active- (celery worker)"

    if airflow_component == "DYNAMIC_ADDITIONAL_WORKER":
        return "MainProcess] -active- (celery worker)"

    if airflow_component == "ADDITIONAL_WEBSERVER":
        return "/usr/local/airflow/.local/bin/airflow webserver"

    if airflow_component == "WEB_SERVER":
        return "/usr/local/airflow/.local/bin/airflow webserver"

    exit_with_status(ExitStatus.INVALID_AIRFLOW_COMPONENT)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 healthcheck.py <airflow_component>")
        exit_with_status(ExitStatus.INSUFFICIENT_ARGUMENTS)

    airflow_component = sys.argv[1]
    airflow_cmd_substring = get_airflow_process_command(airflow_component)
    if is_process_running(airflow_cmd_substring):
        print(f"Airflow process {airflow_component} is running.")
        exit_with_status(ExitStatus.SUCCESS)
    else:
        print(f"Airflow process {airflow_component} not found.")
        exit_with_status(ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY)


