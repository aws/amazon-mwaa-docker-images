"""
Module for running healthcheck on airflow processes.
"""

import subprocess
import sys
import os
from enum import Enum
from typing import List

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

def is_process_running(cmd_substrings: List[str]) -> bool:
    """
    Check if a process containing any of the given command substrings is running.

    :param cmd_substrings: A list of strings to search for in process commands.
    :return: True if a matching process is found, False otherwise.
    """
    try:
        procs = subprocess.check_output(['ps', 'uaxw']).decode().splitlines()
        for proc in procs:
            for substring in cmd_substrings:
                if substring in proc:
                    print(f"Process found: {proc}")
                    return True
    except subprocess.SubprocessError as e:
        print(f"Error checking processes: {e}")
    return False

def get_airflow_process_command(airflow_component: str) -> List[str]:
    """
    Get airflow command substring(s) for a given airflow component.

    :param airflow_component: Airflow component running on host.
    :return: List of airflow command substrings.
    """
    if airflow_component == "SCHEDULER":
        return ["/usr/local/airflow/.local/bin/airflow scheduler"]

    if airflow_component == "WORKER":
        return ["MainProcess] -active- (celery worker)", "/bin/airflow celery worker"]

    if airflow_component == "STATIC_ADDITIONAL_WORKER":
        return ["MainProcess] -active- (celery worker)", "/bin/airflow celery worker"]

    if airflow_component == "DYNAMIC_ADDITIONAL_WORKER":
        return ["MainProcess] -active- (celery worker)", "/bin/airflow celery worker"]

    # Use 'airflow api_server' instead of '/usr/local/airflow/.local/bin/airflow api-server' because setproctitle()
    # renames the process. See: airflow/cli/commands/api_server_command.py#L105 in Airflow source
    if airflow_component == "ADDITIONAL_WEBSERVER":
        return ["airflow api_server"]

    if airflow_component == "WEB_SERVER":
        return ["airflow api_server"]

    exit_with_status(ExitStatus.INVALID_AIRFLOW_COMPONENT)
    # This return will never be reached due to sys.exit() in exit_with_status
    return []  # Return empty list to satisfy type checker

def main():
    """
    Main function to check the health of Airflow components.
    """
    # Check if an 'container_unhealthy' marker file is present
    if os.path.exists("/tmp/mwaa/container_unhealthy"):
        print("/tmp/mwaa/container_unhealthy file found - marking as unhealthy.")
        exit_with_status(ExitStatus.AIRFLOW_COMPONENT_UNHEALTHY)

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

if __name__ == '__main__':
    main()
