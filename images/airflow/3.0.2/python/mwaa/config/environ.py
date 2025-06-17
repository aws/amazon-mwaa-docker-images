"""
Contains functions for retrieving environment variables required to run the containers.
"""

# Python imports
import os
from pathlib import Path
from typing import Dict

# Our imports
from mwaa.logging.config import get_mwaa_logging_env_vars


def get_essential_environ(command: str) -> Dict[str, str]:
    """
    Retrieves the list of essential environment variables that we add to the container.

    The essential environment variables are those that we don't allow the user to
    override.
    """

    airflow_version = os.environ[
        "AIRFLOW_VERSION"  # AIRFLOW_VERSION is defined in the Dockerfile.
    ]

    return {
        "AWS_EXECUTION_ENV": f"Amazon_MWAA_{airflow_version.replace('.', '')}",
        "MWAA_AIRFLOW_COMPONENT": command,
        "MWAA_COMMAND": command,
    }


def get_opinionated_environ() -> Dict[str, str]:
    """
    Retrieves the list of opinionated environment variables that we add to the container.

    The opinionated environment variables are those that we think are good for the
    user but can be overridden if needed.
    """

    _, webserver_log_level, _ = get_mwaa_logging_env_vars("webserver")

    gunicorn_cmd_args = (
        {
            "GUNICORN_CMD_ARGS": f"--log-level {webserver_log_level}",
        }
        if webserver_log_level
        else {}
    )

    return {
        "AIRFLOW_CONN_AWS_DEFAULT": "aws://",
        "CLASSPATH": str(Path.home() / "/dags/classpath/*"),
        "SQLALCHEMY_SILENCE_UBER_WARNING": "1",
        **gunicorn_cmd_args,
    }
