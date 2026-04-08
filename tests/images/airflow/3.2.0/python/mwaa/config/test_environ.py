import os
from unittest.mock import patch
import pytest

from mwaa.config.environ import get_essential_environ, get_opinionated_environ

def test_get_essential_environ_sets_correct_vars(env_helper):
    # Set the AIRFLOW_VERSION env var
    env_helper.set({"AIRFLOW_VERSION": "3.1.6"})

    command = "webserver"
    result = get_essential_environ(command)

    assert result["MWAA_AIRFLOW_COMPONENT"] == command
    assert result["MWAA_COMMAND"] == command
    assert result["AWS_EXECUTION_ENV"] == "Amazon_MWAA_316"


def test_get_opinionated_environ_includes_logging_args():
    # Patch get_mwaa_logging_env_vars to return a custom log level
    with patch(
        "mwaa.config.environ.get_mwaa_logging_env_vars",
        return_value=(None, "DEBUG", None)
    ):
        result = get_opinionated_environ()

    # Gunicorn args should include the mocked log level
    assert "GUNICORN_CMD_ARGS" in result
    assert result["GUNICORN_CMD_ARGS"].endswith("DEBUG")

    # Other defaults should be present
    assert result["AIRFLOW_CONN_AWS_DEFAULT"] == "aws://"
    assert "CLASSPATH" in result
    assert result["SQLALCHEMY_SILENCE_UBER_WARNING"] == "1"