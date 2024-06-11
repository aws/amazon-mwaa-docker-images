"""Contain functions for building Airflow configuration."""

# Python imports
from typing import Dict
import json
import logging
import os

# 3rd-party Imports
from mwaa.config.database import get_db_connection_string
from mwaa.config.sqs import get_sqs_endpoint, get_sqs_queue_name


logger = logging.getLogger(__name__)


def _get_essential_airflow_celery_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required for Celery executor.

    The required environment variables are mostly under the "celery" section, but
    other sections as well.

    :returns A dictionary containing the environment variables.
    """
    celery_config_module_path = "mwaa.config.celery.MWAA_CELERY_CONFIG"

    return {
        "AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT": "43200",
        "AIRFLOW__CELERY__BROKER_URL": get_sqs_endpoint(),
        "AIRFLOW__CELERY__CELERY_CONFIG_OPTIONS": celery_config_module_path,
        "AIRFLOW__CELERY__RESULT_BACKEND": f"db+{get_db_connection_string()}",
        "AIRFLOW__CELERY__WORKER_ENABLE_REMOTE_CONTROL": "False",
        # These two are not Celery configs per-se, but are used by the Celery executor.
        "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
        "AIRFLOW__OPERATORS__DEFAULT_QUEUE": get_sqs_queue_name(),
    }


def _get_essential_airflow_core_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "core" configuration section.

    :returns A dictionary containing the environment variables.
    """

    fernet_key = {}

    fernet_secret_json = os.environ.get("MWAA__CORE__FERNET_KEY")
    if fernet_secret_json:
        try:
            fernet_key = {
                "AIRFLOW__CORE__FERNET_KEY": json.loads(fernet_secret_json)["FernetKey"]
            }
        except:
            logger.warning(
                "Invalid value for fernet secret. Value not printed for security reasons.",
            )

    return {
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        **fernet_key,
    }


def get_user_airflow_config() -> Dict[str, str]:
    """
    Retrieve the user-defined environment variables for Airflow configuration.

    The user is able to specify additional Airflow configuration by using the
    `MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS`, which should be a JSON object, with key-value
    pairs pertaining to what Airflow configuration environment variables the user wants
    to set.

    :returns A dictionary containing the environment variables.
    """

    airflow_config = {}

    airflow_config_secret = os.environ.get("MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS")
    if airflow_config_secret:
        try:
            airflow_config = json.loads(airflow_config_secret)
        except:
            logger.warning(
                "Invalid value for Airflow config secret. Value not printed for security reasons.",
            )

    return {**airflow_config}


def _get_essential_airflow_db_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "database" configuration section.

    :returns A dictionary containing the environment variables.
    """
    conn_string = get_db_connection_string()
    return {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_string,
    }


def _get_essential_airflow_logging_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "logging" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS": "mwaa.logging.config.LOGGING_CONFIG",
    }


def _get_essential_airflow_metrics_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "metrics" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__METRICS__STATSD_ON": "True",
        "AIRFLOW__METRICS__STATSD_HOST": "localhost",
        "AIRFLOW__METRICS__STATSD_PORT": "8125",
        "AIRFLOW__METRICS__STATSD_PREFIX": "airflow",
        "AIRFLOW__METRICS__METRICS_BLOCK_LIST": "",
        "AIRFLOW__METRICS__METRICS_ALLOW_LIST": "",
    }


def _get_essential_airflow_scheduler_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "scheduler" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR": "True",
    }


def _get_opinionated_airflow_scheduler_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "scheduler" configuration section.

    The difference between this and get_airflow_scheduler_config is that the config set
    here can be overridden by the user.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION": "False",
    }


def _get_opinionated_airflow_secrets_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "secrets" configuration section.

    :returns A dictionary containing the environment variables.
    """
    connection_lookup_pattern = {"connections_lookup_pattern": "^(?!aws_default$).*$"}
    return {
        "AIRFLOW__SECRETS__BACKEND_KWARGS": json.dumps(connection_lookup_pattern),
    }


def _get_essential_airflow_webserver_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "webserver" configuration section.

    :returns A dictionary containing the environment variables.
    """

    flask_secret_key = {}
    flask_secret_secret = os.environ.get("MWAA__WEBSERVER__SECRET")
    if flask_secret_secret:
        try:
            flask_secret_key = {
                "AIRFLOW__WEBSERVER__SECRET_KEY": json.loads(flask_secret_secret)[
                    "secret_key"
                ]
            }
        except:
            logger.warning(
                "Invalid value for the webserver secret key. Value not printed "
                "for security reasons.",
            )

    return {
        "AIRFLOW__WEBSERVER__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
        **flask_secret_key,
    }
    
def _get_essential_airflow_api_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "api" configuration section.
    
    :returns A dictionary containing the environment variables.
    """
    api_config: Dict[str, str] = {}
    if os.environ.get("MWAA__CORE__AUTH_TYPE", "").lower() == "none":
        api_config["AIRFLOW__API__AUTH_BACKENDS"] =  "airflow.api.auth.backend.default"

    return api_config


def get_essential_airflow_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    These environment variables are essential and cannot be overridden by the customer.

    :returns A dictionary containing the environment variables.
    """
    return {
        **_get_essential_airflow_celery_config(),
        **_get_essential_airflow_core_config(),
        **_get_essential_airflow_db_config(),
        **_get_essential_airflow_logging_config(),
        **_get_essential_airflow_metrics_config(),
        **_get_essential_airflow_scheduler_config(),
        **_get_essential_airflow_webserver_config(),
        **_get_essential_airflow_api_config(),
    }


def get_opinionated_airflow_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    These environment variables are essential and cannot be overridden by the customer.

    :returns A dictionary containing the environment variables.
    """
    return {
        **_get_opinionated_airflow_scheduler_config(),
        **_get_opinionated_airflow_secrets_config(),
    }
