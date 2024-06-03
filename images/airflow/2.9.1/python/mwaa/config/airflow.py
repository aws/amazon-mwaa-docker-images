"""Contain functions for building Airflow configuration."""

from typing import Dict

from mwaa.config.database import get_db_connection_string
from mwaa.config.sqs import get_sqs_endpoint, get_sqs_queue_name


def get_airflow_celery_config() -> Dict[str, str]:
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


def get_airflow_core_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "core" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    }


def get_airflow_db_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "database" configuration section.

    :returns A dictionary containing the environment variables.
    """
    conn_string = get_db_connection_string()
    return {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_string,
    }


def get_airflow_logging_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "logging" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS": "mwaa.logging.config.LOGGING_CONFIG",
    }


def get_airflow_metrics_config() -> Dict[str, str]:
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


def get_airflow_scheduler_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "scheduler" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR": "True",
    }


def get_airflow_webserver_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "webserver" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__WEBSERVER__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
    }


def get_airflow_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    :returns A dictionary containing the environment variables.
    """
    return {
        **get_airflow_celery_config(),
        **get_airflow_core_config(),
        **get_airflow_db_config(),
        **get_airflow_logging_config(),
        **get_airflow_metrics_config(),
        **get_airflow_scheduler_config(),
        **get_airflow_webserver_config(),
    }
