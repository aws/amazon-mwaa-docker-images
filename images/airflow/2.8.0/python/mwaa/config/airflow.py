"""Contain functions for building Airflow configuration."""
from typing import Dict

from mwaa.config.database import get_db_connection_string
from mwaa.config.sqs import get_sqs_endpoint, get_sqs_queue_name


def get_airflow_db_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "database" configuration section.
    
    :returns A dictionary containing the environment variables.
    """
    conn_string = get_db_connection_string()
    return {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": conn_string,
    }


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
        "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
        # Not a Celery config per-se, but is used by the Celery executor.
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


def get_airflow_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    :returns A dictionary containing the environment variables.
    """
    return {
        **get_airflow_core_config(),
        **get_airflow_db_config(),
        **get_airflow_celery_config(),
    }
