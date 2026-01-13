"""Contain functions for building Airflow configuration."""

# Python imports
from functools import cache
from typing import Dict
import json
import logging
import os

# 3rd-party imports
from airflow.configuration import conf

# Our imports
from mwaa.config.database import get_db_connection_string
from mwaa.config.sqs import get_sqs_endpoint, get_sqs_queue_name


logger = logging.getLogger(__name__)


def _get_essential_airflow_executor_config(executor_type: str) -> Dict[str, str]:
    """
    Retrieve the environment variables required for executor. Currently, two executors
    are supported:
        - LocalExecutor: All tasks are run on a local process
        - CeleryExecutor (Default): All tasks are run on Celery worker processes

    :param executor_type A string indicating the type of executor to use.

    :returns A dictionary containing the environment variables.
    """

    match executor_type.lower():
        case 'localexecutor':
            return {
                "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            }
        case 'celeryexecutor':
            celery_config_module_path = "mwaa.config.celery.MWAA_CELERY_CONFIG"
            return {
                "AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT": "43200",
                "AIRFLOW__CELERY__BROKER_URL": get_sqs_endpoint(),
                "AIRFLOW__CELERY__CELERY_CONFIG_OPTIONS": celery_config_module_path,
                "AIRFLOW__CELERY__RESULT_BACKEND": f"db+{get_db_connection_string()}",
                "AIRFLOW__CELERY__WORKER_ENABLE_REMOTE_CONTROL": "False",
                "AIRFLOW__CORE__EXECUTOR": "CeleryExecutor",
                "AIRFLOW__OPERATORS__DEFAULT_QUEUE": get_sqs_queue_name(),
            }
        case _:
            raise ValueError(f"Executor type {executor_type} is not supported.")


def _get_essential_airflow_core_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "core" configuration section.

    :returns A dictionary containing the environment variables.
    """

    fernet_key = {}
    api_server_url = {}
    fernet_secret_json = os.environ.get("MWAA__CORE__FERNET_KEY")
    api_server_config = os.environ.get("MWAA__CORE__API_SERVER_URL")
    if api_server_config:
        prefix = "" if "://" in api_server_config else "https://"
        api_server_url = {
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": prefix
            + api_server_config
            + "/execution",
        }
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
        **api_server_url,
        **fernet_key,
    }


def _get_opinionated_airflow_core_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "core" configuration section.

    :returns A dictionary containing the environment variables.
    """

    return {
        "AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER": "True",
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

def _get_opinionated_airflow_db_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "database" configuration section.

    The difference between this and _get_essential_airflow_db_config is that the config set
    here can be overridden by the user.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS": "mwaa.config.database.MWAA_CONNECT_ARGS",
    }

def _get_essential_airflow_auth_config() -> Dict[str, str]:
    if os.environ.get("MWAA__CORE__AUTH_TYPE", "").lower() == "mwaa-iam":
        return {
            "AIRFLOW__CORE__AUTH_MANAGER": "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
        }
    # Use default SimpleAuthManager for development. By setting all admins to true, any user/pwd can be used to login.
    # SIMPLE_AUTH_MANAGER_USERS is in username:role format. Set SIMPLE_AUTH_MANAGER_ALL_ADMINS=false to have dedicated
    # roles. In that case, the password for each username will be printed in webserver logs.
    return {
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin,viewer:viewer",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
    }

def _get_essential_airflow_api_auth_config() -> Dict[str, str]:

    """
    Retrieve the environment variables for Airflow's "api_auth" configuration section.

    :returns A dictionary containing the environment variables.
    """
    api_config: Dict[str, str] = {}
    api_config["AIRFLOW__API_AUTH__JWT_SECRET"] = os.environ.get("MWAA__CORE__FERNET_KEY")
    api_config["AIRFLOW__API_AUTH__JWT_ALGORITHM"] = "HS256"

    return api_config

def _get_essential_aiflow_execution_api_config() -> Dict[str, str]:

    """
    Retrieve the environment variables for Airflow's "execution_api" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME": "86400"
    }

def _get_essential_airflow_logging_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "logging" configuration section.

    :returns A dictionary containing the environment variables.
    """
    return {
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS": "mwaa.logging.config.LOGGING_CONFIG",
        "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG": "False",
    }


@cache
def _get_mwaa_cloudwatch_integration_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to enable CloudWatch Metrics integration.

    :returns A dictionary containing the environment variables.
    """
    enabled = (
        os.environ.get("MWAA__CLOUDWATCH_METRICS_INTEGRATION__ENABLED", "false").lower()
        == "true"
    )
    if not enabled:
        # MWAA CloudWatch Metrics integration isn't enabled.
        logging.info("MWAA CloudWatch Metrics integration is NOT enabled.")
        return {}

    logging.info("MWAA CloudWatch Metrics integration is enabled.")

    metrics_section = conf.getsection("metrics")
    if metrics_section is None:
        raise RuntimeError(
            "Unexpected error: couldn't find 'metrics' section in Airflow configuration."
        )
    metrics_defaults = {
        f"AIRFLOW__METRICS__{option.upper()}": conf.get_default_value("metrics", option)  # type: ignore
        for option in metrics_section.keys()
    }

    # In MWAA, we use the metrics for monitoring purposes, hence we don't allow the user
    # to override the Airflow configurations for metrics. However, we still give the
    # customer the ability to control metrics via the options below, which we process in
    # the sidecar. Hence, we save the customer-provided values for these metrics in a
    # volume that the sidecar has access to, but then force enable them in Airflow so
    # the latter always publish metrics.
    customer_config_path = os.environ.get(
        "MWAA__CLOUDWATCH_METRICS_INTEGRATION__CUSTOMER_CONFIG_PATH"
    )
    if customer_config_path:
        user_config = get_user_airflow_config()
        for option, default_value in [
            ("statsd_on", "True"),
            ("metrics_block_list", ""),
            ("metrics_allow_list", ""),
        ]:
            c = user_config.get(f"AIRFLOW__METRICS__{option.upper()}", default_value)
            config_path = os.path.join(customer_config_path, f"{option}.txt")
            try:
                with open(config_path, "w") as f:
                    print(c, file=f)  # type: ignore
            except:
                logger.error(
                    f"Failed to write {option} to {config_path}. This might "
                    f"result in metrics misconfiguration."
                )

    return {
        # We don't allow the user to change the metrics configuration as that can break
        # the integration with CloudWatch Metrics.
        **metrics_defaults,
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
        "AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT": "1800.0",
    }


def _get_opinionated_airflow_secrets_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's "secrets" configuration section.

    :returns A dictionary containing the environment variables.
    """
    connection_lookup_pattern = {"connections_lookup_pattern": "^(?!aws_default$).*$"}
    return {
        "AIRFLOW__SECRETS__BACKEND_KWARGS": json.dumps(connection_lookup_pattern),
        "AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS": json.dumps(connection_lookup_pattern),
    }

def _get_opinionated_airflow_usage_data_config() -> Dict[str, str]:
    """
    Retrieve the environment variables for Airflow's usage data configuration section.

    This config can be overridden by the user.

    :returns A dictionary containing the environment variables.
    """

    return {
        "AIRFLOW__USAGE_DATA_COLLECTION__ENABLED": "False",
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
                "AIRFLOW__API__SECRET_KEY": json.loads(flask_secret_secret)[
                    "secret_key"
                ]
            }
        except:
            logger.warning(
                "Invalid value for the webserver secret key. Value not printed "
                "for security reasons.",
            )

    return {
        "AIRFLOW__FAB__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
        **flask_secret_key,
    }

def get_essential_airflow_config(executor_type: str) -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    These environment variables are essential and cannot be overridden by the customer.

    :param executor_type A string indicating the type of executor to use.

    :returns A dictionary containing the environment variables.
    """
    return {
        **_get_essential_airflow_executor_config(executor_type),
        **_get_essential_airflow_core_config(),
        **_get_essential_airflow_db_config(),
        **_get_essential_airflow_logging_config(),
        **_get_mwaa_cloudwatch_integration_config(),
        **_get_essential_airflow_scheduler_config(),
        **_get_essential_airflow_webserver_config(),
        **_get_essential_airflow_auth_config(),
        **_get_essential_airflow_api_auth_config(),
        **_get_essential_aiflow_execution_api_config(),
    }


def get_opinionated_airflow_config() -> Dict[str, str]:
    """
    Retrieve the environment variables required to set Airflow configurations.

    These environment variables can be overridden by the customer.

    :returns A dictionary containing the environment variables.
    """
    return {
        **_get_opinionated_airflow_core_config(),
        **_get_opinionated_airflow_scheduler_config(),
        **_get_opinionated_airflow_secrets_config(),
        **_get_opinionated_airflow_usage_data_config(),
        **_get_opinionated_airflow_db_config(),
    }
