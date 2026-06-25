"""
Environment configuration module for Amazon MWAA.

This module handles the setup and configuration of environment variables
and settings required for running Apache Airflow in Amazon MWAA (Managed
Workflows for Apache Airflow) environments.
"""
# Python imports
from datetime import timedelta
from typing import Dict
import json
import logging
import os
import shlex
import time

# Our imports
from mwaa.config.airflow import (
    get_essential_airflow_config,
    get_opinionated_airflow_config,
    get_user_airflow_config
)
from mwaa.logging.config import MWAA_LOGGERS
from mwaa.config.environ import get_essential_environ, get_opinionated_environ
from mwaa.subprocess.subprocess import Subprocess
from mwaa.subprocess.conditions import TimeoutCondition



logger = logging.getLogger("mwaa.entrypoint")

STARTUP_SCRIPT_SIGTERM_PATIENCE_INTERVAL = timedelta(seconds=5)
STARTUP_SCRIPT_MAX_EXECUTION_TIME = (
    timedelta(minutes=5) - STARTUP_SCRIPT_SIGTERM_PATIENCE_INTERVAL
)

def _execute_startup_script(cmd: str, environ: Dict[str, str]) -> Dict[str, str]:
    """
    Execute user startup script.

    :param cmd - The MWAA command the container is running, e.g. "worker", "scheduler". This is used for logging
    purposes so the logs of the execution of the startup script get sent to the correct place.
    :param environ: A dictionary containing the environment variables.
    """
    # Skip startup script execution for migrate-db command
    if cmd == "migrate-db":
        logger.info("Skipping startup script execution for migrate-db command.")
        return {}
    startup_script_path = os.environ.get("MWAA__CORE__STARTUP_SCRIPT_PATH", "")
    if not startup_script_path:
        logger.info("MWAA__CORE__STARTUP_SCRIPT_PATH is not provided.")
        return {}

    EXECUTE_USER_STARTUP_SCRIPT_PATH = "execute-user-startup-script"
    POST_STARTUP_SCRIPT_VERIFICATION_PATH = "post-startup-script-verification"
    # For hybrid worker/scheduler containers we publish the startup script logs
    # to the worker CloudWatch log group.
    PROCESS_LOGGER_PREFIX = "worker" if cmd == "hybrid" else cmd;
    PROCESS_LOGGER = logging.getLogger(MWAA_LOGGERS.get(f"{PROCESS_LOGGER_PREFIX}_startup"))

    if os.path.isfile(startup_script_path):
        logger.info("Executing customer startup script.")

        start_time = time.time()  # Capture start time
        startup_script_process = Subprocess(
            cmd=["/bin/bash", EXECUTE_USER_STARTUP_SCRIPT_PATH],
            env=environ,
            process_logger=PROCESS_LOGGER,
            conditions=[
                TimeoutCondition(STARTUP_SCRIPT_MAX_EXECUTION_TIME),
            ],
            friendly_name=f"{PROCESS_LOGGER_PREFIX}_startup",
            sigterm_patience_interval=STARTUP_SCRIPT_SIGTERM_PATIENCE_INTERVAL,
        )
        startup_script_process.start()
        end_time = time.time()
        duration = end_time - start_time
        PROCESS_LOGGER.info(f"Startup script execution time: {duration:.2f} seconds.")

        logger.info("Executing post startup script verification.")
        verification_process = Subprocess(
            cmd=["/bin/bash", POST_STARTUP_SCRIPT_VERIFICATION_PATH],
            env=environ,
            process_logger=PROCESS_LOGGER,
            conditions=[
                TimeoutCondition(STARTUP_SCRIPT_MAX_EXECUTION_TIME),
            ],
            friendly_name=f"{PROCESS_LOGGER_PREFIX}_startup",
        )
        verification_process.start()

        customer_env_vars_path = "/tmp/customer_env_vars.json"
        if os.path.isfile(customer_env_vars_path):
            try:
                with open(customer_env_vars_path, "r") as f:
                    customer_env_dict = json.load(f)
                logger.info("Successfully read the customer's environment variables.")
                return customer_env_dict
            except Exception as e:
                logger.error(f"Error reading the customer's environment variables: {e}")
                PROCESS_LOGGER.error(
                    "[ERROR] Failed to load environment variables from startup script. "
                    "Please verify your startup script configuration."
                )
                raise Exception(f"Failed to read customer's environment variables from startup script: {e}")

        else:
            logger.error(
                "An unexpected error occurred: the file containing the customer-defined "
                "environment variables could not be located. If the customer's startup "
                "script defines environment variables, this error message indicates that "
                "those variables won't be exported to the Airflow tasks."
            )
            PROCESS_LOGGER.error(
                "[ERROR] An unexpected error occurred: Failed to locate environment variables file from startup script.")
            raise Exception(
                "Failed to access customer environment variables file: Service was unable to create or locate /tmp/customer_env_vars.json")

    else:
        logger.info(f"No startup script found at {startup_script_path}.")
        return {}

def _export_env_variables(environ: dict[str, str]):
    """
    Export the environment variables to .bashrc and .bash_profile.

    For Airflow to function properly, a bunch of environment variables needs to be
    defined, which we do in the entrypoint. However, during development, a need might
    arise for bashing into the Docker container and doing some debugging, e.g. running
    a bunch of Airflow CLI commands. This won't be possible if the necessary environment
    variables are not defined, which is the case unless we have them defined in the
    .bashrc/.bash_profile files. This function does exactly that.

    :param environ: A dictionary containing the environment variables to export.
    """
    # Get the home directory of the current user
    home_dir = os.path.expanduser("~")
    bashrc_path = os.path.join(home_dir, ".bashrc")
    bash_profile_path = os.path.join(home_dir, ".bash_profile")

    # Environment variables to append
    env_vars_to_append = [
        f"export {key}={shlex.quote(value)}\n" for key, value in environ.items()
    ]

    # Append to .bashrc
    with open(bashrc_path, "a") as bashrc:
        bashrc.writelines(env_vars_to_append)

    # Append to .bash_profile
    with open(bash_profile_path, "a") as bash_profile:
        bash_profile.writelines(env_vars_to_append)

def _is_protected_os_environ(key: str) -> bool:
    # Protected environment variables
    protected_vars = [
        # Environment ID and name are set by MWAA for
        # informational purposes, and shouldn't be overridden by the customer.
        "AIRFLOW_ENV_ID",
        "AIRFLOW_ENV_NAME",
        # Airflow home directory cannot be overridden
        # as this will break MWAA setup.
        "AIRFLOW_HOME",
        # This is an internal MWAA identifier and
        # shouldn't be modified by users.
        "AIRFLOW_TASK_REVISION_ID",
        # Airflow version is managed by MWAA and
        # shouldn't be overridden manually.
        "AIRFLOW_VERSION",
        # The following two are needed by the IAM
        # plugin and shouldn't be overridden.
        "AIRFLOW__AWS_MWAA__REDIRECT_URL",
        "JWT_PUBLIC_KEY",
        # This is set to match the endpoint created
        # by MWAA and shouldn't be overridden.
        "AIRFLOW__WEBSERVER__BASE_URL",
        # Airflow 3 replaces WEBSERVER with APISERVER, so this is Airflow 3's
        # equivalent of AIRFLOW__WEBSERVER__BASE_URL
        "AIRFLOW__API__BASE_URL",
        # Default AWS region set by MWAA and used for AWS services.
        "AWS_DEFAULT_REGION",
        # AWS_REGION has a broader scope that is used by not just MWAA but
        # by the AWS SDK in general if the default region is not set.
        "AWS_REGION",
        # This identifies the customer account associated
        # with the MWAA environment.
        "CUSTOMER_ACCOUNT_ID",
        # These following are set by or needed for Fargate and
        # shouldn't be modified by the user.
        "ECS_AGENT_URI",
        "ECS_CONTAINER_METADATA_URI",
        "ECS_CONTAINER_METADATA_URI_V4",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_EXECUTION_ENV",
        # We don't allow the customer to override the PYTHONPATH, as this can break our
        # Python setup easily.
        "PYTHONPATH",
        # We disable Python buffering as we want to make
        # sure all print statements are sent to us immediately
        # so we can control when to send them to CloudWatch Logs.
        "PYTHONUNBUFFERED",
        # This is used to validate the version of Watchtower installed
        # which we don't allow the customer to override.
        "WATCHTOWER_VERSION",
        # This is used to control whether we use NON-CRITICAL LOGGING flow with Fluentbit
        # which we don't allow the customer to override.
        "USE_NON_CRITICAL_LOGGING",
        # This decides whether components should use Airflow Internal API for DB connectivity,
        # which is an experimental feature we do not support for now.
        "AIRFLOW__CORE__DATABASE_ACCESS_ISOLATION",
    ]

    # Check whether this is an MWAA configuration or a protected variable
    return key.startswith("MWAA__") or key in protected_vars

def setup_environment_variables(command: str, executor_type: str) -> Dict:
    """Set up and return environment variables for Airflow execution.

            Configures the necessary environment variables based on the provided command
            and executor type for running Airflow tasks.

            Args:
                command (str): The Airflow command to be executed.
                executor_type (str): The type of executor to be used (e.g., 'Local', 'Celery').

            Returns:
                Dict: A dictionary containing all configured environment variables
                      required for the Airflow execution.
    """
    # Add the necessary environment variables.
    mwaa_essential_airflow_config = get_essential_airflow_config(executor_type)
    mwaa_opinionated_airflow_config = get_opinionated_airflow_config()
    mwaa_essential_airflow_environ = get_essential_environ(command)
    mwaa_opinionated_airflow_environ = get_opinionated_environ()
    user_airflow_config = {} if command == "migrate-db" else get_user_airflow_config()


    startup_script_environ = _execute_startup_script(
        command,
        {
            **os.environ,
            **mwaa_opinionated_airflow_config,
            **mwaa_opinionated_airflow_environ,
            **user_airflow_config,
            **mwaa_essential_airflow_environ,
            **mwaa_essential_airflow_config,
        },
    )
    environ = {
        **os.environ,
        # Custom configuration and environment variables that we think are good, but
        # allow the user to override.
        **mwaa_opinionated_airflow_config,
        **mwaa_opinionated_airflow_environ,
        # What the user defined in the startup script.
        **startup_script_environ,
        # What the user passed via Airflow config secrets (specified by the
        # MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS environment variable.)
        **user_airflow_config,
        # The MWAA__x__y environment variables that are passed to the container are
        # considered protected environment variables that cannot be overridden at
        # runtime to avoid breaking the functionality of the container.
        **{
            key: value
            for (key, value) in os.environ.items()
            if _is_protected_os_environ(key)
        },
        # Essential variables that our setup will not function properly without, hence
        # it always has the highest priority.
        **mwaa_essential_airflow_config,
        **mwaa_essential_airflow_environ,
    }

    # IMPORTANT NOTE: The level for this should stay "DEBUG" to avoid logging customer
    # custom environment variables, which potentially contains sensitive credentials,
    # to stdout which, in this case of Fargate hosting (like in Amazon MWAA), ends up
    # being captured and sent to the service hosting.
    logger.debug(f"Environment variables: %s", environ)

    # Export the environment variables to .bashrc and .bash_profile to enable
    # users to run a shell on the container and have the necessary environment
    # variables set for using airflow CLI.
    _export_env_variables(environ)

    return environ
