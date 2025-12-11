"""
This is the entrypoint of the Docker image when running Airflow components.

The script gets called with the Airflow component name, e.g. scheduler, as the
first and only argument. It accordingly runs the requested Airflow component
after setting up the necessary configurations.
"""

# Setup logging first thing to make sure all logs happen under the right setup. The
# reason for needing this is that typically a `logger` object is defined at the top
# of the module and is used through out it. So, if we import a module before logging
# is setup, its `logger` object will not have the right setup.
# ruff: noqa: E402
# fmt: off
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import logging.config
logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
# fmt: on

# Python imports
import asyncio
import logging
import os
import sys
import time

# 3rd party imports
import boto3
from botocore.exceptions import ClientError

# Our imports
from mwaa.execute_command import execute_command
from mwaa.config.setup_environment import setup_environment_variables
from mwaa.config.sqs import (
    get_sqs_queue_name,
    should_create_queue,
)
from mwaa.utils.cmd import run_command
from mwaa.utils.dblock import with_db_lock
from mwaa.utils.statsd import get_statsd
from mwaa.utils.user_requirements import install_user_requirements

# Usually, we pass the `__name__` variable instead as that defaults to the
# module path, i.e. `mwaa.entrypoint` in this case. However, since this is
# the entrypoint script, `__name__` will have the value of `__main__`, hence
# we hard-code the module path.
logger = logging.getLogger("mwaa.entrypoint")


def _setup_console_log_level(command: str):
    # Set up console log level environment variable based on command
    component_mapping = {
        'scheduler': 'MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL',
        'worker': 'MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL',
        'webserver': 'MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL'
    }

    if command in component_mapping:
        source_var = component_mapping[command]
        os.environ['AIRFLOW_CONSOLE_LOG_LEVEL'] = os.environ[source_var]
    else:
        os.environ['AIRFLOW_CONSOLE_LOG_LEVEL'] = 'INFO'


def _configure_root_logger(command: str):
    _setup_console_log_level(command)
    # Doing a local import because we can't import
    # LOGGING_CONFIG before setting AIRFLOW_CONSOLE_LOG_LEVEL
    # as it will lead to root logger's log level set to default value
    from mwaa.logging.config import LOGGING_CONFIG
    logging.config.dictConfig(LOGGING_CONFIG)

def _mark_as_unhealthy():
    # Create a health check marker file

    # max of (service grace period or container health check startPeriod) + health check (retries * interval)
    # 300 + 10 * 75 = ~1075
    max_timeout = 1100

    try:
        os.makedirs('/tmp/mwaa', exist_ok=True)
        open('/tmp/mwaa/container_unhealthy', 'w').close()

        logger.error("Marked container as unhealthy via /tmp/mwaa/container_unhealthy")
    except Exception as file_err:
        logger.error("Failed to write /tmp/mwaa/container_unhealthy: %s", str(file_err))

    container_runtime = time.time() - CONTAINER_START_TIME
    if container_runtime < max_timeout:
        logger.info("Container running for %s seconds, still in grace period. Sleeping for %s seconds.",
                    int(container_runtime), max_timeout)
        time.sleep(max_timeout)
    else:
        logger.info("Container running for %s seconds, not in grace period. Skipping sleep.",
                    int(container_runtime))

# TODO Fix the "type: ignore"s in this file.

AVAILABLE_COMMANDS = [
    "webserver",
    "scheduler",
    "worker",
    "migrate-db",
    "hybrid",
    "shell",
    "resetdb",
    "spy",
    "test-requirements",
    "test-startup-script",
]

# Save the start time of the container. This is used later to with the sidecar
# monitoring because we need to have a grace period before we start reporting timeouts
# related to sidecar endpoint not reporting health messages.
CONTAINER_START_TIME = time.time()


async def airflow_db_migrate(environ: dict[str, str]):
    """
    Migrate/Initialize Airflow database.

    Used in the migrate-db container and will handle both upgrades and downgrades

    Before Airflow can be used, a call to `airflow db migrate` must be done. This
    function does this. This function is called in the entrypoint to make sure that,
    for any Airflow component, the database is initialized before it starts.

    :param environ: A dictionary containing the environment variables.
    """
    await run_command("python3 -m mwaa.database.migrate_with_downgrade", env=environ)


async def airflow_dag_reserialize():
    """
    Reserialize the dags.

    Have the process run the dag_reserialize command to make sure the dags are compatible
    with the current airflow database version.
    """
    await run_command("python3 -m mwaa.database.reserialize")


async def increase_pool_size_if_insufficient(environ: dict[str, str]):
    """
    Increase the default pool size if it is too small.

    Increases the default_pool size to 10000 if it is less than or equal 5000
    as it improves performance in environments.

    :param environ: A dictionary containing the environment variables.
    """
    problematic_pool_size = 5000

    try:
        command_output = []

        # Get the current default_pool size
        await run_command("airflow pools get default_pool | grep default_pool | awk '{print $3}'",
                        env=environ, stdout_logging_method=lambda output : command_output.append(output))

        # Increasing the pool size if it is the default size
        if len(command_output) == 1 and int(command_output[0]) <= problematic_pool_size:
            logger.info("Setting default_pool size to 10000.")
            await run_command("airflow pools set default_pool 10000 default", env=environ)
            stats = get_statsd()
            stats.incr("mwaa.pool.increased_default_pool_size", 1)
    except Exception as error:
        logger.error(f"Error checking if pool issue is present: {error}")

@with_db_lock(5678)
async def create_airflow_user(environ: dict[str, str]):
    """
    Create the 'airflow' user.

    To be able to login to the webserver, you need a user. This function creates a user
    with default credentials.

    Notice that this should only be used in development context. In production, other
    means need to be employed to create users with strong passwords. Alternatively, with
    MWAA setup, a plugin is employed to integrate with IAM (not implemented yet.)

    :param environ: A dictionary containing the environment variables.
    """
    logger.info("Calling 'airflow users create' to create the webserver user.")
    await run_command(
        "airflow users create "
        "--username airflow "
        "--firstname Airflow "
        "--lastname Admin "
        "--email airflow@example.com "
        "--role Admin "
        "--password airflow",
        env=environ,
    )


@with_db_lock(1357)
def create_queue() -> None:
    """
    Create the SQS required by Celery.

    In our setup, we use SQS as the backend for Celery. Usually, this should be created
    before hand. However, sometimes you might want to create the SQS queue during
    startup. One such example is when using the elasticmq server as a mock SQS server.
    """
    if not should_create_queue():
        return
    queue_name = get_sqs_queue_name()
    endpoint = os.environ.get("MWAA__SQS__CUSTOM_ENDPOINT")
    sqs = boto3.client("sqs", endpoint_url=endpoint)  # type: ignore
    try:
        # Try to get the queue URL to check if it exists
        sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]  # type: ignore
        logger.info(f"Queue {queue_name} already exists.")
    except ClientError as e:
        # If the queue does not exist, create it
        if (
            e.response.get("Error", {}).get("Message")  # type: ignore
            == "The specified queue does not exist."
        ):
            try:
                response = sqs.create_queue(QueueName=queue_name)  # type: ignore
                queue_url = response["QueueUrl"]  # type: ignore
                logger.info(f"Queue created: {queue_url}")
            except ClientError as create_error:
                # Handle race condition where another container created the queue
                # between our check and creation attempt (409 Conflict)
                if create_error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 409:  # type: ignore
                    logger.info(f"Queue {queue_name} was created by another process.")
                else:
                    raise create_error
        else:
            # If there is a different error, raise it
            raise e


async def main() -> None:
    """Start execution of the script."""
    try:
        (
            _,
            command,
        ) = sys.argv
        if command not in AVAILABLE_COMMANDS:
            exit(
                f"Invalid command: {command}. "
                f'Use one of {", ".join(AVAILABLE_COMMANDS)}.'
            )
    except Exception as e:
        exit(
            f"Invalid arguments: {sys.argv}. Please provide one argument with one of"
            f'the values: {", ".join(AVAILABLE_COMMANDS)}. Error was {e}.'
        )
    _configure_root_logger(command)
    logger.info(f"Warming a Docker container for an Airflow {command}.")

    # Get executor type
    executor_type = os.environ.get("MWAA__CORE__EXECUTOR_TYPE", "CeleryExecutor")
    environ = setup_environment_variables(command, executor_type)

    if command == "migrate-db":
        await airflow_db_migrate(environ)
        print("Finished running db validations")
        return

    await install_user_requirements(command, environ)

    if command == "test-requirements":
        print("Finished testing requirements")
        return

    await increase_pool_size_if_insufficient(environ)
    if executor_type.lower() == "celeryexecutor":
        create_queue()

    execute_command(command, environ, CONTAINER_START_TIME)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        logger.error("Fatal error in entrypoint: %s", str(ex), exc_info=True)
        _mark_as_unhealthy()
        sys.exit(1)
elif os.environ.get("MWAA__CORE__TESTING_MODE", "false") != "true":
    logger.error("This module cannot be imported.")
    sys.exit(1)
