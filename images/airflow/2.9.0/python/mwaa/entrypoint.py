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
from mwaa.logging_setup import setup_logging

setup_logging()

# Python imports
import asyncio
import logging
import os
import sys
import time

# 3rd party imports
import boto3
from botocore.exceptions import ClientError

from mwaa.config.airflow import get_airflow_config
from mwaa.config.sqs import (
    get_sqs_queue_name,
    should_create_queue,
)
from mwaa.utils.cmd import run_command
from mwaa.utils.dblock import with_db_lock


# Usually, we pass the `__name__` variable instead as that defaults to the
# module path, i.e. `mwaa.entrypoint` in this case. However, since this is
# the entrypoint script, `__name__` will have the value of `__main__`, hence
# we hard-code the module path.
logger = logging.getLogger("mwaa.entrypoint")

# TODO Fix the "type: ignore"s in this file.


AVAILABLE_COMMANDS = [
    "webserver",
    "scheduler",
    "worker",
    "triggerer",
    "shell",
    "spy",
]


@with_db_lock(1234)
async def airflow_db_init(environ: dict[str, str]):
    """
    Initialize Airflow database.

    Before Airflow can be used, a call to `airflow db migrate` must be done. This
    function does this. This function is called in the entrypoint to make sure that,
    for any Airflow component, the database is initialized before it starts.

    This function uses a DB lock to make sure that no two processes execute this
    function at the same time.

    :param environ: A dictionary containing the environment variables.
    """
    logger.info("Calling 'airflow db migrate' to initialize the database.")
    await run_command("airflow db migrate", env=environ)


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
            e.response.get("Error", {}).get("Code")
            == "AWS.SimpleQueueService.NonExistentQueue"
        ):
            response = sqs.create_queue(QueueName=queue_name)  # type: ignore
            queue_url = response["QueueUrl"]  # type: ignore
            logger.info(f"Queue created: {queue_url}")
        else:
            # If there is a different error, raise it
            raise e


async def install_user_requirements(environ: dict[str, str]):
    """
    Install user requirements.

    User requirements should be placed in a requirements.txt file and the environment
    variable `MWAA__CORE__REQUIREMENTS_PATH` should be set to the location of that file.
    In a Docker Compose setup, you would usually want to create a volume that maps a
    requirements.txt file in the host machine somewhere in the container, and then set
    the `MWAA__CORE__REQUIREMENTS_PATH` accordingly.

    :param environ: A dictionary containing the environment variables.
    """
    requirements_file = environ.get("MWAA__CORE__REQUIREMENTS_PATH")
    logger.info(f"MWAA__CORE__REQUIREMENTS_PATH = {requirements_file}")
    if requirements_file and os.path.isfile(requirements_file):
        logger.info(f"Installing user requirements from {requirements_file}...")
        await run_command(
            f"safe-pip-install -r {str(requirements_file)}",
            stdout_logging_method=logger.info,
            stderr_logging_method=logger.error,
        )
    else:
        logger.info("No user requirements to install.")


def export_env_variables(environ: dict[str, str]):
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
        # TODO Need to escape value.
        f'export {key}="{value}"\n'
        for key, value in environ.items()
    ]

    # Append to .bashrc
    with open(bashrc_path, "a") as bashrc:
        bashrc.writelines(env_vars_to_append)

    # Append to .bash_profile
    with open(bash_profile_path, "a") as bash_profile:
        bash_profile.writelines(env_vars_to_append)


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

    logger.info(f"Warming a Docker container for an Airflow {command}.")

    # Add the necessary environment variables.
    environ = {**os.environ, **get_airflow_config()}

    # IMPORTANT NOTE: The level for this should stay "DEBUG" to avoid logging customer
    # custom environment variables, which potentially contains sensitive credentials,
    # to stdout which, in this case of Fargate hosting (like in Amazon MWAA), ends up
    # being captured and sent to the service hosting.
    logger.debug(f"Environment variables: {environ}")

    await airflow_db_init(environ)
    await create_airflow_user(environ)
    create_queue()
    await install_user_requirements(environ)

    # Export the environment variables to .bashrc and .bash_profile to enable
    # users to run a shell on the container and have the necessary environment
    # variables set for using airflow CLI.
    export_env_variables(environ)

    match command:
        case "shell":
            os.execlpe("/bin/bash", "/bin/bash", environ)
        case "spy":
            while True:
                time.sleep(1)
        case "worker":
            os.execlpe("airflow", "airflow", "celery", "worker", environ)
        case _:
            os.execlpe("airflow", "airflow", command, environ)


if __name__ == "__main__":
    asyncio.run(main())
else:
    logger.error("This module cannot be imported.")
    sys.exit(1)
