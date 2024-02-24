"""
This is the entrypoint of the Docker image when running Airflow components.

The script gets called with the Airflow component name, e.g. scheduler, as the
first and only argument. It accordingly runs the requested Airlfow component
after setting up the necessary configurations.
"""

# Python imports
import os
import sys
import time
import subprocess
from typing import Any, Callable, TypeVar, cast


# 3rd party imports
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Our imports
from mwaa.config.airflow import get_airflow_config
from mwaa.config.database import get_db_connection_string
from mwaa.config.sqs import (
    get_sqs_queue_name,
    should_create_queue,
)


def abort(err_msg: str, exit_code: int = 1):
    """
    Print an error message and then exit the process with the given exit code.

    :param err_msg: The error message to print before exiting.
    :param exit_code: The exit code.
    """
    print(err_msg)
    sys.exit(exit_code)


AVAILABLE_COMMANDS = [
    "webserver",
    "scheduler",
    "worker",
    "triggerer",
    "shell",
    "spy",
]


F = TypeVar("F", bound=Callable[..., Any])


def db_lock(lock_id: int, timeout: int = 300 * 1000) -> Callable[[F], F]:
    """
    Generate a decorator that can be used to protect a function by a database lock.

    This is useful when a function needs to be protected against multiple simultaneous
    executions. For example, during Airflow database initialization, we want to make
    sure that only one process is doing it. Since normal lock mechanisms only apply to
    the same process, a database lock becomes a viable solution.

    :param lock_id: A unique ID for the lock. When multiple processes try to use the
      same lock ID, only one process will be granted the lock at one time. However,
      if the processes have different lock IDs, they will be granted the locks at the
      same time.
    :param timeout: The maximum time the process is allowed to hold the lock. After this
      time expires, the lock is automatically released.
    
    :returns A decorator that can be applied to a function to protect it with a DB lock.
    """
    def decorator(func: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name: str = func.__name__
            db_engine: Engine = create_engine(
                get_db_connection_string()  # Assuming this is defined elsewhere
            )
            print(f"Obtaining lock for {func_name}...")
            with db_engine.connect() as conn: # type: ignore
                try:
                    conn.execute( # type: ignore
                        text("SET LOCK_TIMEOUT to :timeout"), {"timeout": timeout}
                    )
                    conn.execute( # type: ignore
                        text("SELECT pg_advisory_lock(:id)"), {"id": lock_id}
                        )
                    print(f"Obtained lock for {func_name}.")

                    try:
                        func(*args, **kwargs)
                    except Exception as e:
                        abort(
                            f"Failed while executing {func_name}. " + f"Error: {e}."
                        )  # Assuming abort is defined elsewhere
                except Exception as e:
                    abort(
                        f"Failed to obtain DB lock for {func_name}. " + f"Error: {e}."
                    )
                finally:
                    print(f"Releasing lock for {func_name}...")
                    conn.execute( # type: ignore
                        text("SET LOCK_TIMEOUT TO DEFAULT")
                        )
                    conn.execute( # type: ignore
                        text("SELECT pg_advisory_unlock(:id)"), {"id": lock_id}
                    )
                    print(f"Released lock for {func_name}")

        return cast(F, wrapper)

    return decorator


@db_lock(1234)
def airflow_db_init(environ: dict[str, str]):
    """
    Initialize Airflow database.

    Before Airflow can be used, a call to `airflow db migrate` must be done. This
    function does this. This function is called in the entrypoint to make sure that,
    for any Airflow component, the database is initialized before it starts.

    This function uses a DB lock to make sure that no two processes execute this
    function at the same time.

    :param environ: A dictionary containing the environment variables.
    """
    print("Calling 'airflow db migrate' to initialize the database.")
    response = subprocess.run(
        ["airflow db migrate"], shell=True, check=True, text=True, env=environ
    )

    if response.returncode:
        raise RuntimeError(f"Failed to migrate db. Error: {response.stderr}")


@db_lock(5678)
def create_airflow_user(environ: dict[str, str]):
    """
    Create the 'airflow' user.

    To be able to login to the webserver, you need a user. This function creates a user
    with default credentials.

    Notice that this should only be used in development context. In production, other
    means need to be employed to create users with strong passwords. Alternatively, with
    MWAA setup, a plugin is employed to integrate with IAM (not implemented yet.)

    :param environ: A dictionary containing the environment variables.
    """
    print("Calling 'airflow users create' to create the webserver user.")
    response = subprocess.run(
        " ".join(
            [
                "airflow",
                "users",
                "create",
                "--username",
                "airflow",
                "--firstname",
                "Airflow",
                "--lastname",
                "Admin",
                "--email",
                "airflow@example.com",
                "--role",
                "Admin",
                "--password",
                "airflow",
            ]
        ),
        shell=True,
        check=True,
        text=True,
        env=environ,
    )

    if response.returncode:
        raise RuntimeError(f"Failed to create user. Error: {response.stderr}")


@db_lock(1357)
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
        sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        print(f"Queue {queue_name} already exists.")
    except ClientError as e:
        # If the queue does not exist, create it
        if (
            e.response.get("Error", {}).get("Code")
            == "AWS.SimpleQueueService.NonExistentQueue"
        ):
            response = sqs.create_queue(QueueName=queue_name)
            queue_url = response["QueueUrl"]
            print(f"Queue created: {queue_url}")
        else:
            # If there is a different error, raise it
            raise e


def install_user_requirements(environ: dict[str, str]):
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
    print(f"MWAA__CORE__REQUIREMENTS_PATH = {requirements_file}")
    if requirements_file and os.path.isfile(requirements_file):
        print(f"Installing user requirements from {requirements_file}...")
        subprocess.run(
            [
                "safe-pip-install",
                "-r",
                str(requirements_file),
            ],
            check=True,
        )
    else:
        print("No user requirements to install.")


def export_env_variables(environ: dict[str, str]):
    """
    Export the environment variables to .bashrc and .bash_profile.

    For Aiflow to function properly, a bunch of enviornment variables needs to be
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


def main() -> None:
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
        print(sys.argv)
        exit(
            "Invalid arguments. Please provide one argument with one of"
            f'the values: {", ".join(AVAILABLE_COMMANDS)}. Error was {e}.'
        )

    print(f"Warming a Docker container for an Airflow {command}.")

    # Add the necessary environment variables.
    environ = {**os.environ, **get_airflow_config()}

    airflow_db_init(environ)
    create_airflow_user(environ)
    create_queue()
    install_user_requirements(environ)

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
    main()
else:
    print("This module cannot be imported.")
    sys.exit(1)
