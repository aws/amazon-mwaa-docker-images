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


def abort(err_msg: str, exit_code: int = 1) -> None:
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


def verify_versions() -> None:
    major, minor, micro, *_ = sys.version_info
    assert os.environ["PYTHON_VERSION"] == f"{major}.{minor}.{micro}"


F = TypeVar("F", bound=Callable[..., Any])


def db_lock(lock_id: int, timeout: int = 300 * 1000) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name: str = func.__name__
            db_engine: Engine = create_engine(
                get_db_connection_string()  # Assuming this is defined elsewhere
            )
            print(f"Obtaining lock for {func_name}...")
            with db_engine.connect() as conn:
                try:
                    conn.execute(
                        text("SET LOCK_TIMEOUT to :timeout"), {"timeout": timeout}
                    )
                    conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": lock_id})
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
                    conn.execute(text("SET LOCK_TIMEOUT TO DEFAULT"))
                    conn.execute(
                        text("SELECT pg_advisory_unlock(:id)"), {"id": lock_id}
                    )
                    print(f"Released lock for {func_name}")

        return cast(F, wrapper)

    return decorator


@db_lock(1234)
def airflow_db_init(environ: dict[str, str]) -> None:
    print("Calling 'airflow db migrate' to initialize the database.")
    response = subprocess.run(
        ["airflow db migrate"], shell=True, check=True, text=True, env=environ
    )

    if response.returncode:
        raise RuntimeError(f"Failed to migrate db. Error: {response.stderr}")


@db_lock(5678)
def create_www_user(environ: dict[str, str]) -> None:
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


def install_user_requirements(environ: dict[str, str]) -> None:
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


def export_env_variables(environ: dict[str, str]) -> None:
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
    """Entrypoint of the script."""

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
    create_www_user(environ)
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
