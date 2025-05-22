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
import logging.config
from mwaa.logging.config import (
    DAG_PROCESSOR_LOGGER_NAME,
    LOGGING_CONFIG,
    SCHEDULER_LOGGER_NAME,
    TRIGGERER_LOGGER_NAME,
    WEBSERVER_LOGGER_NAME,
    WORKER_LOGGER_NAME,
)
logging.config.dictConfig(LOGGING_CONFIG)
# fmt: on

# Python imports
from datetime import timedelta
from functools import cache
from typing import Callable, Dict, List, Optional
import asyncio
import json
import logging
import os
import re
import shlex
import sys
import time

# 3rd party imports
import boto3
from botocore.exceptions import ClientError

# Our imports
from mwaa.celery.task_monitor import WorkerTaskMonitor
from mwaa.config.airflow import (
    get_essential_airflow_config,
    get_opinionated_airflow_config,
    get_user_airflow_config,
)
from mwaa.config.environ import get_essential_environ, get_opinionated_environ
from mwaa.config.sqs import (
    get_sqs_queue_name,
    should_create_queue,
)
from mwaa.logging.config import MWAA_LOGGERS
from mwaa.logging.loggers import CompositeLogger
from mwaa.subprocess.conditions import (
    SIDECAR_DEFAULT_HEALTH_PORT,
    AirflowDbReachableCondition,
    TaskMonitoringCondition,
    ProcessCondition,
    SidecarHealthCondition,
    TimeoutCondition,
)
from mwaa.subprocess.subprocess import Subprocess, run_subprocesses
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
    "hybrid",
    "shell",
    "resetdb",
    "spy",
]
MWAA_DOCS_REQUIREMENTS_GUIDE = "https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html#working-dags-dependencies-test-create"
STARTUP_SCRIPT_SIGTERM_PATIENCE_INTERVAL = timedelta(seconds=5)
STARTUP_SCRIPT_MAX_EXECUTION_TIME = (
    timedelta(minutes=5) - STARTUP_SCRIPT_SIGTERM_PATIENCE_INTERVAL
)
USER_REQUIREMENTS_MAX_INSTALL_TIME = timedelta(minutes=9)

# Save the start time of the container. This is used later to with the sidecar
# monitoring because we need to have a grace period before we start reporting timeouts
# related to sidecar endpoint not reporting health messages.
CONTAINER_START_TIME = time.time()

# Hybrid container runs both scheduler and worker as essential subprocesses.
# Therefore the default worker patience is increased to mitigate task
# failures due to scheduler failure.
HYBRID_WORKER_SIGTERM_PATIENCE_INTERVAL_DEFAULT = timedelta(seconds=130)


async def airflow_db_init(environ: dict[str, str]):
    """
    Initialize Airflow database.

    Before Airflow can be used, a call to `airflow db migrate` must be done. This
    function does this. This function is called in the entrypoint to make sure that,
    for any Airflow component, the database is initialized before it starts.

    :param environ: A dictionary containing the environment variables.
    """
    await run_command("python3 -m mwaa.database.migrate", env=environ)


@with_db_lock(4321)
async def airflow_db_reset(environ: dict[str, str]):
    """
    Reset Airflow metadata database.

    This function resets the Airflow metadata database. It is called when the `resetdb`
    command is specified.

    :param environ: A dictionary containing the environment variables.
    """
    logger.info("Resetting Airflow metadata database.")
    await run_command("airflow db reset --yes", env=environ)


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
            response = sqs.create_queue(QueueName=queue_name)  # type: ignore
            queue_url = response["QueueUrl"]  # type: ignore
            logger.info(f"Queue created: {queue_url}")
        else:
            # If there is a different error, raise it
            raise e


def _read_requirements_file(requirements_file: str) -> str:
    # Use pip's `auto_decode` method to make sure we read the contents of the
    # requirements.txt file exactly like they do.
    # NOTE It is not ideal to use an internal function from another library, but
    # the alternative would be to copy their code, but that has license implication
    # and can get outdated. Since we rely on pip anyway, the harm from accepting this
    # bad practice is minimized.
    from pip._internal.utils.encoding import auto_decode

    with open(requirements_file, "rb") as f:
        return auto_decode(f.read())


def _requirements_has_constraints(requirements_file: str):
    content = _read_requirements_file(requirements_file)
    for line in content.splitlines():
        # Notice that this regex check will also match lines with commented out
        # constraints flag. This is intentional as a mechanism for users who want to
        # avoid enforcing the default Airflow constraints, yet don't want to provide a
        # constraints file.
        if re.search(r"-c |--constraint ", line):
            return True
    return False


async def install_user_requirements(cmd: str, environ: dict[str, str]):
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

        subprocess_logger = CompositeLogger(
            "requirements_composite_logging",  # name can be anything unused.
            # We use a set to avoid double logging to console if the user doesn't
            # use CloudWatch for logging.
            *set(
                [
                    logging.getLogger(MWAA_LOGGERS.get(f"{cmd}_requirements")),
                    logger,
                ]
            ),
        )

        extra_args = []
        try:
            if not _requirements_has_constraints(requirements_file):
                subprocess_logger.warning(
                    "WARNING: Constraints should be specified for requirements.txt. "
                    f"Please see {MWAA_DOCS_REQUIREMENTS_GUIDE}"
                )
                subprocess_logger.warning("Forcing local constraints")
                extra_args = ["-c", os.environ["AIRFLOW_CONSTRAINTS_FILE"]]
        except:
            subprocess_logger.warning(
                "Cannot determine whether the requirements.txt file has constraints "
                "or not; forcing local constraints."
            )
            extra_args = ["-c", os.environ["AIRFLOW_CONSTRAINTS_FILE"]]

        pip_process = Subprocess(
            cmd=["safe-pip-install", "-r", requirements_file, *extra_args],
            env=environ,
            process_logger=subprocess_logger,
            conditions=[
                TimeoutCondition(USER_REQUIREMENTS_MAX_INSTALL_TIME),
            ],
            friendly_name=f"{cmd}_requirements",
        )
        pip_process.start()
        if pip_process.process and pip_process.process.returncode != 0:
            subprocess_logger.error(
                "ERROR: pip installation exited with a non-zero error code. This could "
                "be the result of package conflict. Notice that MWAA enforces a list "
                "of critical packages, e.g. Airflow, Celery, among others, whose "
                "version cannot be overridden by the customer as that can break our "
                "setup. Please double check your requirements.txt file."
            )
    else:
        logger.info("No user requirements to install.")


def execute_startup_script(cmd: str, environ: Dict[str, str]) -> Dict[str, str]:
    """
    Execute user startup script.

    :param cmd - The MWAA command the container is running, e.g. "worker", "scheduler". This is used for logging
    purposes so the logs of the execution of the startup script get sent to the correct place.
    :param environ: A dictionary containing the environment variables.
    """
    startup_script_path = os.environ.get("MWAA__CORE__STARTUP_SCRIPT_PATH", "")
    if not startup_script_path:
        logger.info("MWAA__CORE__STARTUP_SCRIPT_PATH is not provided.")
        return {}

    EXECUTE_USER_STARTUP_SCRIPT_PATH = "execute-user-startup-script"
    POST_STARTUP_SCRIPT_VERIFICATION_PATH = "post-startup-script-verification"
    PROCESS_LOGGER = logging.getLogger(MWAA_LOGGERS.get(f"{cmd}_startup"))

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
            friendly_name=f"{cmd}_startup",
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
            friendly_name=f"{cmd}_startup",
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
                return {}

        else:
            logger.error(
                "An unexpected error occurred: the file containing the customer-defined "
                "environment variables could not be located. If the customer's startup "
                "script defines environment variables, this error message indicates that "
                "those variables won't be exported to the Airflow tasks."
            )
            return {}

    else:
        logger.info(f"No startup script found at {startup_script_path}.")
        return {}


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
        f"export {key}={shlex.quote(value)}\n" for key, value in environ.items()
    ]

    # Append to .bashrc
    with open(bashrc_path, "a") as bashrc:
        bashrc.writelines(env_vars_to_append)

    # Append to .bash_profile
    with open(bash_profile_path, "a") as bash_profile:
        bash_profile.writelines(env_vars_to_append)


def create_airflow_subprocess(
    args: List[str],
    environ: Dict[str, str],
    logger_name: str,
    friendly_name: str,
    conditions: List[ProcessCondition] = [],
    on_sigterm: Optional[Callable[[], None]] = None,
    sigterm_patience_interval: timedelta | None = None
):
    """
    Create a subprocess for an Airflow command.

    Notice that while this function creates the sub-process, it will not start it.
    So, the caller is responsible for starting it.

    :param args - The arguments to pass to the Airflow CLI.
    :param environ - A dictionary containing the environment variables.
    :param logger_name - The name of the logger to use for capturing the generated logs.

    :returns The created subprocess.
    """
    logger: logging.Logger = logging.getLogger(logger_name)
    kwargs = {
            "cmd": ["airflow", *args],
            "env": environ,
            "process_logger": logger,
            "friendly_name": friendly_name,
            "conditions": conditions,
            "on_sigterm": on_sigterm,
            "is_essential": True
    }
    if sigterm_patience_interval is not None:
        kwargs['sigterm_patience_interval'] = sigterm_patience_interval
    return Subprocess(
        **kwargs
    )


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
    ]

    # Check whether this is an MWAA configuration or a protected variable
    return key.startswith("MWAA__") or key in protected_vars


@cache
def _is_sidecar_health_monitoring_enabled():
    enabled = (
        os.environ.get(
            "MWAA__HEALTH_MONITORING__ENABLE_SIDECAR_HEALTH_MONITORING", "false"
        ).lower()
        == "true"
    )
    if enabled:
        logger.info("Sidecar health monitoring is enabled.")
    else:
        logger.info("Sidecar health monitoring is NOT enabled.")
    return enabled


def _get_sidecar_health_port():
    try:
        return int(
            os.environ.get(
                "MWAA__HEALTH_MONITORING__SIDECAR_HEALTH_PORT",
                SIDECAR_DEFAULT_HEALTH_PORT,
            )
        )
    except:
        return SIDECAR_DEFAULT_HEALTH_PORT


def _create_airflow_webserver_subprocesses(environ: Dict[str, str]):
    return [
        create_airflow_subprocess(
            ["api-server"],
            environ=environ,
            logger_name=WEBSERVER_LOGGER_NAME,
            friendly_name="webserver",
            conditions=[
                AirflowDbReachableCondition(airflow_component="webserver"),
            ],
        ),
    ]


def _create_airflow_worker_subprocesses(environ: Dict[str, str], sigterm_patience_interval: timedelta | None = None):
    conditions = _create_airflow_process_conditions('worker')
    # MWAA__CORE__TASK_MONITORING_ENABLED is set to 'true' for workers where we want to monitor count of tasks currently getting
    # executed on the worker. This will be used to determine if idle worker checks are to be enabled.
    task_monitoring_enabled = (
        os.environ.get("MWAA__CORE__TASK_MONITORING_ENABLED", "false").lower() == "true"
    )
    # If MWAA__CORE__TERMINATE_IF_IDLE is set to 'true', then as part of the task monitoring if the task count reaches zero, then the
    # worker will be terminated.
    terminate_if_idle = (
        os.environ.get("MWAA__CORE__TERMINATE_IF_IDLE", "false").lower() == "true" and task_monitoring_enabled
    )
    # If MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED is set to 'true', then as part of the task monitoring, the monitor will expect certain
    # signals to be sent from MWAA. These signals will represent MWAA service side events such as start of an environment update.
    mwaa_signal_handling_enabled = (
        os.environ.get("MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED", "false").lower() == "true" and task_monitoring_enabled
    )
    if task_monitoring_enabled:
        logger.info("Worker task monitoring is enabled.")
        # Initializing the monitor responsible for performing idle worker checks if enabled.
        worker_task_monitor = WorkerTaskMonitor(mwaa_signal_handling_enabled)
    else:
        logger.info("Worker task monitoring is NOT enabled.")
        worker_task_monitor = None

    if worker_task_monitor:
        conditions.append(TaskMonitoringCondition(worker_task_monitor, terminate_if_idle))

    def on_sigterm() -> None:
        # When a SIGTERM is caught, we pause the Airflow Task consumption and wait 5 seconds in order
        # for any in-flight messages in the SQS broker layer to be processed and
        # corresponding Airflow task instance to be created. Once that is done, we can
        # start gracefully shutting down the worker. Without this, the SQS broker may
        # consume messages from the queue, terminate before creating the corresponding
        # Airflow task instance and abandon SQS messages in-flight.
        if worker_task_monitor:
            worker_task_monitor.pause_task_consumption()
            time.sleep(5)

    # Finally, return the worker subprocesses.
    return [
            create_airflow_subprocess(
                ["celery", "worker"],
                environ=environ,
                logger_name=WORKER_LOGGER_NAME,
                friendly_name="worker",
                conditions=conditions,
                on_sigterm=on_sigterm,
                sigterm_patience_interval=sigterm_patience_interval
            )
        ]


def _create_airflow_scheduler_subprocesses(environ: Dict[str, str], conditions: List):
    """
    Get the scheduler subproceses: scheduler, dag-processor, and triggerer.

    :param environ: A dictionary containing the environment variables.
    :param conditions: A list of subprocess conditions.
    :returns: Scheduler subprocesses.
    """
    return [
            create_airflow_subprocess(
                [airflow_cmd],
                environ=environ,
                logger_name=logger_name,
                friendly_name=friendly_name,
                conditions=conditions if airflow_cmd == "scheduler" else [],
            )
            for airflow_cmd, logger_name, friendly_name in [
                ("scheduler", SCHEDULER_LOGGER_NAME, "scheduler"),
                ("dag-processor", DAG_PROCESSOR_LOGGER_NAME, "dag-processor"),
                ("triggerer", TRIGGERER_LOGGER_NAME, "triggerer"),
            ]
        ]


def _create_airflow_process_conditions(airflow_cmd: str):
    """
    Get conditions for the given Airflow command.

    :param airflow_cmd: The command to get conditions for, e.g. "scheduler"
    :returns: A list of conditions for the given Airflow command.
    """
    conditions: List[ProcessCondition] = [
        AirflowDbReachableCondition(airflow_component=airflow_cmd),
    ]
    if _is_sidecar_health_monitoring_enabled():
        conditions.append(
            SidecarHealthCondition(
                airflow_component=airflow_cmd,
                container_start_time=CONTAINER_START_TIME,
                port=_get_sidecar_health_port(),
            ),
        )
    return conditions
def run_airflow_command(cmd: str, environ: Dict[str, str]):
    """
    Run the given Airflow command in a subprocess.

    :param cmd - The command to run, e.g. "worker".
    :param environ: A dictionary containing the environment variables.
    """
    match cmd:
        case "scheduler":
            conditions = _create_airflow_process_conditions('scheduler')
            subprocesses = _create_airflow_scheduler_subprocesses(environ, conditions)
            # Schedulers, triggers, and DAG processors are all essential processes and
            # if any fails, we want to exit the container and let it restart.
            run_subprocesses(subprocesses)
        case "worker":
            run_subprocesses(_create_airflow_worker_subprocesses(environ))
        case "webserver":
            run_subprocesses(_create_airflow_webserver_subprocesses(environ))
        # Hybrid runs the scheduler and celery worker processes is a single container.
        case "hybrid":
            # The Sidecar healthcheck is currently limited to one healthcheck per port
            # so the hybrid container can only include healthchecks for one subprocess.
            # Only the worker healcheck conditions are enabled to monitor container health, so
            # we pass an empty list of conditions to the scheduler process and make worker
            # process essential.
            scheduler_subprocesses = _create_airflow_scheduler_subprocesses(environ, [])

            # Since both scheduler and workers are launched together as essential
            # the default patience interval for worker needs to be increased to better
            # allow for in-flight tasks to complete in case of scheduler failure.
            try:
                worker_patience_interval_seconds = os.getenv('MWAA__HYBRID_CONTAINER__SIGTERM_PATIENCE_INTERVAL', None)
                if worker_patience_interval_seconds is not None:
                    worker_patience_interval = timedelta(seconds=int(worker_patience_interval_seconds))
                else:
                    worker_patience_interval = HYBRID_WORKER_SIGTERM_PATIENCE_INTERVAL_DEFAULT
            except (ValueError, TypeError):
                worker_patience_interval = HYBRID_WORKER_SIGTERM_PATIENCE_INTERVAL_DEFAULT

            worker_subprocesses = _create_airflow_worker_subprocesses(environ,
                                                                   sigterm_patience_interval=worker_patience_interval)
            run_subprocesses(scheduler_subprocesses + worker_subprocesses)

        case _:
            raise ValueError(f"Unexpected command: {cmd}")

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

    # Get executor type
    executor_type = os.environ.get("MWAA__CORE__EXECUTOR_TYPE", "CeleryExecutor")

    # Add the necessary environment variables.
    mwaa_essential_airflow_config = get_essential_airflow_config(executor_type)
    mwaa_opinionated_airflow_config = get_opinionated_airflow_config()
    mwaa_essential_airflow_environ = get_essential_environ(command)
    mwaa_opinionated_airflow_environ = get_opinionated_environ()
    user_airflow_config = get_user_airflow_config()

    startup_script_environ = execute_startup_script(
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

    await install_user_requirements(command, environ)
    await airflow_db_init(environ)
    if executor_type.lower() == "celeryexecutor":
        create_queue()

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
        # Disabling the "resetdb" command for now, as it is pretty risky to have such
        # a destructive command adjacent to other commands used in production; a simple
        # code mistake can result in wiping out production databases. Instead, testing
        # commands like this should be in a completely isolated boundary, with
        # protection mechanism to ensure they are not accidentally executed in
        # production.
        # case "resetdb":
        #     # Perform the resetdb functionality
        #     await airflow_db_reset(environ)
        #     # After resetting the db, initialize it again
        #     await airflow_db_init(environ)
        case "scheduler" | "webserver" | "worker" | "hybrid":
            run_airflow_command(command, environ)
        case _:
            raise ValueError(f"Invalid command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
else:
    logger.error("This module cannot be imported.")
    sys.exit(1)
