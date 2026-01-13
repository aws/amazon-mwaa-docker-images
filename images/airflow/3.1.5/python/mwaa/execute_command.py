"""
Command execution module for Amazon MWAA environments.

This module provides functionality to execute Airflow commands with proper
environment configuration and timing management. It handles various Airflow
commands including scheduler, webserver, worker, and hybrid modes within
the MWAA execution environment.
"""
# Python imports
from datetime import timedelta
from functools import cache
from typing import List, Dict, Optional, Callable
import logging
import os
import time

# Our imports
from mwaa.celery.task_monitor import WorkerTaskMonitor
from mwaa.logging.config import (
    DAG_PROCESSOR_LOGGER_NAME,
    LOGGING_CONFIG,
    SCHEDULER_LOGGER_NAME,
    TRIGGERER_LOGGER_NAME,
    WEBSERVER_LOGGER_NAME,
    WORKER_LOGGER_NAME,
)
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


logger = logging.getLogger("mwaa.entrypoint")


# Hybrid container runs both scheduler and worker as essential subprocesses.
# Therefore the default worker patience is increased to mitigate task
# failures due to scheduler failure.
HYBRID_WORKER_SIGTERM_PATIENCE_INTERVAL_DEFAULT = timedelta(seconds=130)

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

def _create_airflow_subprocess(
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
        _create_airflow_subprocess(
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


    mwaa_worker_idleness_verification_interval = int(environ.get("MWAA__CORE__WORKER_IDLENESS_VERIFICATION_INTERVAL",
                                                                    "20"))

    if task_monitoring_enabled:
        logger.info(f"Worker task monitoring is enabled with idleness verification interval: "
                    f"{mwaa_worker_idleness_verification_interval}")
        # Initializing the monitor responsible for performing idle worker checks if enabled.
        worker_task_monitor = WorkerTaskMonitor(mwaa_signal_handling_enabled, mwaa_worker_idleness_verification_interval)
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
        _create_airflow_subprocess(
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
        _create_airflow_subprocess(
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


def _run_airflow_command(cmd: str, environ: Dict[str, str]):
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


def execute_command(command: str, env: Dict[str, str], container_start_time: float):
    """Execute the specified Airflow command with the given environment configuration.

    This function sets up the container start time and executes the provided Airflow
    command with the specified environment variables.

    Args:
        command (str): The Airflow command to execute.
        env (Dict[str, str]): Dictionary of environment variables to be used.
        container_start_time (float): The timestamp when the container was started.

    Returns:
        None

    Global Variables:
        CONTAINER_START_TIME: Updates the global container start time.
    """
    global CONTAINER_START_TIME
    CONTAINER_START_TIME = container_start_time
    match command:
        case "shell":
            os.execlpe("/bin/bash", "/bin/bash", env)
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
            _run_airflow_command(command, env)
        case _:
            raise ValueError(f"Invalid command: {command}")