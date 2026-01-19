"""
Contain various conditions that can be added to a process.

We frequently need to monitor and control running processes. For example, we might want
to limit the running time of a process to X minutes. Or, we might need to monitor the
health of a process and restart it if necessary. Process conditions make this possible.
"""

from __future__ import annotations

# Python imports
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from dateutil.tz import tz
from functools import cached_property
from types import FrameType, TracebackType
from typing import Callable, Deque, Optional
import logging
import signal
import socket
import sys
import time

# 3rd-party imports
from airflow.configuration import conf
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# Our imports
from mwaa.celery.task_monitor import WorkerTaskMonitor
from mwaa.config.database import get_db_connection_string, MWAA_CONNECT_ARGS
from mwaa.logging.utils import throttle
from mwaa.subprocess import ProcessStatus
from mwaa.utils.plogs import generate_plog

from mwaa.utils.statsd import get_statsd

logger = logging.getLogger(__name__)


@dataclass
class ProcessConditionResponse:
    """
    Encapsulates all the information regarding a single execution of a health check.
    """

    condition: ProcessCondition
    successful: bool
    message: str = ""
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz.tzutc()))

    @property
    def name(self):
        """
        Return the name of the condition this response is about.

        :returns The name of the condition.
        """
        return self.condition.name

    def __str__(self) -> str:
        """
        Return the string representation of this response.

        :returns The string representation of this response.
        """
        if self.successful:
            return (
                f"At {self.timestamp} condition {self.name} succeeded with "
                f"message: {self.message}"
            )
        else:
            return (
                f"At {self.timestamp} condition {self.name} failed with "
                f"message: {self.message}"
            )


_PROCESS_CONDITION_DEFAULT_MAX_HISTORY = 10


class ProcessCondition:
    """
    Base class for all process conditions.

    A process condition is, as the name suggests, a condition that must be satisfied
    for a process to continue running. The
    """

    def __init__(
        self,
        name: str | None = None,
        max_history: int = _PROCESS_CONDITION_DEFAULT_MAX_HISTORY,
    ):
        """
        Initialize the process condition.

        :param name: The name of the condition. You can pass None for this to use the
          name of the class of the condition, which is usually sufficient, unless you
          want to customize the name.
        """
        self.name = name if name else self.__class__.__name__
        self.history: Deque[ProcessConditionResponse] = deque(maxlen=max_history)
        self.closed = False

    def prepare(self):
        """
        Called by the Subprocess class to indicate the start of the subprocess.
        """
        pass

    def close(self):
        """
        Free any resources obtained by the condition.
        """
        if self.closed:
            return
        self._close()
        self.closed = True

    def _close(self):
        pass

    def __enter__(self) -> ProcessCondition:
        """
        Enter the runtime context related to this object.
        """
        self.prepare()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        traceback: TracebackType,
    ):
        """
        Exit the runtime context related to this object.
        """
        self._close()

    def check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        response = self._check(process_status)
        self.history.append(response)
        return response

    def _check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        raise NotImplementedError()


SIDECAR_DEFAULT_HEALTH_PORT = 8200
SOCKET_BUFFER_SIZE = 1024


# Socket timeout. Wait time upon receiving a message from sidecar.
# The connection to the socket is considered as timed out if it has to wait more than
# this threshold In seconds
_SOCKET_TIMEOUT_SECONDS = 1
# The time to wait for the sidecar, during which timeouts from the sidecar are ignored.
_SIDECAR_WAIT_PERIOD = timedelta(minutes=5)


class SidecarHealthCondition(ProcessCondition):
    """
    A health check that reads health status from the sidecar.

    The sidecar is another container that lives adjacent to the airflow container of the
    MWAA Fargate tasks. It is responsible for a couple of tasks, including health
    monitoring. The latter's logic sends back the result of its health assessment back
    to the main container, which is then read by this class.
    """

    def __init__(
        self,
        airflow_component: str,
        container_start_time: float,
        port: int = SIDECAR_DEFAULT_HEALTH_PORT,
    ):
        """
        :param airflow_component: The airflow component to check.
        :param container_start_time: The epoch in seconds, i.e. time.time(), when the
          container started.
        :param port: The port the sidecar sends health monitoring results to.
        """
        super().__init__()
        self.airflow_component = airflow_component
        self.port = port
        self.socket: socket.socket | None
        self.container_start_time: float = container_start_time

    def prepare(self):
        """
        Called by the Subprocess class to indicate the start of the subprocess.

        Here, we create the UDP socket that listens for health messages from the
        MWAA sidecar.
        """
        self.socket = socket.socket(
            socket.AF_INET,  # Internet
            socket.SOCK_DGRAM,  # UDP
        )
        logger.info(f"Binding to port {self.port}")
        self.socket.bind(("127.0.0.1", self.port))
        self.socket.settimeout(_SOCKET_TIMEOUT_SECONDS)

    def _close(self):
        """
        Free the socket that was created.
        """
        if self.socket:
            self.socket.close()

    def _generate_autorestart_plog(self):
        """
        Generate a processable log that the service can ingest to know that a restart
        on an Airlfow worker/scheduler has happened and report health metrics.
        """

        # Unlike normal logs, plogs are ingested by the service to take various actions.
        # Hence, we always use 'print', to avoid log level accidentally stopping them.
        print(
            generate_plog(
                "AutoRestartLogsProcessor",
                f"[{self.airflow_component}] Restarting process...",
            )
        )

    @throttle(seconds=60, instance_level_throttling=True) # avoid excessive calls to process conditions
    def _check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        if self.socket is None:
            raise RuntimeError(
                "Unexpected error: socket object and start time shouldn't be None."
            )
        try:
            status, _ = self.socket.recvfrom(SOCKET_BUFFER_SIZE)
            status = status.decode("utf-8")
            match status.lower():
                case "red":
                    response = ProcessConditionResponse(
                        condition=self,
                        successful=False,
                        message=f"Status received from sidecar: {status}",
                    )
                    logger.error(response.message)
                case "blue" | "yellow":
                    # We treat blue/yellow as healthy to avoid unnecessary restarts,
                    # but we log a warning.
                    response = ProcessConditionResponse(
                        condition=self,
                        successful=True,
                        message=f"Status received from sidecar: {status}",
                    )
                    logger.warning(response.message)
                case "healthy":
                    response = ProcessConditionResponse(
                        condition=self,
                        successful=True,
                        message=f"Status received from sidecar: {status}",
                    )
                    logger.info(response.message)
                case _:
                    response = ProcessConditionResponse(
                        condition=self,
                        successful=True,
                        message=f"Unexpected response retrieved from sidecar: {status}. "
                        "Treating the status as HEALTHY. This may be a false positive "
                        "so it should be investigated, unless it is happening at the "
                        "start of the container before the sidecar monitoring is up "
                        "and emitting health indicators.",
                    )
                    logger.warning(response.message)
        except Exception:
            if (
                time.time() - self.container_start_time
                > _SIDECAR_WAIT_PERIOD.total_seconds()
            ):
                response = ProcessConditionResponse(
                    condition=self,
                    successful=True,
                    message="Reading the health status from the sidecar timed out. "
                    "Unable to positively determine health, so assuming healthy. This "
                    "may be a false positive so it should be investigated.",
                )
                logger.error(response.message, exc_info=sys.exc_info())
            else:
                response = ProcessConditionResponse(
                    condition=self,
                    successful=True,
                    message="Reading the health status from the sidecar timed out, but "
                    "ignoring this since the container just started, so the sidecar "
                    "monitoring might not have been initialized yet.",
                )
                logger.info(response.message)

        if not response.successful:
            self._generate_autorestart_plog()

        return response


class TimeoutCondition(ProcessCondition):
    """
    A timeout condition is used to control the running time of a process.

    A timeout condition always checks, except when the process has run for more than the
    allowed time, at which point this condition fails check, and results in terminating
    the process.
    """

    def __init__(self, timeout: timedelta):
        """
        Initialize the timeout condition.

        :param timeout: The maximum time the process is allowed to run.
        """
        super().__init__()
        self.timeout = timeout
        self.start_time: float | None = None

    def prepare(self):
        """
        Called by the Subprocess class to indicate the start of the subprocess.

        Here, we set the `start_time` field to save the time the process started.
        """
        self.start_time = time.time()

    @throttle(seconds=60, instance_level_throttling=True) # avoid excessive calls to process conditions
    def _check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        if not self.start_time:
            raise RuntimeError("TimeoutCondition has not been initialized")
        running_time_ms = (time.time() - self.start_time) * 1000
        timeout_ms = self.timeout.total_seconds() * 1000
        if running_time_ms < timeout_ms:
            return ProcessConditionResponse(condition=self, successful=True)
        else:
            return ProcessConditionResponse(
                condition=self,
                successful=False,
                message=f"Process timed out after running for more than "
                f"{running_time_ms} milliseconds when the maximum running time "
                f"is {timeout_ms} milliseconds.",
            )


class AirflowDbReachableCondition(ProcessCondition):
    """
    A condition for ensuring the Airflow database is reachable.
    """

    def __init__(self, airflow_component: str):
        """
        Initialize the check object.

        :param airflow_component: The airflow component to check.
        """
        super().__init__()
        self.airflow_component = airflow_component
        self.healthy: bool = True

    def prepare(self):
        """
        Initialize the condition.
        """
        # values taken from entrypoint.sh
        engine_args = {}
        if not self._is_db_connection_pooling_enabled:
            logging.info(
                "Connection pooling is disabled. AirflowDbReachableCondition will not pool connections."
            )
            engine_args["poolclass"] = NullPool
        else:
            logging.info(
                "Connection pooling is enabled. AirflowDbReachableCondition will pool connections."
            )
        self.engine = create_engine(
            get_db_connection_string(),
            connect_args=MWAA_CONNECT_ARGS,
            **engine_args,
        )

    def _generate_health_plog(self, healthy: bool, health_changed: bool):
        if health_changed:
            health_status = (
                "CONNECTION_BECAME_HEALTHY"
                if healthy
                else "CONNECTION_BECAME_UNHEALTHY"
            )
        else:
            health_status = "CONNECTION_HEALTHY" if healthy else "CONNECTION_UNHEALTHY"
        # Unlike normal logs, plogs are ingested by the service to take various actions.
        # Hence, we always use 'print', to avoid log level accidentally stopping them.
        print(
            generate_plog(
                "RDSHealthLogsProcessor",
                f"[{self.airflow_component}] connection with RDS Meta DB is {health_status}.",
            )
        )

    @throttle(seconds=60, instance_level_throttling=True) # avoid excessive calls to process conditions
    def _check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        # only run db healthcheck from scheduler for now.
        logging.info("Performing health check on Airflow DB.")

        # test db connectivity
        try:
            # try connection to the RDS metadata db and run a test query
            with self.engine.connect() as connection:  # type: ignore
                connection.execute(text("SELECT 1"))  # type: ignore
            healthy = True
            message = "Successfully connected to database."
            logger.info(message)
        except Exception as ex:
            healthy = False
            message = f"Couldn't connect to database. Error: {ex}"
            logger.error(message)
        response = ProcessConditionResponse(
            condition=self,
            # This condition is currently informational only, to report metrics and
            # logs. We have an issue to implement a restart after a certain grace
            # period: https://github.com/aws/amazon-mwaa-docker-images/issues/75
            successful=True,
            message=message,
        )
        self._generate_health_plog(
            healthy,
            healthy != self.healthy,  # compare the current health against the last.
        )
        self.healthy = healthy
        return response

    @cached_property
    def _is_db_connection_pooling_enabled(self) -> bool:
        return conf.getboolean(  # type: ignore
            "database", "sql_alchemy_pool_enabled", fallback=False
        )


class TaskMonitoringCondition(ProcessCondition):
    """
    A condition for regularly communicating with the worker task monitor to ensure
    graceful shutdown of workers in case of auto scaling.

    :param worker_task_monitor: The worker task monitor. See the implementation of
      WorkerTaskMonitor for more details on what this monitor does.
    :param terminate_if_idle: Whether to terminate the worker if it is idle.
    """

    WORKER_MONITOR_CLOSED_TIME_THRESHOLD = timedelta(seconds=30)


    def __init__(
        self,
        worker_task_monitor: WorkerTaskMonitor,
        terminate_if_idle: bool,
    ):
        """
        Initialize the instance.

        :param worker_task_monitor: The worker task monitor. See the implementation of
          WorkerTaskMonitor for more details on what this monitor does.
        """
        super().__init__()
        self.worker_task_monitor = worker_task_monitor
        self.terminate_if_idle = terminate_if_idle
        self.stats = get_statsd()
        self.last_check_time = 0
        self.time_since_closed = 0

    def prepare(self):
        """
        Initialize the condition.
        """
        pass

    def _close(self):
        """
        Close the auto scaling condition.
        """
        self.worker_task_monitor.close()

    def _get_failed_condition_response(self, message: str) -> ProcessConditionResponse:
        """
        Get the failed condition response.

        :param message: The message to include in the response.
        :returns The failed condition response.
        """
        logger.info(message)
        return ProcessConditionResponse(
            condition=self,
            successful=False,
            message=message,
        )

    def _publish_metrics(self):
        is_closed = self.worker_task_monitor.is_closed()
        dt = time.time() - self.last_check_time
        self.last_check_time = time.time()
        if not is_closed:
            self.time_since_closed = 0
        else:
            self.time_since_closed += dt
        self.stats.incr("mwaa.task_monitor.task_count", self.worker_task_monitor.get_current_task_count())
        self.stats.incr("mwaa.task_monitor.cleanup_task_count", self.worker_task_monitor.get_cleanup_task_count())
        if self.time_since_closed > TaskMonitoringCondition.WORKER_MONITOR_CLOSED_TIME_THRESHOLD.total_seconds():
            self.stats.incr("mwaa.task_monitor.worker_shutdown_delayed", 1)



    @throttle(seconds=10, instance_level_throttling=True) # avoid excessive calls to process conditions
    def _check(self, process_status: ProcessStatus) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """

        if process_status == ProcessStatus.RUNNING:
            self.worker_task_monitor.cleanup_abandoned_resources()
            self.worker_task_monitor.process_next_signal()
            # If the allowed time limit for waiting for activation signal has been breached, then we give up on further wait and exit.
            if self.worker_task_monitor.is_activation_wait_time_limit_breached():
                return self._get_failed_condition_response("Allowed time limit for activation has been breached. Exiting")
            # If the worker is marked to be killed, then we exit the worker without waiting for the tasks to be completed.
            elif self.worker_task_monitor.is_marked_for_kill():
                return self._get_failed_condition_response("Worker has been marked for kill. Exiting.")
            # If the worker is marked to be terminated, then we exit the worker after waiting for the tasks to be completed.
            elif self.worker_task_monitor.is_marked_for_termination():
                logger.info("Worker has been marked for termination, checking for idleness before terminating.")
                if self.worker_task_monitor.is_worker_idle():
                    return self._get_failed_condition_response("Worker marked for termination has become idle. Exiting.")
                elif self.worker_task_monitor.is_termination_time_limit_breached():
                    return self._get_failed_condition_response("Allowed time limit for graceful termination has been breached. Exiting")
                else:
                    logger.info("Worker marked for termination is NOT yet idle. Waiting.")
            elif self.terminate_if_idle and self.worker_task_monitor.is_worker_idle():
                # After detecting worker idleness, we pause further work consumption via
                # Celery, wait and check again for idleness.
                logger.info("Worker process is idle and needs to be terminated. Pausing task consumption.")
                self.worker_task_monitor.pause_task_consumption()
                if self.worker_task_monitor.is_worker_idle():
                    return self._get_failed_condition_response("Worker which should be terminated if idle has been "
                                                               "found to be idle. Exiting.")
                else:
                    logger.info("Worker picked up new tasks during shutdown, reviving worker.")
                    self.worker_task_monitor.resume_task_consumption()
                    self.worker_task_monitor.reset_monitor_state()
            else:
                logger.info("Worker process is either NOT idle or has not been marked for termination. No action is needed.")
        else:
            logger.info(
                f"Worker process finished (status is {process_status.name}). "
                "No need to monitor tasks anymore."
            )

        self._publish_metrics()

        # For all other scenarios, the condition defaults to returning success.
        return ProcessConditionResponse(
            condition=self,
            successful=True,
        )
