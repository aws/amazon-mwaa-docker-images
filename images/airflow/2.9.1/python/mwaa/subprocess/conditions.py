"""
Contain various conditions that can be added to a process.

We frequently need to monitor and control running processes. For example, we might want
to limit the running time of a process to X minutes. Or, we might need to monitor the
health of a process and restart it if necessary. Process conditions make this possible.
"""

from __future__ import annotations

# Python imports
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property
from types import TracebackType
from dateutil.tz import tz
import logging
import socket
import sys
import time

# 3rd-party imports
from airflow.configuration import conf
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Our imports
from mwaa.config.database import get_db_connection_string

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


class ProcessCondition:
    """
    Base class for all process conditions.

    A process condition is, as the name suggests, a condition that must be satisfied
    for a process to continue running. The
    """

    def __init__(self, name: str | None = None):
        """
        Initialize the process condition.

        :param name: The name of the condition. You can pass None for this to use the
          name of the class of the condition, which is usually sufficient, unless you
          want to customize the name.
        """
        self.name = name if name else self.__class__.__name__

    def prepare(self):
        """
        Called by the Subprocess class to indicate the start of the subprocess.
        """
        pass

    def close(self):
        """
        Free any resources obtained by the condition.
        """
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
        self.close()

    def check(self) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
        raise NotImplementedError()


SIDECAR_ADDRESS = "127.0.0.1"
SIDECAR_HEALTH_PORT = 8200
SOCKET_BUFFER_SIZE = 1024


# Socket timeout. Wait time upon receiving a message from sidecar.
# The connection to the socket is considered as timed out if it has to wait more than
# this threshold In seconds
SOCKET_TIMEOUT_SECONDS = 1


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
        host: str = SIDECAR_ADDRESS,
        port: int = SIDECAR_HEALTH_PORT,
    ):
        """
        :param airflow_component: The airflow component to check.
        :param host: The host where the sidecar lives. This is currently 127.0.0.1, but
          a custom value can be provided if needed.
        :param port: The port the sidecar sends health monitoring results to.
        """
        super().__init__()
        self.airflow_component = airflow_component
        self.host = host
        self.port = port
        self.socket: socket.socket | None

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
        self.socket.bind((self.host, self.port))
        self.socket.settimeout(SOCKET_TIMEOUT_SECONDS)

    def close(self):
        """
        Free the socket that was created
        """
        if self.socket:
            self.socket.close()

    def check(self) -> ProcessConditionResponse:
        """
        Execute the condition and return the response.

        :returns A ProcessConditionResponse containing data about the response.
        """
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
            response = ProcessConditionResponse(
                condition=self,
                successful=True,
                message="Reading the health status from the sidecar timed out. Unable "
                "to positively determine health. Assuming status is healthy. This may "
                "be a false positive so it should be investigated, unless it is "
                "happening at the start of the container before the sidecar monitoring "
                "is up and emitting health indicators.",
            )
            logger.error(response.message, exc_info=sys.exc_info())

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
        self.timeout = timeout
        self.start_time: float | None = None

    def prepare(self):
        """
        Called by the Subprocess class to indicate the start of the subprocess.

        Here, we set the `start_time` field to save the time the process started.
        """
        self.start_time = time.time()

    def check(self) -> ProcessConditionResponse:
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

    def prepare(self):
        """
        Initialize the condition.
        """
        # values taken from entrypoint.sh
        engine_args = {}
        if not self._is_db_connection_pooling_enabled:
            logging.info(
                "Connection pooling is disabled. AirflowDbHealthCheck will not pool connections."
            )
            engine_args["poolclass"] = NullPool
        else:
            logging.info(
                "Connection pooling is enabled. AirflowDbHealthCheck will pool connections."
            )
        self.engine = create_engine(
            get_db_connection_string(),
            connect_args={"connect_timeout": 3},
            **engine_args,
        )

    def check(self) -> ProcessConditionResponse:
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
                connection.execute("SELECT 1")  # type: ignore
            response = ProcessConditionResponse(
                condition=self,
                successful=True,
                message="Successfully connected to database.",
            )
            logger.info(response.message)
        except Exception as ex:
            response = ProcessConditionResponse(
                condition=self,
                successful=False,
                message=f"Couldn't connect to database. Error: {ex}",
            )
            logger.error(response.message)
        return response

    @cached_property
    def _is_db_connection_pooling_enabled(self) -> bool:
        return conf.getboolean(  # type: ignore
            "database", "sql_alchemy_pool_enabled", fallback=False
        )
