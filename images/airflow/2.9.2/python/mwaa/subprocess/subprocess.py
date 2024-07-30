"""
This module contains the Subprocess class which we use manage subprocesses.

The Subprocess class is used for running Airflow components, e.g. scheduler, installing
requirements, and potentially other use cases requiring a sub-process. This class
supports capturing output from the sub-process and sending them to a Python logger. It
helps support CloudWatch Logs integration.
"""

# Python imports
from datetime import timedelta
from subprocess import Popen
from types import FrameType, TracebackType
from typing import Any, Callable, Dict, List, Optional
import atexit
import fcntl
import logging
import os
import signal
import subprocess
import sys
import time

# Our imports
from mwaa.logging.loggers import CompositeLogger
from mwaa.logging.utils import throttle
from mwaa.subprocess import ProcessStatus
from mwaa.subprocess.conditions import ProcessCondition, ProcessConditionResponse


# The maximum time we can wait for a process to gracefully respond to a SIGTERM signal
# from us before we forcefully terminate the process with a SIGKILL.
_SIGTERM_DEFAULT_PATIENCE_INTERVAL = timedelta(seconds=90)


module_logger = logging.getLogger(__name__)


_ALL_SUBPROCESSES: List["Subprocess"] = []


class Subprocess:
    """A class for running sub-processes, monitoring them, and capturing their logs."""

    def __init__(
        self,
        *,
        cmd: List[str],
        env: Dict[str, str] = {**os.environ},
        process_logger: logging.Logger = module_logger,
        friendly_name: str | None = None,
        conditions: List[ProcessCondition] = [],
        sigterm_patience_interval: timedelta = _SIGTERM_DEFAULT_PATIENCE_INTERVAL,
        on_sigterm: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize the Subprocess object.

        :param cmd: the command to run.
        :param env: A dictionary containing the environment variables to pass.
        :param logger: The logger object to use to publish logs coming from the process.
        :param timeout: If specified, the process will be terminated if it takes more
          time than what is specified here.
        :param friendly_name: If specified, this name will be used in log messages to
          refer to this process. When possible, it is recommended to set this.
        :param conditions: The conditions that must be met at all times, otherwise the
          process gets terminated. This can be useful for, for example, monitoring,
          limiting running time of the process, etc.
        """
        self.cmd = cmd
        self.env = env
        self.process_logger = process_logger if process_logger else module_logger
        # The dual logger is used in case we want to publish logs using both, the logger
        # of the process and the logger of this Python module. This is useful in case
        # some messages are useful to both, the customer (typically the customer's
        # CloudWatch) and the service (Fargate, i.e. the service's CloudWatch).
        self.dual_logger = CompositeLogger(
            "process_module_dual_logger",  # name can be anything unused.
            # We use a set to avoid double logging using the module logger if the user
            # doesn't pass a logger.
            *set([self.process_logger, module_logger]),
        )
        self.friendly_name = friendly_name
        self.name = "Unknown"
        self.conditions = conditions
        self.sigterm_patience_interval = sigterm_patience_interval
        self.on_sigterm = on_sigterm

        self.start_time: float | None = None
        self.process: Popen[Any] | None = None

        self.is_shut_down = False

        # Add this subprocess to the global list of subprocesses to be able to stop it
        # in case of SIGTERM signal.
        _ALL_SUBPROCESSES.append(self)

    def _set_name(self):
        if self.process and self.friendly_name:
            self.name = f'Process "{self.friendly_name}" (PID {self.process.pid})'
        elif self.process:
            self.name = f"Process with PID {self.process.pid}"
        else:
            self.name = "Unknown"

    def __str__(self):
        """
        Return a string identifying the sub-process.

        If the user has specified a friendly name, this method will make use of it.
        Hence, it is recommended to specify a friendly name.

        :return A string identifying the sub-process.
        """
        return self.name

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
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
        self.shutdown()

    def start(
        self,
        auto_enter_execution_loop: bool = True,
    ):
        """
        Start the subprocess.

        If auto_enter_execution_loop is set to True, this method enters a loop that
        monitors the process and captures its logs until the process finishes and there
        are no more logs to capture.

        :param auto_enter_execution_loop: If True, this method will automatically enter
          into execution loop and will not exit until the process finishes. If False,
          the caller will be responsible for entering the loop. The latter case is
          useful if the caller wants to run multiple sub-processes and have them run in
          parallel.
        """

        # Initialize process conditions if any.
        for condition in self.conditions:
            condition.prepare()

        try:
            self.process = self._create_python_subprocess()
            # Stores a friendly name for the process for logging purposes.
            self._set_name()

            # Stop the process at exit.
            atexit.register(self.shutdown)

            self.process_status = ProcessStatus.RUNNING_WITH_NO_LOG_READ

            if auto_enter_execution_loop:
                while self.execution_loop_iter():
                    if self.process_status == ProcessStatus.RUNNING_WITH_NO_LOG_READ:
                        # There are no pending logs in the process, so we sleep for a while
                        # to avoid getting into a continuous execution that spikes the CPU
                        # and impacts the Airflow process.
                        time.sleep(1)
        except Exception as ex:
            module_logger.fatal(
                "Unexpected error occurred while trying to start a process "
                f"for command '{self.cmd}': {ex}",
                exc_info=sys.exc_info(),
            )

    @throttle(60)  # so we don't make excessive calls to process conditions
    def _check_process_conditions(self) -> List[ProcessConditionResponse]:
        # Evaluate all conditions
        checked_conditions = [c.check(self.process_status) for c in self.conditions]

        # Filter out the unsuccessful conditions
        failed_conditions = [c for c in checked_conditions if not c.successful]

        return failed_conditions

    def execution_loop_iter(self):
        """
        Execute a single iteration of the execution loop.

        The execution loop is a loop that continuously monitors the status of the
        process and captures logs if any. This method executes a single iteration and
        exit, expecting the caller to repeatedly call this method until it returns
        `False`, meaning that the process has exited and there are no more logs to
        ingest.

        :return: True if the process is still running and/or there are more logs to read.
        """
        if not self.process:
            # Process is not running anymore.
            return False

        if self.process_status == ProcessStatus.FINISHED_WITH_NO_MORE_LOGS:
            # The process has finished and there are no more logs to read so we
            # just return False so the caller stop looping.
            return False

        self.process_status = self._capture_output_line_from_process(self.process)

        process_completed_and_logs_processed = (
            self.process_status == ProcessStatus.FINISHED_WITH_NO_MORE_LOGS
        )

        if process_completed_and_logs_processed:
            # We are done; call shutdown to ensure that we free all resources.
            self.shutdown()
        elif self.process_status in [
            ProcessStatus.RUNNING_WITH_NO_LOG_READ,
            ProcessStatus.RUNNING_WITH_LOG_READ,
        ]:
            # The process is still running, so we need to check conditions.
            failed_conditions = self._check_process_conditions()

            if failed_conditions:
                module_logger.warning(
                    f"""
{self} is being stopped due to the following reason(s): {' '.join([
    f'{i+1}) {c.message}' for i, c in enumerate(failed_conditions)
])}
""".strip()
                )
                self.shutdown()
                self.process_status = ProcessStatus.FINISHED_WITH_NO_MORE_LOGS
                process_completed_and_logs_processed = True

        return not process_completed_and_logs_processed

    def _create_python_subprocess(self) -> Popen[Any]:
        module_logger.info(f"Starting new subprocess for command '{self.cmd}'...")
        self.start_time = time.time()
        process = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Send to stdout so we can see it in the logs
            start_new_session=True,
            env=self.env,
        )

        # Make the stdout of the process non-blocking so the management and monitoring
        # code in this class can still run.
        if process.stdout is not None:
            fl = fcntl.fcntl(process.stdout, fcntl.F_GETFL)
            fcntl.fcntl(process.stdout, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        module_logger.info(
            f"New subprocess for command '{self.cmd}' started. "
            f"New process ID is {process.pid}. "
            f"Parent process ID is {os.getpid()}."
        )
        return process

    def _capture_output_line_from_process(self, process: Popen[Any]):
        """
        Read log lines from the Airflow process and upload them to CloudWatch.

        :param process: The process to read lines from.

        :return: The process status. See ProcessStatus enum for the possible values.
        """
        line = b""
        if process.stdout and not process.stdout.closed:
            line = process.stdout.readline()
        process_finished = process.poll() is not None
        if line == b"" and process_finished:
            return ProcessStatus.FINISHED_WITH_NO_MORE_LOGS
        if line:
            # Send the log to the logger.
            self.process_logger.info(line.decode("utf-8"))
            return (
                ProcessStatus.FINISHED_WITH_LOG_READ
                if process_finished
                else ProcessStatus.RUNNING_WITH_LOG_READ
            )
        else:
            return ProcessStatus.RUNNING_WITH_NO_LOG_READ

    def shutdown(self):
        """Stop and close the subprocess and its resources."""
        if self.is_shut_down:
            # Already shut down.
            return
        self.is_shut_down = True

        if self.process and self.process_status in [
            ProcessStatus.RUNNING_WITH_NO_LOG_READ,
            ProcessStatus.RUNNING_WITH_LOG_READ,
        ]:
            self._shutdown_python_subprocess(self.process)
        self.process = None

        for condition in self.conditions:
            condition.close()

    def _shutdown_python_subprocess(self, process: Popen[Any]):
        # Do nothing if process has already terminated
        if self.process is None or self.process.poll() is not None:
            return
        module_logger.info(f"Shutting down {self}")
        try:
            module_logger.info(f"Sending SIGTERM to {self}")
            self.process.terminate()
            action_taken = "terminated"
        except OSError:
            module_logger.error(f"Failed to send SIGTERM to {self}. Sending SIGKILL...")
            self.process.kill()
            action_taken = "killed"
        sigterm_patience_interval_secs = self.sigterm_patience_interval.total_seconds()
        try:
            outs, _ = self.process.communicate(timeout=sigterm_patience_interval_secs)
            if outs:
                self.process_logger.info(outs.decode("utf-8"))
        except subprocess.TimeoutExpired:
            module_logger.error(
                f"Failed to kill {self} with a SIGTERM signal. Process didn't "
                f"respond to SIGTERM after {sigterm_patience_interval_secs} "
                "seconds. Sending SIGKILL..."
            )
            self.process.kill()
            action_taken = "killed"
        module_logger.info(
            f"Process {action_taken}. Return code is {self.process.returncode}."
        )


def run_subprocesses(
    subprocesses: List[Subprocess], essential_subprocesses: List[Subprocess] = []
):
    """
    Run the given subprocesses in parallel.

    This utility function is useful if you want to have multiple processes running in
    parallel and you want to make sure that you ingest logs from all of them in
    parallel. This works by calling the start() method of each subprocess with a False
    value for the auto_enter_execution_loop parameter. This will result into starting
    the process but not monitoring its logs. The caller would then need to manually call
    the loop() method to ingest logs, which is what we do here for all processes.

    :param subprocesses: A list of Subprocess objects to run in parallel.
    :param essential_subprocesses: A sub-list of the processes that that must continue
      running, otherwise all sub-processes will be terminated. This is useful when we
      want to have multiple sub-processes running any container, and exit the container
      if any of them fails, e.g. the scheduler container, which contains the scheduler,
      triggerer, and DAG processor.
    """
    all_processes = subprocesses + essential_subprocesses
    for s in all_processes:
        s.start(False)  # False since we want to run the subprocesses in parallel
    running_processes = all_processes
    read_some_logs = True
    while len(running_processes) > 0:
        read_some_logs = False
        finished_processes: List[Subprocess] = []
        for s in running_processes:
            if not s.execution_loop_iter():
                finished_processes.append(s)
            if s.process_status in [
                ProcessStatus.FINISHED_WITH_LOG_READ,
                ProcessStatus.RUNNING_WITH_LOG_READ,
            ]:
                read_some_logs = True

        # Remove finished processes from the list of running processes.
        running_processes = [s for s in running_processes if s not in finished_processes]

        finished_essential_processes = [
            s for s in finished_processes if s in essential_subprocesses
        ]

        if finished_essential_processes:
            names = [str(p) for p in finished_essential_processes]
            module_logger.warning(
                f"The following essential process(es) exited: {', '.join(names)}. "
                "Terminating other subprocesses..."
            )
            for s in running_processes:
                s.shutdown()
            break

        if not read_some_logs:
            # We didn't read any logs from any process. Sleep for a bit so we don't
            # enter into a tight loop that spikes the CPU.
            time.sleep(1)


def _sigterm_handler(signal: int, frame: Optional[FrameType]):
    # TODO The shutdown() method blocks for up to 90 seconds (as specified by
    # _SIGTERM_DEFAULT_PATIENCE_INTERVAL), which is problematic with multiple processes,
    # especially on Fargate (used by MWAA), as the total time could exceed Fargate's
    # 2-minute limit before SIGKILL.
    #
    # Currently, however, only the worker takes a long time to shut down, and in that
    # case we don't have multiple processes running. In the future, however, we need to
    # convert this class to use asyncio to send SIGTERMs in parallel. This work will be
    # tracked here: https://github.com/aws/amazon-mwaa-docker-images/issues/110.

    module_logger.info("Caught SIGTERM, shutting down running processes...")

    while _ALL_SUBPROCESSES:
        p = _ALL_SUBPROCESSES.pop()
        if not p.is_shut_down and p.on_sigterm:
            p.on_sigterm()
        p.shutdown()


# TODO We need to make sure that no other part in the code base makes use of this method
# as this will remove our handler. We can do this via a quality check script that scans
# our code and ensures this, similar to the pip_install_check.py script. This will be
# tracked here: https://github.com/aws/amazon-mwaa-docker-images/issues/111
signal.signal(signal.SIGTERM, _sigterm_handler)
