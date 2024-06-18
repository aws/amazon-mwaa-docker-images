"""
This module contains the Subprocess class which we use manage subprocesses.

The Subprocess class is used for running Airflow components, e.g. scheduler, installing
requirements, and potentially other use cases requiring a sub-process. This class
supports capturing output from the sub-process and sending them to a Python logger. It
helps support CloudWatch Logs integration.
"""

# Python imports
from datetime import timedelta
from enum import Enum
from subprocess import Popen
from typing import Any, Dict, List
import atexit
import fcntl
import logging
import os
import signal
import subprocess
import sys
import time

# Our imports
from mwaa.logging.utils import throttle
from mwaa.subprocess.conditions import ProcessCondition


# The maximum time we can wait for a process to gracefully respond to a SIGTERM signal
# from us before we forcefully terminate the process with a SIGKILL.
_SIGTERM_DEFAULT_PATIENCE_INTERVAL = timedelta(seconds=90)


module_logger = logging.getLogger(__name__)


class ProcessStatus(Enum):
    """
    An enum that represents the status of a process.

    The status can be one of the following:
    - FINISHED_WITH_NO_MORE_LOGS: The process has finished and there are no more logs to
      read.
    - FINISHED_WITH_LOG_READ: The process has finished but a log was recently read,
      meaning there are potentially more logs that need to be read.
    - RUNNING_WITH_NO_LOG_READ: The process is running but no log was read in the latest
      attempt to read logs, thus a sleep is required before attempting to read more
      logs to avoid spiking the CPU.
    - RUNNING_WITH_LOG_READ: The process is running and a log was read in the latest
      attempt, thus we should continue trying to read more logs to avoid delaying
      logs publishing.
    """

    FINISHED_WITH_NO_MORE_LOGS = 0
    FINISHED_WITH_LOG_READ = 1
    RUNNING_WITH_NO_LOG_READ = 2
    RUNNING_WITH_LOG_READ = 3


class Subprocess:
    """A class for running sub-processes, monitoring them, and capturing their logs."""

    def __init__(
        self,
        *,
        cmd: List[str],
        env: Dict[str, str] = {**os.environ},
        logger: logging.Logger = module_logger,
        friendly_name: str | None = None,
        conditions: List[ProcessCondition] = [],
        sigterm_patience_interval: timedelta = _SIGTERM_DEFAULT_PATIENCE_INTERVAL,
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
        self.process_logger = logger if logger else module_logger
        self.friendly_name = friendly_name
        self.conditions = conditions
        self.sigterm_patience_interval = sigterm_patience_interval

        self.start_time: float | None = None
        self.process: Popen[Any] | None = None

    def __str__(self):
        """
        Return a string identifying the sub-process.

        If the user has specified a friendly name, this method will make use of it.
        Hence, it is recommended to specify a friendly name.

        :return A string identifying the sub-process.
        """
        if self.process and self.friendly_name:
            return f"Process {self.friendly_name} (PID {self.process.pid})"
        elif self.process:
            return f"Process with PID {self.process.pid}"
        else:
            return ""

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
            self.process = self._start_process()

            # Create a function closure that knows how to cleanup
            def cleanup():
                if self.process is not None:
                    self._kill_subprocess(self.process)

            atexit.register(cleanup)

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

            # TODO Create a handler that can be used to hook the code that gracefully
            # shutdowns the worker.

    def stop(self):
        """Stop the subprocess."""
        if self.process:
            self._kill_subprocess(self.process)
            self.process = None

    def close(self):
        """
        Close the subprocess.
        """
        for condition in self.conditions:
            condition.close()

    @throttle(60)  # so we don't make excessive calls to process conditions
    def _check_process_conditions(self):
        # Evaluate all conditions
        checked_conditions = [c.check() for c in self.conditions]

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
            raise RuntimeError("Process is not started")

        if self.process_status == ProcessStatus.FINISHED_WITH_NO_MORE_LOGS:
            # The process has finished and there are no more logs to read so we
            # just return False so the caller stop looping.
            return False

        self.process_status = self._capture_output_line_from_process(self.process)

        # Filter out the unsuccessful conditions
        failed_conditions = self._check_process_conditions()

        if failed_conditions:
            self.process_logger.error(
                f"""
{self} failed due to the following conditions failing:

{os.linesep.join([' - ' + c.message for c in failed_conditions])}

A SIGTERM followed potentially by a SIGKILL will be sent to terminate the process.
            """.strip()
            )
            self._kill_subprocess(self.process)
            self.process_status = ProcessStatus.FINISHED_WITH_NO_MORE_LOGS

        return self.process_status != ProcessStatus.FINISHED_WITH_NO_MORE_LOGS

    def _start_process(self) -> Popen[Any]:
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

    def _kill_subprocess(self, process: Popen[Any]):
        def _error(msg: str):
            # TODO Consider using a CompositeLogger instead if we need to log to console
            # and the process logger in multiple places in this module.
            module_logger.error(msg)
            self.process_logger.error(msg)

        # Do nothing if process has already terminated
        if process.poll() is not None:
            return
        _error(f"Killing {str(self)}")
        try:
            os.kill(process.pid, signal.SIGTERM)
        except OSError:
            _error(
                f"Failed to kill {str(self)} with a SIGTERM signal. "
                f"Failed to send signal {signal.SIGTERM}. Sending SIGKILL..."
            )
            os.kill(process.pid, signal.SIGKILL)
        sigterm_patience_interval_secs = self.sigterm_patience_interval.total_seconds()
        try:
            outs, _ = process.communicate(timeout=sigterm_patience_interval_secs)
            if outs:
                self.process_logger.info(outs.decode("utf-8"))
        except subprocess.TimeoutExpired:
            _error(
                f"Failed to kill {str(self)} with a SIGTERM signal. Process didn't "
                f"respond to SIGTERM after {sigterm_patience_interval_secs} "
                "seconds. Sending SIGKILL..."
            )
            os.kill(process.pid, signal.SIGKILL)
        _error("Process killed. Return code %s" % process.returncode)


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
    for s in subprocesses:
        s.start(False)  # False since we want to run the subprocesses in parallel

    read_some_logs = True
    while len(subprocesses) > 0:
        read_some_logs = False
        finished_processes: List[Subprocess] = []
        for s in subprocesses:
            if not s.execution_loop_iter():
                finished_processes.append(s)
            if s.process_status in [
                ProcessStatus.FINISHED_WITH_LOG_READ,
                ProcessStatus.RUNNING_WITH_LOG_READ,
            ]:
                read_some_logs = True

        # Remove finished processes from the list of running processes.
        subprocesses = [s for s in subprocesses if s not in finished_processes]

        finished_essential_processes = [
            s for s in finished_processes if s in essential_subprocesses
        ]

        if finished_essential_processes:
            names = [str(p) for p in finished_essential_processes]
            module_logger.error(
                f"The following essential process(es) exited: {', '.join(names)}. "
                "Terminating other subprocesses..."
            )
            for s in subprocesses:
                s.stop()
            break

        if not read_some_logs:
            # We didn't read any logs from any process. Sleep for a bit so we don't
            # enter into a tight loop that spikes the CPU.
            time.sleep(1)
