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
from threading import Thread

# Our imports
from mwaa.logging.loggers import CompositeLogger
from mwaa.subprocess import ProcessStatus
from mwaa.subprocess.conditions import ProcessCondition, ProcessConditionResponse


# The maximum time we can wait for a process to gracefully respond to a SIGTERM signal
# from us before we forcefully terminate the process with a SIGKILL.
_SIGTERM_DEFAULT_PATIENCE_INTERVAL = timedelta(seconds=90)

# The time for which the log reader will sleep when subprocess is still running
# But last log read was empty.
_SUBPROCESS_LOG_POLL_IDLE_SLEEP_INTERVAL = timedelta(milliseconds=100)


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
        is_essential: bool = False
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
        :param is_essential: If process should be considered essential by process managers.
        """
        self.cmd = cmd
        self.env = env
        self.process_logger = process_logger if process_logger else module_logger
        self.is_essential = is_essential
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
        self.log_thread: Thread | None = None

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

            self.process_status = ProcessStatus.RUNNING

            self.log_thread = Thread(target=self._read_subprocess_log_stream, args=(self.process,))
            if auto_enter_execution_loop:
                self.log_thread.start()
                start_time = time.time()
                while self.execution_loop_iter():
                    dt = time.time() - start_time
                    time.sleep(max(1.0 - dt, 0))
                    start_time = time.time()
                self.log_thread.join()
        except Exception as ex:
            module_logger.fatal(
                "Unexpected error occurred while trying to start a process "
                f"for command '{self.cmd}': {ex}",
                exc_info=sys.exc_info(),
            )

    def _check_process_conditions(self) -> List[ProcessConditionResponse]:
        # Evaluate all conditions
        checked_conditions = [c.check(self.process_status) for c in self.conditions]

        # Filter out the unsuccessful conditions
        failed_conditions = [c for c in checked_conditions if c and not c.successful]

        return failed_conditions

    def _read_subprocess_log_stream(self, process: Popen[Any]):
        """
        Poll process stdout and forward logs to subprocess logger
        until process has terminated and stream is empty.
        If stream is empty but process is still running then sleep
        for small duration to avoid wasted cpu resources.
        """
        stream = process.stdout
        while True:
            if not stream or stream.closed:
                break
            line = stream.readline()
            if line == b"":
                if process.poll() is not None:
                    break
                else:
                    time.sleep(_SUBPROCESS_LOG_POLL_IDLE_SLEEP_INTERVAL.total_seconds())
            else:
                log_level = os.environ['AIRFLOW_CONSOLE_LOG_LEVEL']
                decoded_line = line.decode("utf-8", errors="replace").rstrip()
                match log_level:
                    case "ERROR":
                        self.process_logger.error(decoded_line)
                    case "WARNING":
                        self.process_logger.warning(decoded_line)
                    case _:
                        self.process_logger.info(decoded_line)
    
    def _get_subprocess_status(self, process: Popen[Any]):
        return ProcessStatus.RUNNING if process.poll() is None else ProcessStatus.FINISHED

    def execution_loop_iter(self):
        """
        Execute a single iteration of the execution loop.

        The execution loop is a loop that continuously monitors the status of the
        process. This method executes a single iteration and exits, expecting the
        caller to repeatedly call this method until it returns
        `False`, meaning that the process has exited.

        :return: True if the process is still running.
        """
        if not self.process:
            # Process is not running anymore.
            return False

        if self.process_status == ProcessStatus.FINISHED:
            # The process has finished.
            # just return False so the caller stop looping.
            return False

        self.process_status = self._get_subprocess_status(self.process)

        if self.process_status == ProcessStatus.FINISHED:
            # We are done; call shutdown to ensure that we free all resources.
            self.shutdown()
        elif self.process_status == ProcessStatus.RUNNING and self.conditions:
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
                self.process_status = ProcessStatus.FINISHED
                return False

        return True

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

    def shutdown(self):
        """Stop and close the subprocess and its resources."""
        if self.is_shut_down:
            # Already shut down.
            return
        self.is_shut_down = True

        if self.process and self.process_status == ProcessStatus.RUNNING:
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
            module_logger.info(f"Sending SIGTERM to process group {self}")
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            action_taken = "terminated"
        except OSError:
            module_logger.error(f"Failed to send SIGTERM to process group {self}. Sending SIGKILL...")
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            action_taken = "killed"

        sigterm_patience_interval_secs = self.sigterm_patience_interval.total_seconds()
        try:
            outs, _ = self.process.communicate(timeout=sigterm_patience_interval_secs)
            if outs:
                self.process_logger.info(outs.decode("utf-8"))
        except subprocess.TimeoutExpired:
            module_logger.error(
                f"Failed to kill {self} with a SIGTERM signal. Process group didn't "
                f"respond to SIGTERM after {sigterm_patience_interval_secs} "
                "seconds. Sending SIGKILL..."
            )
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            action_taken = "killed"

        module_logger.info(
            f"Process group {action_taken}. Return code is {self.process.returncode}."
        )

    def start_log_capture(self):
        """Start async capturing of logs from subprocess and forward them to process logger"""
        if self.log_thread:
            self.log_thread.start()

    def finish_log_capture(self):
        """Block until capturing of subprocess logs is complete"""
        if self.log_thread:
            self.log_thread.join()


def run_subprocesses(subprocesses: List[Subprocess]):
    """
    Run the given subprocesses in parallel.

    This utility function is useful if you want to have multiple processes running in
    parallel and you want to make sure that you ingest logs from all of them in
    parallel. This works by calling the start() method of each subprocess with a False
    value for the auto_enter_execution_loop parameter. This will result into starting
    the process but not monitoring its logs. The caller would then need to manually call
    the loop() method to ingest logs, which is what we do here for all processes.
    When a subprocess marked as essential exits, all other subprocesses will be shutdown.

    :param subprocesses: A list of Subprocess objects to run in parallel.
    """
    for s in subprocesses:
        s.start(False)  # False since we want to run the subprocesses in parallel
        s.start_log_capture()
    running_processes = subprocesses
    while len(running_processes) > 0:
        start_time = time.time()
        finished_processes: List[Subprocess] = []
        for s in running_processes:
            if not s.execution_loop_iter():
                finished_processes.append(s)

        # Remove finished processes from the list of running processes.
        running_processes = [s for s in running_processes if s not in finished_processes]

        finished_essential_processes = [
            s for s in finished_processes if s.is_essential
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
        dt = time.time() - start_time
        time.sleep(max(1.0 - dt, 0))
    for s in subprocesses:
        s.finish_log_capture()



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
