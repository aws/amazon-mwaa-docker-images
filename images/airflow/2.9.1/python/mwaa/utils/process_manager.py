# Python imports
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
import traceback


# The maximum time to wait for process termination when SIGTERM is sent. Unit: seconds
SIGTERM_TIMEOUT_SECONDS = 90


class ProcessStatus(Enum):
    FINISHED_WITH_NO_MORE_LOGS = 0
    FINISHED_WITH_LOG_READ = 1
    RUNNING_WITH_NO_LOG_READ = 2
    RUNNING_WITH_LOG_READ = 3


logger = logging.getLogger(__name__)


class Subprocess:
    def __init__(
        self,
        *,
        cmd: List[str],
        env: Dict[str, str] = {**os.environ},
        logger: logging.Logger = logger,
        tee_logs: bool = False,
    ):
        self.cmd = cmd
        self.env = env
        # TODO Should we use a different default logger?
        self.logger = logger if logger else logging.getLogger(__name__)
        self.process: Popen[Any] | None = None
        self.tee_logs = tee_logs

    def start(self):
        try:
            self.process = self._start_process()

            # Create a function closure that knows how to cleanup
            def cleanup():
                if self.process is not None:
                    self._kill_subprocess(self.process)

            atexit.register(cleanup)

            process_status = ProcessStatus.RUNNING_WITH_NO_LOG_READ

            while not process_status == ProcessStatus.FINISHED_WITH_NO_MORE_LOGS:
                process_status = self._capture_output_line_from_process(self.process)

                if process_status == ProcessStatus.RUNNING_WITH_NO_LOG_READ:
                    # There are no pending logs in the process, so we sleep for a while
                    # to avoid getting into a continuous execution that spikes the CPU
                    # and impacts the Airflow process.
                    time.sleep(1)
        except Exception as ex:
            print(f"Unexpected error occurred in console logging: {ex}")
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback)

            # TODO Create a handler that can be used to hook the code that gracefully
            # shutdowns the worker.

    def _start_process(self) -> Popen[Any]:
        """Start a new subprocess and return it"""

        print(f"Starting new subprocess for command '{self.cmd}'...")
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

        print(
            f"New subprocess for command '{self.cmd}' started. "
            f"New process ID is {process.pid}. "
            f"Parent process ID is {os.getpid()}."
        )
        return process

    def _capture_output_line_from_process(self, process: Popen[Any]):
        """Read log lines from the Airflow process and upload them to Cloudwatch"""
        if not process.stdout:
            # TODO - Is this the right exception to throw?
            raise RuntimeError("Process stdout is empty")
        line = process.stdout.readline()
        process_finished = process.poll() is not None
        if line == b"" and process_finished:
            return ProcessStatus.FINISHED_WITH_NO_MORE_LOGS
        if line:
            # Send the log to the logger.
            self.logger.info(line.decode("utf-8"))
            if self.tee_logs:
                print(line.decode("utf-8"))
            return (
                ProcessStatus.FINISHED_WITH_LOG_READ
                if process_finished
                else ProcessStatus.RUNNING_WITH_LOG_READ
            )
        else:
            return ProcessStatus.RUNNING_WITH_NO_LOG_READ

    def _kill_subprocess(self, process: Popen[Any]):
        # Do nothing if process has already terminated
        if process.poll() is not None:
            return
        print("Killing process %s" % process.pid)
        try:
            os.kill(process.pid, signal.SIGTERM)
        except OSError:
            print(
                "Failed to kill the process {0} with a SIGTERM signal. Failed to send signal {1}. Sending SIGKILL...".format(
                    process.pid, signal.SIGTERM
                )
            )
            os.kill(process.pid, signal.SIGKILL)
        try:
            outs, _ = process.communicate(timeout=SIGTERM_TIMEOUT_SECONDS)
            if outs:
                self.logger.info(outs.decode("utf-8"))
        except subprocess.TimeoutExpired:
            print(
                "Failed to kill the process {0} with a SIGTERM signal. Process timed out. Sending SIGKILL...".format(
                    process.pid
                )
            )
            os.kill(process.pid, signal.SIGKILL)
        print("Process killed. Return code %s" % process.returncode)
