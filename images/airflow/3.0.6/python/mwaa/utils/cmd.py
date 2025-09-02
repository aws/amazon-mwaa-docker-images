"""A module containing utility functions for executing sub-commands."""

# Python imports
import asyncio
import logging
import logging.config
import os
import subprocess
import sys
from asyncio import StreamReader
from typing import Callable

logger = logging.getLogger(__name__)


def abort(err_msg: str, exit_code: int = 1):
    """
    Print an error message and then exit the process with the given exit code.

    :param err_msg: The error message to print before exiting.
    :param exit_code: The exit code.
    """
    logger.error(err_msg)
    sys.exit(exit_code)


class CommandError(RuntimeError):
    """An exception used by the `run_command` method in case of execution errors."""

    def __init__(self, return_code: int, command: str):
        """
        Create a `CommandError` exception instance.

        :param return_code: The return code of the command.
        :param command: The command that was executed.
        """
        self.return_code = return_code
        self.command = command
        super().__init__(
            f"Command '{command}' exited with non-zero status {return_code}"
        )


async def run_command(
    command: str,
    env: dict[str, str] = {**os.environ},
    stdout_logging_method: Callable[[str], None] | None = None,
    stderr_logging_method: Callable[[str], None] | None = None,
):
    """
    Run a command asynchronously.

    :param command: The command to run.
    :param env: The environment variables to set. By default, all current environment
      variables are used.
    :param stdout_logging_method: The method to use for streaming stdout logs. If not
      specified, the `info` method of the logger object for this module is used.
    :param stderr_logging_method: The method to use for streaming stderr logs. If not
      specified, the `error` method of the logger object for this module is used.
    """
    if not stdout_logging_method:
        stdout_logging_method = logging.getLogger(__name__).info
    if not stderr_logging_method:
        # Use the "info" method for stderr as well as shell scripts frequently use
        # stderr even for informational output (e.g. in case the stdout should be
        # allowed to be piped to another process, hence non-pipeable info is sent to
        # stderr), and in such cases using the "error" method can be misleading if
        # "[ERROR]" appears in logs when in fact there are no errors.
        stderr_logging_method = logging.getLogger(__name__).info

    # Start the subprocess
    process: asyncio.subprocess.Process = await asyncio.create_subprocess_shell(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    logger.info(f"Created a process (PID={process.pid}) to execute command {command}.")

    # Read stdout and stderr asynchronously
    async def stream_output(
        stream: StreamReader, logging_method: Callable[[str], None]
    ) -> None:
        while True:
            line: bytes = await stream.readline()
            if not line:
                break
            logging_method(line.decode().strip())

    # Schedule reading stdout and stderr concurrently
    await asyncio.gather(
        stream_output(process.stdout, stdout_logging_method),  # type: ignore
        stream_output(process.stderr, stderr_logging_method),  # type: ignore
    )

    # Wait for the process to complete
    return_code = await process.wait()

    # Raise exception if return code is non-zero
    if return_code != 0:
        raise CommandError(return_code, command)
