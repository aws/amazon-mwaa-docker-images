"""
The subprocess module contains important constructs for creating and managing processes.
"""

from enum import Enum


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
