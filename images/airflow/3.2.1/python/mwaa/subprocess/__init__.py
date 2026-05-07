"""
The subprocess module contains important constructs for creating and managing processes.
"""

from enum import Enum


class ProcessStatus(Enum):
    """
    An enum that represents the status of a process.

    The status can be one of the following:
    - FINISHED: The process has finished.
    - RUNNING: The process is running. 
    """

    FINISHED = 1
    RUNNING = 2
