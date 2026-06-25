"""A module containing various utility functions related to logging."""

# Python imports
from types import TracebackType
from typing import Mapping, Tuple, TypeAlias
import logging


# These type aliases are borrowed directly from Python's logging module. They are
# defined as private, so using them triggers a linting error.
SysExcInfoType: TypeAlias = (
    tuple[type[BaseException], BaseException, TracebackType | None]
    | tuple[None, None, None]
)
ArgsType: TypeAlias = tuple[object, ...] | Mapping[str, object]
ExcInfoType: TypeAlias = None | bool | SysExcInfoType | BaseException


class CompositeLogger(logging.Logger):
    """
    A composite logger that forwards log messages to multiple underlying loggers.

    This class allows you to aggregate multiple logger instances and treat them as a
    single logger. When you log a message using this composite logger, it will propagate
    the message to all the underlying loggers.

    An example useful use case is if we want to tee the output of a subprocess as well
    as sending its log to CloudWatch. So, instead of adding special code to also tee the
    output of the subprocess, we can simply create a composite logger and add both the
    subprocess logger and an stdout logger.
    """

    def __init__(self, name: str, *loggers: logging.Logger) -> None:
        """
        Initialize the CompositeLogger with multiple logger instances.

        Args:
            *loggers (logging.Logger): Variable number of logger instances to be aggregated.

        """
        super().__init__(name)
        self.loggers: Tuple[logging.Logger, ...] = loggers

    def _log(
        self,
        level: int,
        msg: object,
        args: ArgsType,
        exc_info: ExcInfoType = None,
        extra: Mapping[str, object] | None = None,
        stack_info: bool = False,
        stacklevel: int = 1,
    ):
        for logger in self.loggers:
            logger._log(level, msg, args, exc_info, extra, stack_info, stacklevel)
