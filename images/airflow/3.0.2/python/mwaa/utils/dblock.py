"""
This module contains helpful utility for dealing with database locks.

In particular, it contains the nice `with_db_lock` decorator that automatically protects
the execution of the function it is applied to by a database lock.
"""

# Python imports
import asyncio
import logging
import logging.config
from typing import Any, Awaitable, Callable, TypeVar, Union, cast

# 3rd party imports
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Our imports
from mwaa.config.database import get_db_connection_string

logger = logging.getLogger(__name__)


F = TypeVar("F", bound=Callable[..., Any])


def _obtain_db_lock(conn: Any, lock_id: int, timeout_ms: int, friendly_name: str):
    try:
        logger.info(f"Obtaining lock for {friendly_name}...")
        conn.execute(text("SET LOCK_TIMEOUT to :timeout"), {"timeout": timeout_ms})
        conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": lock_id})
        logger.info(f"Obtained lock for {friendly_name}...")
    except Exception as ex:
        raise DbLockError("Failed to obtain lock for {friendly_name}.", ex)


def _release_db_lock(conn: Any, lock_id: int, friendly_name: str):
    try:
        logger.info(f"Releasing lock for {friendly_name}...")
        conn.execute(text("SET LOCK_TIMEOUT TO DEFAULT"))
        conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": lock_id})
        logger.info(f"Released lock for {friendly_name}")
    except Exception as ex:
        msg = "Failed to release lock for {friendly_name}."
        logging.error(msg)
        raise DbLockError(msg, ex)


class DbLockError(RuntimeError):
    """
    Exception raised for errors related to database locking.

    This exception is intended to be raised when there are issues with acquiring
    or releasing locks in a database operation.

    Inherits from:
        RuntimeError: The built-in Python runtime error class.

    Example usage:
        try:
            # Database locking operation
        except DbLockError as e:
            # Handle the error
    """

    pass


def with_db_lock(
    lock_id: int, timeout_ms: int = 300 * 1000
) -> Callable[
    [Union[Callable[..., Any], Callable[..., Awaitable[Any]]]],
    Union[Callable[..., Any], Callable[..., Awaitable[Any]]],
]:
    """
    Generate a decorator that can be used to protect a function by a database lock.

    This is useful when a function needs to be protected against multiple simultaneous
    executions. For example, during Airflow database initialization, we want to make
    sure that only one process is doing it. Since normal lock mechanisms only apply to
    the same process, a database lock becomes a viable solution.

    :param lock_id: A unique ID for the lock. When multiple processes try to use the
      same lock ID, only one process will be granted the lock at one time. However,
      if the processes have different lock IDs, they will be granted the locks at the
      same time.
    :param timeout_ms: The maximum time, in milliseconds, the process is allowed to hold
      the lock. After this time expires, the lock is automatically released.

    :returns A decorator that can be applied to a function to protect it with a DB lock.
    """

    def decorator(
        func: Union[Callable[..., Any], Callable[..., Awaitable[Any]]],
    ) -> Union[Callable[..., Any], Callable[..., Awaitable[Any]]]:
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name: str = func.__name__
            db_engine: Engine = create_engine(get_db_connection_string())
            with db_engine.connect() as conn:  # type: ignore
                try:
                    _obtain_db_lock(conn, lock_id, timeout_ms, func_name)
                    return await func(*args, **kwargs)
                finally:
                    _release_db_lock(conn, lock_id, func_name)

        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name: str = func.__name__
            db_engine: Engine = create_engine(get_db_connection_string())
            with db_engine.connect() as conn:  # type: ignore
                try:
                    _obtain_db_lock(conn, lock_id, timeout_ms, func_name)
                    return func(*args, **kwargs)
                finally:
                    _release_db_lock(conn, lock_id, func_name)

        if asyncio.iscoroutinefunction(func):
            return cast(Callable[..., Awaitable[Any]], async_wrapper)
        else:
            return sync_wrapper

    return decorator
