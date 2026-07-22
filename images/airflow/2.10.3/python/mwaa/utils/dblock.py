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
from sqlalchemy.engine import Connection

# Our imports
from mwaa.config.database import get_db_connection_string
from mwaa.utils.db_retry import with_db_retry, MAINTENANCE_ENGINE_KWARGS

logger = logging.getLogger(__name__)


F = TypeVar("F", bound=Callable[..., Any])


@with_db_retry
def _connect_with_retry() -> Connection:
    """Establish a DB connection with retry on transient RDS Proxy failures."""
    from mwaa.config.airflow_rds_iam_patch import is_using_rds_proxy, use_iam_credentials
    if use_iam_credentials() and is_using_rds_proxy():
        from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
        token = RDSIAMCredentialProvider.get_token()
        conn_url = RDSIAMCredentialProvider.create_db_connection_url(token)
    else:
        conn_url = get_db_connection_string()
    engine = create_engine(conn_url, **MAINTENANCE_ENGINE_KWARGS)
    return engine.connect()


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
            conn = _connect_with_retry()
            try:
                _obtain_db_lock(conn, lock_id, timeout_ms, func_name)
                return await func(*args, **kwargs)
            finally:
                try:
                    _release_db_lock(conn, lock_id, func_name)
                finally:
                    conn.close()

        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name: str = func.__name__
            conn = _connect_with_retry()
            try:
                _obtain_db_lock(conn, lock_id, timeout_ms, func_name)
                return func(*args, **kwargs)
            finally:
                try:
                    _release_db_lock(conn, lock_id, func_name)
                finally:
                    conn.close()

        if asyncio.iscoroutinefunction(func):
            return cast(Callable[..., Awaitable[Any]], async_wrapper)
        else:
            return sync_wrapper

    return decorator
