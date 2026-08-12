"""Database connection retry utilities for MWAA maintenance workflows."""

import logging
import os

from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.pool import NullPool
from tenacity import (
    after_log,
    before_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = int(os.environ.get("MWAA_DB_RETRY_ATTEMPTS", "7"))
_RETRY_MIN_WAIT = int(os.environ.get("MWAA_DB_RETRY_MIN_WAIT", "2"))
_RETRY_MAX_WAIT = int(os.environ.get("MWAA_DB_RETRY_MAX_WAIT", "60"))

MAINTENANCE_ENGINE_KWARGS = {
    "poolclass": NullPool,
    "connect_args": {"connect_timeout": 3},
}

with_db_retry = retry(
    stop=stop_after_attempt(_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=_RETRY_MIN_WAIT, max=_RETRY_MAX_WAIT),
    retry=retry_if_exception_type((OperationalError, InterfaceError)),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
    reraise=True,
)
