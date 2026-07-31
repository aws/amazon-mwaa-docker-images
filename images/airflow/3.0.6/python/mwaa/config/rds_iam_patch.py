"""RDS IAM authentication patch for SQLAlchemy connections.

Attaches a global listener to SQLAlchemy's Engine class that intercepts
metadata database connections and replaces credentials with fresh IAM tokens.

This module is feature-flag controlled by USE_IAM_CREDENTIALS=true,
only activates for RDS Proxy environments, and skips the migrate-db process.
"""

import logging
import os

from sqlalchemy.engine import make_url

from mwaa.config.rds_iam_credentials import (
    RDSIAMCredentialProvider,
    is_using_rds_proxy,
    use_iam_credentials,
)

logger = logging.getLogger(__name__)

MWAA_AIRFLOW_COMPONENT = os.environ.get("MWAA_AIRFLOW_COMPONENT")


def _is_from_migrate_db() -> bool:
    """Check if the current process is the migrate-db container."""
    if MWAA_AIRFLOW_COMPONENT is None:
        logger.debug("MWAA_AIRFLOW_COMPONENT not set in environment.")
        return False
    return MWAA_AIRFLOW_COMPONENT == "migrate-db"


def _get_metadata_url():
    """Get the parsed metadata DB URL from environment."""
    conn_str = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not conn_str:
        return None
    return make_url(conn_str)


def _is_accessing_metadata_db(dialect, cargs, cparams) -> bool:
    """Return True if the connection targets the Airflow metadata DB."""
    metadata_url = _get_metadata_url()
    if metadata_url is None:
        return False

    # Try detecting from cargs (dsn string)
    if cargs:
        try:
            url_obj = make_url(cargs[0])
            if (
                url_obj.host == metadata_url.host
                and url_obj.database == metadata_url.database
                and url_obj.port == metadata_url.port
                and url_obj.username in ["airflow_user", "adminuser"]
            ):
                return True
        except Exception:
            pass

    # Try detecting from cparams (kwargs dict)
    host = cparams.get("host")
    db = cparams.get("dbname") or cparams.get("database")
    port = cparams.get("port")
    username = cparams.get("username") or cparams.get("user")

    return (
        host == metadata_url.host
        and db == metadata_url.database
        and port == metadata_url.port
        and username in ["airflow_user", "adminuser"]
    )


def install_rds_iam_patch() -> None:
    """Install the RDS IAM authentication patch if conditions are met.

    Conditions:
    - USE_IAM_CREDENTIALS=true
    - Environment uses RDS Proxy (MWAA__DB__POSTGRES_SSLMODE=require)
    - Current process is NOT migrate-db
    """
    if not use_iam_credentials():
        logger.info("RDS IAM patch not installed: USE_IAM_CREDENTIALS is not true.")
        return

    if not is_using_rds_proxy():
        logger.info("RDS IAM patch not installed: not using RDS Proxy.")
        return

    if _is_from_migrate_db():
        logger.info("RDS IAM patch not installed: running in migrate-db process.")
        return

    from sqlalchemy import event
    from sqlalchemy.engine import Engine

    def patch_rds_iam_authentication(dialect, conn_rec, cargs, cparams):
        """SQLAlchemy do_connect event listener that injects IAM tokens."""
        if not _is_accessing_metadata_db(dialect, cargs, cparams):
            return

        token = RDSIAMCredentialProvider.get_token()
        url = make_url(RDSIAMCredentialProvider.create_db_connection_url(token))

        cparams.update(url.translate_connect_args(username="user"))
        cparams.update(url.query)

    event.listen(Engine, "do_connect", patch_rds_iam_authentication)
    logger.info("RDS IAM authentication patch installed successfully.")
