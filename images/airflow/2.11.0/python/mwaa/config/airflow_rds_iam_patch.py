"""RDS IAM authentication patch for Airflow database connections."""
import logging
import os
import sys

import sqlalchemy

logger = logging.getLogger(__name__)

logger.info("airflow_rds_iam_patch loaded")

try:
    # Airflow 2.4+ (SQLAlchemy ≥1.4)
    from sqlalchemy.engine import make_url  # type: ignore[attr-defined]
except ImportError:
    # Airflow ≤2.3 (SQLAlchemy 1.3)
    from sqlalchemy.engine.url import make_url  # type: ignore[attr-defined]

from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
from mwaa.config.database import get_db_connection_string


def is_from_migrate_db():
    """Check if running from migrate-db component."""
    MWAA_AIRFLOW_COMPONENT = os.environ.get('MWAA_AIRFLOW_COMPONENT')
    if MWAA_AIRFLOW_COMPONENT == None:
        logger.error("MWAA_AIRFLOW_COMPONENT does not exist as an environment variable.")
        return False
    return 'migrate-db' == MWAA_AIRFLOW_COMPONENT

def is_using_rds_proxy():
    """Check if using RDS proxy with SSL mode."""
    SSL_MODE = os.environ.get('SSL_MODE')
    if SSL_MODE is None:
        logger.error("SSL_MODE does not exist as an environment variable.")
        return False
    return 'require' == SSL_MODE

# Normalize the metadata DB URL once
metadata_url = make_url(get_db_connection_string())

def is_accessing_metadata_db(dialect, cargs, cparams):
    """Return True if the given SQLAlchemy URL points to the Airflow metadata DB."""

    # --- 1. Try detecting from cargs (dsn string) ---
    if cargs:
        try:
            url_obj = make_url(cargs[0])
            if (
                url_obj.host == metadata_url.host and
                url_obj.database == metadata_url.database and
                url_obj.port == metadata_url.port
            ):
                return True
        except Exception:
            pass

    # --- 2. Try detecting from cparams (kwargs dict) ---
    host = cparams.get('host')
    db = cparams.get('dbname') or cparams.get('database')
    port = cparams.get("port")

    # Compare essential components that uniquely identify the same DB
    return (
        host == metadata_url.host and
        db == metadata_url.database and
        port == metadata_url.port
    )


# Attaches a global listener to SQLAlchemy's Engine class
# But do not attach the MigrateDb processes

if is_using_rds_proxy() and not is_from_migrate_db():
    from sqlalchemy.engine import Engine
    from sqlalchemy import event

    def patch_rds_iam_authentication(dialect, conn_rec, cargs, cparams):
        """Patch database connection with RDS IAM authentication."""
        # Skip non-metadata DB connections
        if not is_accessing_metadata_db(dialect, cargs, cparams):
            return
        
        token = RDSIAMCredentialProvider.get_token()
        url = make_url(RDSIAMCredentialProvider.create_db_connection_url(token))

        # Update the connection parameters with the new token
        cparams.update(url.translate_connect_args(username='user'))
        cparams.update(url.query)

    event.listen(Engine, "do_connect", patch_rds_iam_authentication)