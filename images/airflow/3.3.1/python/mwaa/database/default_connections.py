"""
Connections MWAA creates in the Airflow metadata database.

Unlike ``aws_default``, which is an environment variable, these are rows in the metadata database, so
they appear in Admin -> Connections and can be edited or deleted there. An environment
variable would silently take precedence over a connection of the same id created in the UI.

Each connection is created only if no connection with its id exists, so an existing connection is
never overwritten.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# DuckDB's default connection, typed so that DuckDB gets S3 access as the execution role.
#
# ``duckdb_default`` is the connection DuckDBExecuteQueryOperator uses when a task gives no
# ``conn_id``. Its type, ``duckdb_aws`` (the Amazon provider, apache/airflow#73301), makes the
# operator use AwsDuckDBHook, which loads the httpfs and aws extensions and creates a
# ``credential_chain`` S3 secret. The image installs those extensions for its default DuckDB version
# (bootstrap/02-airflow/002-install-duckdb-extensions.sh), and the hook downloads them for any other
# version, so the connection needs no extras. It has no host, so DuckDB stays in memory.
DUCKDB_DEFAULT: dict[str, Any] = {
    "conn_id": "duckdb_default",
    "conn_type": "duckdb_aws",
    "description": (
        "Created by Amazon MWAA. DuckDB with S3 access as the environment's execution role. "
        "Edit or delete it to change what DuckDB tasks without a conn_id use."
    ),
}

DEFAULT_CONNECTIONS: list[dict[str, Any]] = [DUCKDB_DEFAULT]


def create_default_connections() -> None:
    """Create each default connection whose id is not in the metadata database yet."""
    from airflow.models import Connection
    from airflow.utils.session import create_session

    with create_session() as session:
        for fields in DEFAULT_CONNECTIONS:
            conn_id = fields["conn_id"]
            if session.query(Connection).filter(Connection.conn_id == conn_id).first():
                logger.info(
                    "Connection %s already exists; leaving it unchanged.", conn_id
                )
                continue
            session.add(Connection(**fields))
            logger.info("Created connection %s.", conn_id)
