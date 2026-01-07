"""Contain functions for retrieving Airflow database-related configuration."""

import json
import logging
import os
from operator import itemgetter
from typing import Tuple


logger = logging.getLogger(__name__)


def get_db_credentials() -> Tuple[str, str]:
    """
    Retrieve database credentials from environment variables.

    This function looks for database credentials in two possible locations within the
    environment variables:
    1. MWAA__DB__CREDENTIALS: expects a JSON string containing "username" and "password"
       keys.
    2. MWAA__DB__POSTGRES_USER and MWAA__DB__POSTGRES_PASSWORD: separate environment
       variables for the username and password.

    The function first checks for the presence of "MWAA__DB__CREDENTIALS". If found, it
    parses the JSON string to extract the username and password. If not found, it then
    looks for the "MWAA__DB__POSTGRES_USER" and "MWAA__DB__POSTGRES_PASSWORD"
    environment variables.

    If neither method finds the credentials, a RuntimeError is raised indicating the
    absence of necessary environment variables for database connection.

    :returns Tuple[str, str]: A tuple containing the PostgreSQL username and password.

    :raises RuntimeError If neither MWAA__DB__CREDENTIALS nor MWAA__DB__POSTGRES_USER
    and MWAA__DB__POSTGRES_PASSWORD environment variables are set, indicating that the
    database credentials are not provided.
    """
    if "MWAA__DB__CREDENTIALS" in os.environ:
        logger.info("Reading database credentials from MWAA__DB__CREDENTIALS.")
        db_secrets = json.loads(os.environ["MWAA__DB__CREDENTIALS"])
        postgres_user = db_secrets["username"]
        postgres_password = db_secrets["password"]
    elif (
        "MWAA__DB__POSTGRES_USER" in os.environ
        and "MWAA__DB__POSTGRES_PASSWORD" in os.environ
    ):
        logger.info(
            "Reading database credentials from MWAA__DB__POSTGRES_USER/"
            "MWAA__DB__POSTGRES_USER environment variables."
        )
        postgres_user = os.environ["MWAA__DB__POSTGRES_USER"]
        postgres_password = os.environ["MWAA__DB__POSTGRES_PASSWORD"]
    else:
        raise RuntimeError(
            "Couldn't find database credentials in environment variables. "
            "Please pass them either in MWAA__DB__CREDENTIALS as a JSON with "
            "'username' and 'password' fields, or in MWAA__DB__POSTGRES_USER "
            "and MWAA__DB__POSTGRES_PASSWORD."
        )
    return postgres_user, postgres_password


def get_db_connection_string() -> str:
    """
    Retrieve the connection string for communicating with metadata database.

    :returns The connection string.

    :raises RuntimeError if the required environment variables are not set.
    """
    env_vars_names = [
        "MWAA__DB__POSTGRES_HOST",
        "MWAA__DB__POSTGRES_PORT",
        "MWAA__DB__POSTGRES_DB",
        "MWAA__DB__POSTGRES_SSLMODE",
    ]
    try:
        (
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_sslmode,
        ) = itemgetter(*env_vars_names)(os.environ)
        (postgres_user, postgres_password) = get_db_credentials()
    except Exception as e:
        raise RuntimeError(
            "One or more of the required environment variables for "
            "configuring Postgres are not set. Please ensure you set all "
            "all the following environment variables: "
            f'{", ".join(env_vars_names)}. This was the result of the '
            f"following exception: {e}"
        )

    if not postgres_sslmode:
        postgres_sslmode = "require"

    protocol = "postgresql+psycopg2"
    creds = f"{postgres_user}:{postgres_password}"
    addr = f"{postgres_host}:{postgres_port}"
    return f"{protocol}://{creds}@{addr}/{postgres_db}?sslmode={postgres_sslmode}"

# Per recommendation from https://airflow.apache.org/docs/apache-airflow/3.0.6/howto/set-up-database.html,
# since we are using Amazon RDS
MWAA_CONNECT_ARGS = {
    "connect_timeout": 15,
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}
