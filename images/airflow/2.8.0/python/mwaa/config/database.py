import os
import json
from operator import itemgetter
from typing import Tuple


def get_db_credentials() -> Tuple[str, str]:
    """
    Retrieves database credentials from environment variables.

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

    Returns:
        Tuple[str, str]: A tuple containing the PostgreSQL username and password.

    Raises:
        RuntimeError: If neither MWAA__DB__CREDENTIALS nor MWAA__DB__POSTGRES_USER and
        MWAA__DB__POSTGRES_PASSWORD environment variables are set, indicating that the
        database credentials are not provided.

    Example:
        To use this function, ensure that the required environment variables are set in
        your environment before calling it. Then, you can retrieve the credentials as
        follows:

        >>> user, password = get_db_credentials()
        >>> print(f"Username: {user}, Password: {password}")
    """

    if "MWAA__DB__CREDENTIALS" in os.environ:
        print("Reading database credentilas from MWAA__DB__CREDENTIALS.")
        db_secrets = json.loads(os.environ["MWAA__DB__CREDENTIALS"])
        postgres_user = db_secrets["username"]
        postgres_password = db_secrets["password"]
    elif (
        "MWAA__DB__POSTGRES_USER" in os.environ
        and "MWAA__DB__POSTGRES_PASSWORD" in os.environ
    ):
        print(
            "Reading database credentilas from MWAA__DB__POSTGRES_USER/ "
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
    Retrieves the connection string to use for communicating with metadata
    database.
    """

    env_vars_names = [
        "MWAA__DB__POSTGRES_HOST",
        "MWAA__DB__POSTGRES_PORT",
        "MWAA__DB__POSTGRES_DB",
    ]
    try:
        (
            postgres_host,
            postgres_port,
            postgres_db,
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

    protocol = "postgresql+psycopg2"
    creds = f"{postgres_user}:{postgres_password}"
    addr = f"{postgres_host}:{postgres_port}"
    # TODO We need to do what is the necessary to enforce 'require'.
    return f"{protocol}://{creds}@{addr}/{postgres_db}?sslmode=prefer"
