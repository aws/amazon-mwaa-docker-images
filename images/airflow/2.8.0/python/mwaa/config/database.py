import os
from operator import itemgetter


def get_db_connection_string() -> str:
    """
    Retrieves the connection string to use for communicating with metadata
    database.
    """

    env_vars_names = [
        "MWAA__DB__POSTGRES_HOST",
        "MWAA__DB__POSTGRES_PORT",
        "MWAA__DB__POSTGRES_USER",
        "MWAA__DB__POSTGRES_PASSWORD",
        "MWAA__DB__POSTGRES_DB",
    ]
    try:
        (
            postgres_host,
            postgres_port,
            postgres_user,
            postgres_password,
            postgres_db,
        ) = itemgetter(*env_vars_names)(os.environ)
    except Exception as e:
        raise RuntimeError(
            'One or more of the required environment variables for ' +
            'configuring Postgres are not set. Please ensure you set all ' +
            'all the following environment variables: ' +
            f'{", ".join(env_vars_names)}. This was the result of the ' +
            f'following exception: {e}')

    protocol = "postgresql+psycopg2"
    creds = f"{postgres_user}:{postgres_password}"
    addr = f"{postgres_host}:{postgres_port}"
    # TODO We need to do what is the necessary to enforce 'require'.
    return f'{protocol}://{creds}@{addr}/{postgres_db}?sslmode=prefer'
