"""
This script is responsible for running Airflow meta database migrations. This will replace
the migrate script.

IMPORTANT NOTE: This script must be run with all the required environments exported,
just like when running any Airflow command, as it imports Airflow modules and needs to
connect to the meta database, thus all configurations need to be set.
"""

from argparse import Namespace
from packaging.version import Version
from sqlalchemy import create_engine, text
import logging.config
import os
import sys

from mwaa.config.database import get_db_connection_string
from mwaa.utils.dblock import with_db_lock
from airflow.cli.commands import db_command as airflow_db_command

DB_IAM_USERNAME = "airflow_user"
DB_NAME = "AirflowMetadata"

# Usually, we pass the `__name__` variable instead as that defaults to the module path,
# i.e. `mwaa.entrypoint` in this case. However, since this is a script, `__name__` will
# have the value of `__main__`, hence we hard-code the module path.
logger = logging.getLogger("mwaa.database.migrate_with_downgrade")


def _verify_environ():
    """
    This script is supposed to have all the environment variables required for running
    Airflow, since we will be using Airflow modules directly. This function verifies
    they are set by ensuring the existence of the `AWS_EXECUTION_ENV`, which we add
    during the creation of the `environ` dictionary in the entrypoint.py.
    """
    if not os.environ.get("AWS_EXECUTION_ENV", "").startswith("Amazon_MWAA_"):
        logger.error("The necessary environment variables are not set.")
        sys.exit(1)

def _ensure_rds_iam_user():
    try:
        db_engine = create_engine(
            get_db_connection_string(),
            connect_args={"connect_timeout": 3}
        )
        with db_engine.connect() as conn:
            with conn.begin():
                result = conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :rolename"), {"rolename": DB_IAM_USERNAME})
                if not result.fetchone():
                    print(f"Creating user '{DB_IAM_USERNAME}'")
                    conn.execute(text(f"CREATE USER {DB_IAM_USERNAME}"))
                    print(f"Created db rds iam user")
                else:
                    print(f"db rds iam user already exists")

                # Always ensure permissions are up to date
                conn.execute(text(f"GRANT rds_iam TO {DB_IAM_USERNAME}"))
                conn.execute(text(f'GRANT ALL PRIVILEGES ON DATABASE "{DB_NAME}" TO {DB_IAM_USERNAME}'))
                conn.execute(text(f"GRANT ALL ON SCHEMA public TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {DB_IAM_USERNAME}"))
                conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO {DB_IAM_USERNAME}"))
    except Exception as e:
        logger.warning(f"Error while ensuring rds iam db credentials, skipping. {e}")


@with_db_lock(1234)
def _migrate_db():
    try:
        args = Namespace(migration_wait_timeout=1)
        airflow_db_command.check_migrations(args)
        logging.info("The database is migrated to the current version.")
        _check_downgrade_db()
    except TimeoutError:
        logging.info("The database is not yet migrated. Migrating...")
        args = Namespace(
            from_revision=None,
            from_version=None,
            reserialize_dags=False,
            show_sql_only=None,
            to_revision=None,
            to_version=None,
            use_migration_files=None,
        )
        airflow_db_command.migratedb(args)
        logging.info("The database is now migrated.")

def _check_downgrade_db():
    target_version = os.environ.get("MWAA__DB__AIRFLOW_TARGET_VERSION", None)
    current_version = os.environ.get("AIRFLOW_VERSION", None)
    if target_version and current_version and Version(target_version) < Version(current_version):
        logging.info(f"Downgrading the database to {target_version}. Downgrading...")
        args = Namespace(
                from_revision=None,
                from_version=None,
                reserialize_dags=False,
                show_sql_only=None,
                to_revision=None,
                to_version=target_version,
                use_migration_files=None,
                yes=True,
            )
        airflow_db_command.downgrade(args)


def _main():
    _verify_environ()
    _ensure_rds_iam_user()
    _migrate_db()


if __name__ == "__main__":
    _main()
else:
    logger.error(
        "This module cannot be imported. It should be run directly using: python -m mwaa.database.migrate_with_downgrade"
    )
    sys.exit(1)
