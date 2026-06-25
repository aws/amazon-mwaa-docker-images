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
import logging
import logging.config
import os
import sys

from mwaa.config.database import get_db_connection_string
from mwaa.utils.db_retry import with_db_retry, MAINTENANCE_ENGINE_KWARGS
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
        @with_db_retry
        def _connect_static():
            engine = create_engine(
                get_db_connection_string(),
                **MAINTENANCE_ENGINE_KWARGS,
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine

        db_engine = _connect_static()
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

def _downgrade_fab_db():
    """
    Downgrade the FAB (Flask-AppBuilder) provider database migrations.

    When downgrading from Airflow 3.2.1 (providers-fab 3.6.1) to Airflow 3.0.6
    (providers-fab 2.4.1), the FAB provider's own migrations need to be reverted.
    This is equivalent to running: airflow fab-db downgrade --to-version 1.4.0

    The 3.5.0 migration (0001_3_5_0_fix_fab_db_inconsistencies.py) added NOT NULL
    constraints on ab_permission_view.permission_id and view_menu_id, among other
    changes. Reverting this ensures compatibility with providers-fab 2.4.1.
    """
    from airflow.providers.fab.auth_manager.cli_commands import db_command as fab_db_command

    try:
        logging.info("Downgrading FAB provider database to version 1.4.0...")
        fab_args = Namespace(
            to_version="1.4.0",
            to_revision=None,
            from_revision=None,
            from_version=None,
            show_sql_only=None,
            yes=True,
        )
        fab_db_command.downgrade(fab_args)
        logging.info("FAB provider database downgrade completed successfully.")
    except Exception as e:
        logger.error(f"Error while downgrading FAB provider database: {e}")
        raise


def _fix_fab_sequence_defaults():
    """
    Fix FAB (Flask-AppBuilder) security tables after downgrading from Airflow 3.2.1
    (providers-fab 3.6.1 / SQLAlchemy 2.0) to Airflow 3.0.6 (providers-fab 2.4.1 /
    SQLAlchemy 1.4).

    After the downgrade, the ab_* tables may be left
    without a DEFAULT on the 'id' column. This causes NOT NULL violations because
    SQLAlchemy 1.4's ORM omits the 'id' from INSERT statements, relying on the database
    to auto-generate it via a sequence default.

    This function checks these tables and adds the missing sequence + default
    if needed.
    """
    FAB_TABLES_TO_FIX = [
        "ab_permission",
        "ab_view_menu",
        "ab_permission_view",
        "ab_permission_view_role",
        "ab_role",
        "ab_user",
        "ab_user_role",
        "ab_register_user",
        "ab_group",
        "ab_user_group",
        "ab_group_role"
    ]

    try:
        db_engine = create_engine(
            get_db_connection_string(),
            connect_args={"connect_timeout": 3}
        )
        with db_engine.connect() as conn:
            with conn.begin():
                for table in FAB_TABLES_TO_FIX:
                    # Check if the table's id column is missing a default
                    result = conn.execute(
                        text(
                            "SELECT 1 "
                            "FROM pg_attribute a "
                            "JOIN pg_class c ON a.attrelid = c.oid "
                            "JOIN pg_namespace n ON c.relnamespace = n.oid "
                            "LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
                            "WHERE n.nspname = 'public' "
                            "AND c.relname = :table_name "
                            "AND a.attname = 'id' "
                            "AND c.relkind = 'r' "
                            "AND a.attnum > 0 "
                            "AND NOT a.attisdropped "
                            "AND a.attidentity = '' "
                            "AND d.adbin IS NULL"
                        ),
                        {"table_name": table},
                    )
                    if not result.fetchone():
                        logging.info(f"{table}.id already has a default, skipping.")
                        continue

                    seq_name = f"{table}_id_seq"
                    logging.info(
                        f"{table}.id is missing a default. Adding sequence default (seq: {seq_name})..."
                    )

                    # 1. Create the sequence if it doesn't exist
                    conn.execute(
                        text(f"CREATE SEQUENCE IF NOT EXISTS {seq_name} OWNED BY {table}.id")
                    )

                    # 2. Set the sequence value to max(id) + 1 to avoid conflicts
                    conn.execute(
                        text(
                            f"SELECT setval('{seq_name}', COALESCE(MAX(id), 0) + 1, false) "
                            f"FROM {table}"
                        )
                    )

                    # 3. Set the default on the id column
                    conn.execute(
                        text(
                            f"ALTER TABLE {table} ALTER COLUMN id "
                            f"SET DEFAULT nextval('{seq_name}')"
                        )
                    )

                    logging.info(f"Successfully added default nextval('{seq_name}') to {table}.id.")

        logging.info("FAB sequence defaults fix completed successfully.")
    except Exception as e:
        logger.error(f"Error while fixing FAB sequence defaults: {e}")
        raise


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

        if Version(target_version) in (Version("3.0.6"), Version("2.11.0")):
            logging.info(
                "Target version uses an older FAB provider, downgrading FAB provider database..."
            )
            _downgrade_fab_db()

            logging.info(
                "Target version uses FAB 4.x, ensuring ab_* tables have sequence defaults..."
            )
            _fix_fab_sequence_defaults()


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
