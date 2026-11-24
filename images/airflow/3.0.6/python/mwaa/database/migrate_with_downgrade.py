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
import json
import os
import sys
import time
from datetime import datetime, timezone

from mwaa.config.database import get_db_connection_string
from mwaa.config.rds_iam_credentials import (
    RDSIAMCredentialProvider,
    is_using_rds_proxy,
    use_iam_credentials,
)
from mwaa.utils.db_retry import with_db_retry, MAINTENANCE_ENGINE_KWARGS
from mwaa.utils.dblock import with_db_lock
from mwaa.utils.plogs import generate_plog
from airflow.cli.commands import db_command as airflow_db_command

DB_IAM_USERNAME = "airflow_user"
DB_ADMIN_USERNAME = "adminuser"
DB_NAME = "AirflowMetadata"

# Table used to detect missing planner statistics after a major version upgrade.
STATS_PROBE_TABLE = "task_instance"
# Upper bound for ANALYZE so it cannot hold migrate-db past its own timeout.
ANALYZE_STATEMENT_TIMEOUT_MS = 300_000

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

    except Exception as e:
        logger.warning(f"Static credential connection failed: {e}")

        if use_iam_credentials() and is_using_rds_proxy():
            @with_db_retry
            def _connect_iam():
                logger.info("Attempting connection with RDS IAM credentials.")
                token = RDSIAMCredentialProvider.get_token()
                url = RDSIAMCredentialProvider.create_db_connection_url(token)
                engine = create_engine(url, **MAINTENANCE_ENGINE_KWARGS)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("RDS IAM connection successful.")
                return engine

            try:
                db_engine = _connect_iam()
            except Exception as iam_error:
                # Deliberate hard failure, unlike the best-effort paths in this
                # function. Reaching here means neither the static credentials
                # nor an RDS IAM token can open a connection, so the grants
                # below cannot be applied and migrate-db must not continue.
                # Raising explicitly, rather than letting _connect_iam()
                # propagate on its own, keeps the intent unambiguous and puts a
                # single precise error in the logs instead of surfacing later as
                # a lock-acquisition failure in _migrate_db().
                raise RuntimeError(
                    "Could not connect to the metadata database with either "
                    "static or RDS IAM credentials; unable to ensure the RDS "
                    "IAM user."
                ) from iam_error
        else:
            logger.warning(
                "Error while ensuring rds iam db credentials, skipping. %s", e
            )
            return

    try:
        with db_engine.connect() as conn:
            with conn.begin():
                result = conn.execute(
                    text("SELECT 1 FROM pg_roles WHERE rolname = :rolename"),
                    {"rolename": DB_IAM_USERNAME},
                )
                if not result.fetchone():
                    logger.info(f"Creating user '{DB_IAM_USERNAME}'")
                    conn.execute(text(f"CREATE USER {DB_IAM_USERNAME}"))
                    logger.info("Created db rds iam user")
                else:
                    logger.info("db rds iam user already exists")

                # Only the admin role can hand out these privileges. When this
                # runs as airflow_user the grants would fail, so mirror the 2.x
                # behaviour and gate them on the current role.
                current_role = conn.execute(text("SELECT current_user")).scalar()

                if current_role == DB_ADMIN_USERNAME:
                    logger.info(
                        "Current role is %s, setting up permissions for %s",
                        DB_ADMIN_USERNAME,
                        DB_IAM_USERNAME,
                    )
                    conn.execute(text(f"GRANT rds_iam TO {DB_IAM_USERNAME}"))
                    conn.execute(
                        text(
                            f'GRANT ALL PRIVILEGES ON DATABASE "{DB_NAME}" TO {DB_IAM_USERNAME}'
                        )
                    )
                    conn.execute(
                        text(f"GRANT ALL ON SCHEMA public TO {DB_IAM_USERNAME}")
                    )
                    conn.execute(
                        text(
                            f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {DB_IAM_USERNAME}"
                        )
                    )
                    conn.execute(
                        text(
                            f"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {DB_IAM_USERNAME}"
                        )
                    )
                    conn.execute(
                        text(
                            f"GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO {DB_IAM_USERNAME}"
                        )
                    )
                    conn.execute(
                        text(
                            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                            f"GRANT ALL ON TABLES TO {DB_IAM_USERNAME}"
                        )
                    )
                    conn.execute(
                        text(
                            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                            f"GRANT ALL ON SEQUENCES TO {DB_IAM_USERNAME}"
                        )
                    )
                    conn.execute(
                        text(
                            f"ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                            f"GRANT ALL ON FUNCTIONS TO {DB_IAM_USERNAME}"
                        )
                    )
                    # Needed so that airflow_user inherits ownership of objects
                    # created by adminuser during migrations.
                    conn.execute(
                        text(f"GRANT {DB_ADMIN_USERNAME} TO {DB_IAM_USERNAME}")
                    )
                elif current_role == DB_IAM_USERNAME:
                    logger.info("Current role is %s", DB_IAM_USERNAME)
    except Exception as e:
        logger.warning("Error while ensuring rds iam db credentials, skipping. %s", e)


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


def _refresh_planner_statistics():
    """
    Run ANALYZE on the metadata database when its planner statistics are missing.

    pg_upgrade does not carry planner statistics over, so after an Aurora PostgreSQL
    major version upgrade every table looks "never analyzed" until autovacuum gets to
    it, which can take days. Without statistics the planner turns the triggerer's
    `UPDATE task_instance ... WHERE trigger_id IS NOT NULL` into a full table scan
    and pins the writer's CPU. AWS lists ANALYZE as the post-upgrade step to run.

    migrate-db runs after the upgrade and before the Airflow components start, so this
    is the first point where the statistics can be rebuilt. `task_instance` is the
    probe: it only runs ANALYZE when that table has rows but no statistics, so on any
    other run (including every non-upgrade stack update) it costs two small queries.

    Best-effort: a failure here leaves the environment as it is today, so it is logged
    and counted but never fails migrate-db.
    """
    try:
        engine = create_engine(get_db_connection_string(), **MAINTENANCE_ENGINE_KWARGS)
        # ANALYZE writes pg_statistic transactionally; run outside a transaction so a
        # rollback on close cannot discard it.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            if not _stats_missing(conn):
                logger.info("Planner statistics present, skipping ANALYZE.")
                return

            logger.info(
                "No planner statistics for %s, running ANALYZE (timeout %d ms).",
                STATS_PROBE_TABLE,
                ANALYZE_STATEMENT_TIMEOUT_MS,
            )
            conn.execute(text(f"SET statement_timeout = {ANALYZE_STATEMENT_TIMEOUT_MS}"))
            started = time.monotonic()
            conn.execute(text("ANALYZE"))
            elapsed = time.monotonic() - started

            # ANALYZE skips tables the current user does not own with only a WARNING,
            # so check that it actually produced statistics.
            if _stats_missing(conn):
                logger.warning(
                    "ANALYZE finished in %.1f s but %s still has no statistics.",
                    elapsed,
                    STATS_PROBE_TABLE,
                )
                _report_migrate_db_metric("PlannerStatisticsRefreshError")
                return

            logger.info("ANALYZE rebuilt planner statistics in %.1f s.", elapsed)
            _report_migrate_db_metric("PlannerStatisticsRefreshed")
    except Exception as e:
        logger.warning("Error while refreshing planner statistics, skipping. %s", e)
        _report_migrate_db_metric("PlannerStatisticsRefreshError")


def _stats_missing(conn) -> bool:
    """True when the probe table has rows but no planner statistics."""
    has_stats = conn.execute(
        text(
            "SELECT 1 FROM pg_stats "
            "WHERE schemaname = 'public' AND tablename = :table_name LIMIT 1"
        ),
        {"table_name": STATS_PROBE_TABLE},
    ).fetchone()
    if has_stats:
        return False
    # An empty table never gets pg_stats rows, even after ANALYZE.
    has_rows = conn.execute(
        text(f"SELECT 1 FROM {STATS_PROBE_TABLE} LIMIT 1")
    ).fetchone()
    return has_rows is not None


def _report_migrate_db_metric(metric_name: str):
    # migrate-db has no sidecar for StatsD, so print the metric in the sidecar's plog format.
    # StatisticValues, not Value, is what the service's processor reads.
    metric_data = [
        {
            "MetricName": metric_name,
            "Dimensions": [{"Name": "AirflowComponent", "Value": "MigrateDb"}],
            "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "StatisticValues": {"SampleCount": 1, "Sum": 1, "Minimum": 1, "Maximum": 1},
            "Unit": "Count",
        }
    ]
    # Unlike normal logs, plogs are ingested by the service, so 'print' them.
    print(
        generate_plog(
            "AirflowMetricsLogsProcessor", f"Metric data: {json.dumps(metric_data)}"
        )
    )


def _main():
    _verify_environ()
    _ensure_rds_iam_user()
    _migrate_db()
    _refresh_planner_statistics()


if __name__ == "__main__":
    _main()
else:
    logger.error(
        "This module cannot be imported. It should be run directly using: python -m mwaa.database.migrate_with_downgrade"
    )
    sys.exit(1)
