"""
Delete Airflow teams and the resources they own. Run by ``mwaa.database.manage_teams``
during ``migrate-db``::

    python3 -m mwaa.database.delete_teams <team> [<team> ...]

In a single transaction, for each team:

* Fail the unfinished task instances in the team's pools. Otherwise they would stay
  ``scheduled`` forever once the pool is gone.
* Delete the team's connections, variables and pools.
* Delete the team. Its Dag bundle associations are removed by ``ON DELETE CASCADE``;
  the bundles themselves are kept.

Unlike ``airflow teams delete``, this doesn't require resources to be removed first:
the explicit removal list at the API level already guards against accidental deletion.

The database is modified through the Airflow models because the CLI can't do this:
connection and variable listings don't show the owning team, ``airflow variables
delete`` silently skips team-owned variables, ``airflow teams delete`` is blocked by
bundle associations until the Dag processor syncs, and no command fails task
instances. Airflow's REST API deletes the same way. Failed task instances don't
trigger failure callbacks, listeners or emails.

IMPORTANT NOTE: This script must be run with all the required environments exported,
just like when running any Airflow command, as it imports Airflow modules and needs to
connect to the meta database, thus all configurations need to be set.
"""

from datetime import datetime, timezone
import logging
import sys

from sqlalchemy import delete, select, update

from airflow.models.connection import Connection
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.variable import Variable
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

# Usually, we pass the `__name__` variable instead as that defaults to the module path,
# i.e. `mwaa.database.delete_teams` in this case. However, since this is a script,
# `__name__` will have the value of `__main__`, hence we hard-code the module path.
logger = logging.getLogger("mwaa.database.delete_teams")

# Unfinished states that are failed, along with no state. Running task instances are
# left to finish; the heartbeat timeout covers them if they stop.
UNFINISHED_TI_STATES = [
    TaskInstanceState.SCHEDULED,
    TaskInstanceState.QUEUED,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.DEFERRED,
]


def _fail_unfinished_task_instances(pools: list[str], session) -> None:
    """Mark unfinished task instances that use any of ``pools`` as failed."""
    if not pools:
        return
    state_filter = TaskInstance.state.in_(UNFINISHED_TI_STATES) | TaskInstance.state.is_(None)
    task_instances = session.execute(
        select(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.run_id,
            TaskInstance.map_index,
            TaskInstance.pool,
        ).where(TaskInstance.pool.in_(pools)).where(state_filter)
    ).all()
    for ti in task_instances:
        logger.info(
            "Failing task instance dag_id=%s task_id=%s run_id=%s map_index=%s "
            "(pool '%s' is being deleted).",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            ti.map_index,
            ti.pool,
        )
    session.execute(
        update(TaskInstance.__table__)
        .where(TaskInstance.pool.in_(pools))
        .where(state_filter)
        .values(state=TaskInstanceState.FAILED, end_date=datetime.now(timezone.utc))
        .execution_options(synchronize_session=False)
    )


def _delete_team(team: str, session) -> None:
    """Delete ``team`` and the resources it owns, within ``session``."""
    if not session.scalar(select(Team.name).where(Team.name == team)):
        logger.info("Team '%s' does not exist. Skipping deletion.", team)
        return

    pools = list(session.scalars(select(Pool.pool).where(Pool.team_name == team)))
    _fail_unfinished_task_instances(pools, session)

    for model, name_column, kind in (
        (Connection, Connection.conn_id, "connection"),
        (Variable, Variable.key, "variable"),
        (Pool, Pool.pool, "pool"),
    ):
        # Log names only: values can hold secrets.
        for name in session.scalars(select(name_column).where(model.team_name == team)):
            logger.info("Deleting %s '%s' owned by team '%s'.", kind, name, team)
        session.execute(
            delete(model)
            .where(model.team_name == team)
            .execution_options(synchronize_session=False)
        )

    logger.info("Deleting team '%s'.", team)
    session.execute(delete(Team.__table__).where(Team.name == team))


def _main():
    teams = sys.argv[1:]
    if not teams:
        logger.error("No team names were provided.")
        sys.exit(1)

    # One transaction: on any failure nothing is deleted, and migrate-db fails.
    with create_session() as session:
        for team in teams:
            _delete_team(team, session)


if __name__ == "__main__":
    _main()
else:
    logger.error(
        "This module cannot be imported. It should be run directly using: "
        "python -m mwaa.database.delete_teams"
    )
    sys.exit(1)
