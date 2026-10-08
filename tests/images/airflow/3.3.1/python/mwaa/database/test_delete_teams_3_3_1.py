"""Tests for mwaa.database.delete_teams.

delete_teams.py exits if imported, so the module body is executed up to that guard.
Deletions run against the real Airflow schema in in-memory SQLite, with foreign
keys enforced so ``ON DELETE`` behavior is exercised.
"""

import contextlib
import os
import types
import uuid

import pytest
from unittest.mock import patch
from sqlalchemy import create_engine, func, insert, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from airflow.utils.db import import_all_models

import_all_models()

from airflow.models.base import Base  # noqa: E402
from airflow.models.connection import Connection  # noqa: E402
from airflow.models.dagbundle import DagBundleModel  # noqa: E402
from airflow.models.pool import Pool  # noqa: E402
from airflow.models.taskinstance import TaskInstance  # noqa: E402
from airflow.models.team import Team, dag_bundle_team_association_table  # noqa: E402
from airflow.models.variable import Variable  # noqa: E402
from airflow.utils.state import TaskInstanceState  # noqa: E402

_AIRFLOW_VERSION = "3.3.1"


def _load_module():
    """Execute delete_teams.py up to its no-import guard."""
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..", "..",
            "images", "airflow", _AIRFLOW_VERSION,
            "python", "mwaa", "database", "delete_teams.py",
        )
    )
    assert os.path.exists(path), f"missing module: {path}"
    source = open(path).read()
    guard = source.index('if __name__ == "__main__":')
    module = types.ModuleType("mwaa_delete_teams_probe")
    module.__file__ = path
    exec(compile(source[:guard], path, "exec"), module.__dict__)
    return module


@pytest.fixture
def module():
    """The delete_teams module, loaded up to its no-import guard."""
    return _load_module()


@pytest.fixture
def session():
    """A session on an in-memory SQLite database with the Airflow schema."""
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    _seed(session)
    # Enforce foreign keys after seeding, so task instances don't need Dag runs.
    session.commit()
    session.connection().exec_driver_sql("PRAGMA foreign_keys=ON")
    yield session
    session.close()
    engine.dispose()


def _seed(session):
    """Create team_a (with resources), team_b (with resources), and global resources."""
    session.execute(insert(Team), [{"name": "team_a"}, {"name": "team_b"}])
    session.execute(
        insert(Connection.__table__),
        [
            {"conn_id": "conn_a", "conn_type": "http", "team_name": "team_a"},
            {"conn_id": "conn_b", "conn_type": "http", "team_name": "team_b"},
            {"conn_id": "conn_global", "conn_type": "http", "team_name": None},
        ],
    )
    session.execute(
        insert(Variable.__table__),
        [
            {"key": "var_a", "val": "secret", "team_name": "team_a"},
            {"key": "var_b", "val": "secret", "team_name": "team_b"},
            {"key": "var_global", "val": "value", "team_name": None},
        ],
    )
    session.execute(
        insert(Pool.__table__),
        [
            {"pool": "default_pool", "slots": 128, "include_deferred": False, "team_name": None},
            {"pool": "pool_a", "slots": 1, "include_deferred": False, "team_name": "team_a"},
            {"pool": "pool_b", "slots": 1, "include_deferred": False, "team_name": "team_b"},
        ],
    )
    session.execute(
        insert(DagBundleModel.__table__),
        [{"name": "bundle_a", "active": True}],
    )
    session.execute(
        insert(dag_bundle_team_association_table),
        [{"dag_bundle_name": "bundle_a", "team_name": "team_a"}],
    )
    tis = [
        # (task_id, pool, state)
        ("ti_a_none", "pool_a", None),
        ("ti_a_scheduled", "pool_a", TaskInstanceState.SCHEDULED),
        ("ti_a_queued", "pool_a", TaskInstanceState.QUEUED),
        ("ti_a_retry", "pool_a", TaskInstanceState.UP_FOR_RETRY),
        ("ti_a_reschedule", "pool_a", TaskInstanceState.UP_FOR_RESCHEDULE),
        ("ti_a_deferred", "pool_a", TaskInstanceState.DEFERRED),
        ("ti_a_running", "pool_a", TaskInstanceState.RUNNING),
        ("ti_a_success", "pool_a", TaskInstanceState.SUCCESS),
        ("ti_b_scheduled", "pool_b", TaskInstanceState.SCHEDULED),
        ("ti_default_scheduled", "default_pool", TaskInstanceState.SCHEDULED),
    ]
    session.execute(
        insert(TaskInstance.__table__),
        [
            {
                "id": uuid.uuid4(),
                "dag_id": "dag",
                "run_id": "run",
                "task_id": task_id,
                "map_index": -1,
                "pool": pool,
                "state": state,
            }
            for task_id, pool, state in tis
        ],
    )


def _names(session, column, where=None):
    stmt = select(column)
    if where is not None:
        stmt = stmt.where(where)
    return set(session.scalars(stmt))


def _ti_states(session):
    return dict(session.execute(select(TaskInstance.task_id, TaskInstance.state)).all())


@contextlib.contextmanager
def _use_session(module, session):
    """Make the module's create_session yield ``session`` with the same semantics."""
    @contextlib.contextmanager
    def fake_create_session():
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise

    with patch.object(module, "create_session", fake_create_session):
        yield


def test_deletes_team_and_owned_resources(module, session):
    """The team and its connections, variables and pools are deleted."""
    module._delete_team("team_a", session)
    session.commit()

    assert _names(session, Team.name) == {"team_b"}
    assert _names(session, Connection.conn_id) == {"conn_b", "conn_global"}
    assert _names(session, Variable.key) == {"var_b", "var_global"}
    assert _names(session, Pool.pool) == {"default_pool", "pool_b"}


def test_removes_bundle_association_but_keeps_bundle(module, session):
    """The Dag bundle association is removed by ON DELETE CASCADE; the bundle stays."""
    module._delete_team("team_a", session)
    session.commit()

    assert session.scalar(
        select(func.count()).select_from(dag_bundle_team_association_table)
    ) == 0
    assert _names(session, DagBundleModel.name) == {"bundle_a"}


def test_fails_unfinished_task_instances_in_team_pools(module, session):
    """Unfinished task instances in the team's pools are failed; others are untouched."""
    module._delete_team("team_a", session)
    session.commit()

    states = _ti_states(session)
    for task_id in (
        "ti_a_none",
        "ti_a_scheduled",
        "ti_a_queued",
        "ti_a_retry",
        "ti_a_reschedule",
        "ti_a_deferred",
    ):
        assert states[task_id] == TaskInstanceState.FAILED, task_id
    # Running task instances finish normally; finished ones are left as they are.
    assert states["ti_a_running"] == TaskInstanceState.RUNNING
    assert states["ti_a_success"] == TaskInstanceState.SUCCESS
    # Task instances in other teams' pools and global pools are untouched.
    assert states["ti_b_scheduled"] == TaskInstanceState.SCHEDULED
    assert states["ti_default_scheduled"] == TaskInstanceState.SCHEDULED


def test_failed_task_instances_get_end_date(module, session):
    """Failed task instances get an end date, like any other failed task instance."""
    module._delete_team("team_a", session)
    session.commit()

    end_date = session.scalar(
        select(TaskInstance.end_date).where(TaskInstance.task_id == "ti_a_scheduled")
    )
    assert end_date is not None


def test_missing_team_is_skipped(module, session):
    """A team that does not exist is skipped without touching anything."""
    module._delete_team("team_missing", session)
    session.commit()

    assert _names(session, Team.name) == {"team_a", "team_b"}
    assert _ti_states(session)["ti_a_scheduled"] == TaskInstanceState.SCHEDULED


def test_never_logs_secret_values(module, session, caplog):
    """Only resource names are logged, never connection or variable values."""
    with caplog.at_level("INFO", logger="mwaa.database.delete_teams"):
        module._delete_team("team_a", session)

    assert "var_a" in caplog.text
    assert "conn_a" in caplog.text
    assert "secret" not in caplog.text


def test_main_deletes_all_given_teams(module, session):
    """_main deletes every team passed on the command line."""
    with _use_session(module, session), \
            patch.object(module.sys, "argv", ["delete_teams", "team_a", "team_b"]):
        module._main()

    assert _names(session, Team.name) == set()
    assert _names(session, Pool.pool) == {"default_pool"}


def test_main_rolls_back_everything_on_partial_failure(module, session):
    """If deleting one team fails, no team is deleted and the error propagates."""
    original = module._delete_team

    def delete_then_fail(team, session):
        original(team, session)
        if team == "team_b":
            raise RuntimeError("boom")

    with _use_session(module, session), \
            patch.object(module, "_delete_team", side_effect=delete_then_fail), \
            patch.object(module.sys, "argv", ["delete_teams", "team_a", "team_b"]):
        with pytest.raises(RuntimeError, match="boom"):
            module._main()

    assert _names(session, Team.name) == {"team_a", "team_b"}
    assert _names(session, Connection.conn_id) == {"conn_a", "conn_b", "conn_global"}
    assert _ti_states(session)["ti_a_scheduled"] == TaskInstanceState.SCHEDULED


def test_main_without_teams_exits(module):
    """Running the script without team names is an error."""
    with patch.object(module.sys, "argv", ["delete_teams"]):
        with pytest.raises(SystemExit) as exc:
            module._main()
    assert exc.value.code == 1
