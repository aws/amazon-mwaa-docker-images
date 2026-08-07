"""Tests for _ensure_rds_iam_user credential-failure semantics.

migrate_with_downgrade.py deliberately refuses to be imported (it ends in an
``else: sys.exit(1)`` guard so it can only be run as a script), so these tests
execute the module body up to that guard in a throwaway namespace.

The static connection is failed by patching ``get_db_connection_string``, which
both module shapes route through: 3.0.6 builds the engine in an inline
``_connect_static()``, while 3.2.1 and 3.3.0 use a shared
``_create_engine_with_retry()``.
"""

import os
import types

import pytest
from unittest.mock import MagicMock, patch

_AIRFLOW_VERSION = "3.0.6"

_BASE_ENV = {
    "MWAA__DB__POSTGRES_HOST": "test.rds.amazonaws.com",
    "MWAA__DB__POSTGRES_PORT": "5432",
    "MWAA__DB__POSTGRES_DB": "AirflowMetadata",
    "MWAA__DB__POSTGRES_SSLMODE": "require",
    "MWAA__DB__CREDENTIALS": '{"username": "adminuser", "password": "pw"}',
    "AWS_EXECUTION_ENV": "Amazon_MWAA_test",
}


def _load_module():
    """Execute migrate_with_downgrade.py up to its no-import guard."""
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..", "..",
            "images", "airflow", _AIRFLOW_VERSION,
            "python", "mwaa", "database", "migrate_with_downgrade.py",
        )
    )
    assert os.path.exists(path), f"missing module: {path}"
    source = open(path).read()
    guard = source.index('if __name__ == "__main__":')
    module = types.ModuleType("mwaa_migrate_with_downgrade_probe")
    module.__file__ = path
    exec(compile(source[:guard], path, "exec"), module.__dict__)
    return module


def test_raises_when_static_and_iam_both_fail():
    """Neither credential path working must fail loudly, not skip.

    The grants cannot be applied without a connection, so migrate-db must not
    continue: the container should fail. Asserted explicitly because every other
    failure path in this function logs a warning and returns.
    """
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()

        with patch.object(
            module,
            "get_db_connection_string",
            side_effect=Exception("static credentials rejected"),
        ), patch.object(
            module.RDSIAMCredentialProvider,
            "get_token",
            side_effect=Exception("could not mint IAM token"),
        ):
            with pytest.raises(RuntimeError, match="static or RDS IAM credentials"):
                module._ensure_rds_iam_user()


def test_chains_the_underlying_iam_error():
    """The original IAM failure must be preserved for debugging."""
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()

        with patch.object(
            module,
            "get_db_connection_string",
            side_effect=Exception("static credentials rejected"),
        ), patch.object(
            module.RDSIAMCredentialProvider,
            "get_token",
            side_effect=Exception("could not mint IAM token"),
        ):
            with pytest.raises(RuntimeError) as excinfo:
                module._ensure_rds_iam_user()

    assert "could not mint IAM token" in str(excinfo.value.__cause__)


def test_skips_without_raising_when_iam_is_disabled():
    """With the feature flag off, a static failure stays best-effort.

    This preserves the pre-existing contract for non-IAM environments: the
    function logs and returns so migrate-db can carry on.
    """
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "false"}):
        module = _load_module()

        with patch.object(
            module,
            "get_db_connection_string",
            side_effect=Exception("static credentials rejected"),
        ):
            module._ensure_rds_iam_user()  # must not raise


def test_no_iam_fallback_attempted_when_not_using_rds_proxy():
    """IAM auth is only supported through RDS Proxy, so don't try otherwise."""
    env = {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}
    env["MWAA__DB__POSTGRES_SSLMODE"] = "prefer"

    with patch.dict("os.environ", env):
        module = _load_module()

        with patch.object(
            module,
            "get_db_connection_string",
            side_effect=Exception("static credentials rejected"),
        ), patch.object(
            module.RDSIAMCredentialProvider, "get_token"
        ) as mock_get_token:
            module._ensure_rds_iam_user()  # must not raise
            mock_get_token.assert_not_called()


# --- Grant application paths ---


def _mock_engine(current_user, role_exists=True, fail_on=None):
    """Build a mock engine whose connection answers the role queries.

    Patched in at ``create_engine``, which both module shapes route through:
    3.0.6 builds the engine in an inline ``_connect_static()`` while 3.2.1 and
    3.3.0 use a shared ``_create_engine_with_retry()``.

    :param fail_on: substring of a statement that should raise instead of
        returning, used to exercise the grant-failure path.
    """
    conn = MagicMock()

    def execute(statement, params=None):
        text = str(statement)
        if fail_on and fail_on in text:
            raise Exception(f"failed executing {fail_on}")
        result = MagicMock()
        if "pg_roles" in text:
            result.fetchone.return_value = (1,) if role_exists else None
        elif "current_user" in text:
            result.scalar.return_value = current_user
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def _executed_statements(conn):
    return [str(call.args[0]) for call in conn.execute.call_args_list]


def test_grants_are_applied_when_connected_as_adminuser():
    """The full grant block must run when the current role can grant."""
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()
        engine, conn = _mock_engine(current_user="adminuser")

        with patch.object(module, "create_engine", return_value=engine):
            module._ensure_rds_iam_user()

    statements = " | ".join(_executed_statements(conn))
    assert "GRANT rds_iam TO airflow_user" in statements
    assert 'GRANT ALL PRIVILEGES ON DATABASE "AirflowMetadata" TO airflow_user' in statements
    assert "GRANT ALL ON SCHEMA public TO airflow_user" in statements
    assert "GRANT ALL ON ALL TABLES IN SCHEMA public TO airflow_user" in statements
    assert "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO airflow_user" in statements
    assert "GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO airflow_user" in statements
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES" in statements
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES" in statements
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS" in statements
    # The grant that lets airflow_user inherit objects created by adminuser.
    assert "GRANT adminuser TO airflow_user" in statements


def test_iam_user_is_created_when_missing():
    """A missing role must be created before the grants are applied."""
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()
        engine, conn = _mock_engine(current_user="adminuser", role_exists=False)

        with patch.object(module, "create_engine", return_value=engine):
            module._ensure_rds_iam_user()

    assert "CREATE USER airflow_user" in " | ".join(_executed_statements(conn))


def test_grants_are_skipped_when_connected_as_airflow_user():
    """airflow_user cannot grant, so the block must be skipped, not attempted.

    Note this is also how grants end up never applied when only the IAM path
    works, since that connects as airflow_user -- tracked separately.
    """
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()
        engine, conn = _mock_engine(current_user="airflow_user")

        with patch.object(module, "create_engine", return_value=engine):
            module._ensure_rds_iam_user()

    statements = " | ".join(_executed_statements(conn))
    assert "GRANT" not in statements


def test_grant_failure_is_swallowed():
    """Errors while applying grants log and return; they must not propagate."""
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()
        engine, _ = _mock_engine(current_user="adminuser", fail_on="pg_roles")

        with patch.object(module, "create_engine", return_value=engine):
            module._ensure_rds_iam_user()  # must not raise


def test_iam_fallback_succeeds_and_applies_grants():
    """A working IAM fallback must proceed to the role handling.

    Exercises the fallback engine construction, which otherwise only runs on the
    failure paths.
    """
    with patch.dict("os.environ", {**_BASE_ENV, "USE_IAM_CREDENTIALS": "true"}):
        module = _load_module()
        engine, conn = _mock_engine(current_user="airflow_user")

        with patch.object(
            module,
            "get_db_connection_string",
            side_effect=Exception("static credentials rejected"),
        ), patch.object(
            module.RDSIAMCredentialProvider, "get_token", return_value="tok"
        ), patch.object(
            module.RDSIAMCredentialProvider,
            "create_db_connection_url",
            return_value="postgresql+psycopg2://airflow_user:tok@h:5432/d",
        ), patch.object(
            module, "create_engine", return_value=engine
        ):
            module._ensure_rds_iam_user()

    # Reached the role query via the IAM engine.
    assert any("current_user" in s for s in _executed_statements(conn))
