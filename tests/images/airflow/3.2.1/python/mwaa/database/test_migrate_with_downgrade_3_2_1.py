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
from unittest.mock import patch

_AIRFLOW_VERSION = "3.2.1"

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
