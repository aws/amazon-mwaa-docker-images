import pytest
from unittest.mock import patch, MagicMock


def _reset_patch_state():
    """Reset the one-shot install flag so a test can install the patch again."""
    from mwaa.config import rds_iam_patch

    rds_iam_patch._patch_installed = False
    getattr(rds_iam_patch._get_metadata_url, "cache_clear", lambda: None)()



def test_is_from_migrate_db():
    """Test migrate-db detection"""
    from mwaa.config.rds_iam_patch import _is_from_migrate_db

    with patch.dict("os.environ", {"MWAA_AIRFLOW_COMPONENT": "migrate-db"}):
        assert _is_from_migrate_db() is True

    with patch.dict("os.environ", {"MWAA_AIRFLOW_COMPONENT": "scheduler"}):
        assert _is_from_migrate_db() is False

    with patch.dict("os.environ", {}, clear=True):
        assert _is_from_migrate_db() is False


def test_is_using_rds_proxy():
    """Test RDS proxy detection via is_using_rds_proxy helper"""
    from mwaa.config.rds_iam_credentials import is_using_rds_proxy

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "require"}):
        assert is_using_rds_proxy() is True

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "disable"}):
        assert is_using_rds_proxy() is False

    with patch.dict("os.environ", {}, clear=True):
        assert is_using_rds_proxy() is False


def test_use_iam_credentials():
    """Test IAM credentials flag detection"""
    from mwaa.config.rds_iam_credentials import use_iam_credentials

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "true"}):
        assert use_iam_credentials() is True

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}):
        assert use_iam_credentials() is False

    with patch.dict("os.environ", {}, clear=True):
        assert use_iam_credentials() is False


def test_is_accessing_metadata_db():
    """Test metadata database detection"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url"
    ) as mock_get_url:
        mock_url = MagicMock()
        mock_url.host = "test.rds.amazonaws.com"
        mock_url.database = "airflow"
        mock_url.port = 5432
        mock_get_url.return_value = mock_url

        # Test with matching parameters including username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is True

        # Test with adminuser username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "adminuser",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is True

        # Test with non-matching parameters
        cparams = {
            "host": "other.rds.amazonaws.com",
            "database": "other",
            "port": 3306,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False

        # Test with missing username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False


def test_is_accessing_metadata_db_with_cargs():
    """Test metadata database detection using cargs"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url"
    ) as mock_get_url:
        mock_url = MagicMock()
        mock_url.host = "test.rds.amazonaws.com"
        mock_url.database = "airflow"
        mock_url.port = 5432
        mock_get_url.return_value = mock_url

        # Test with matching cargs including airflow_user
        cargs = ["postgresql://airflow_user:pass@test.rds.amazonaws.com:5432/airflow"]
        assert _is_accessing_metadata_db("postgresql", cargs, {}) is True

        # Test with matching cargs including adminuser
        cargs = ["postgresql://adminuser:pass@test.rds.amazonaws.com:5432/airflow"]
        assert _is_accessing_metadata_db("postgresql", cargs, {}) is True


def test_is_accessing_metadata_db_no_conn_str():
    """Test metadata database detection when no connection string is set"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url", return_value=None
    ):
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False


def test_install_rds_iam_patch_not_installed_no_iam():
    """Test patch not installed when USE_IAM_CREDENTIALS is not true"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}, clear=True), patch(
        "sqlalchemy.event.listen"
    ) as mock_listen:
        install_rds_iam_patch()
        mock_listen.assert_not_called()


def test_install_rds_iam_patch_not_installed_no_rds_proxy():
    """Test patch not installed when not using RDS Proxy"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict(
        "os.environ",
        {"USE_IAM_CREDENTIALS": "true", "MWAA__DB__POSTGRES_SSLMODE": "disable"},
    ), patch("sqlalchemy.event.listen") as mock_listen:
        install_rds_iam_patch()
        mock_listen.assert_not_called()


def test_install_rds_iam_patch_not_installed_migrate_db():
    """Test patch not installed for migrate-db process"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict(
        "os.environ",
        {
            "USE_IAM_CREDENTIALS": "true",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
            "MWAA_AIRFLOW_COMPONENT": "migrate-db",
        },
    ):
        _reset_patch_state()
        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            mock_listen.assert_not_called()


def test_install_rds_iam_patch_installed():
    """Test patch installed when all conditions are met"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict(
        "os.environ",
        {
            "USE_IAM_CREDENTIALS": "true",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
            "MWAA_AIRFLOW_COMPONENT": "scheduler",
        },
    ):
        _reset_patch_state()
        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            mock_listen.assert_called_once()


def _clear_metadata_url_cache(module=None):
    """Clear the cached metadata URL, tolerating an uncached implementation."""
    if module is None:
        from mwaa.config import rds_iam_patch as module
    getattr(module._get_metadata_url, "cache_clear", lambda: None)()


# --- Regression tests for the aspects restored from the 2.x implementation ---

# Environment describing a metadata DB reachable through an RDS Proxy. Note the
# deliberate absence of AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: that variable is
# only injected into the environment dict handed to the Airflow subprocesses, so
# code running in-process must not depend on it.
_RDS_PROXY_ENV = {
    "USE_IAM_CREDENTIALS": "true",
    "MWAA__DB__POSTGRES_HOST": "test.rds.amazonaws.com",
    "MWAA__DB__POSTGRES_PORT": "5432",
    "MWAA__DB__POSTGRES_DB": "AirflowMetadata",
    "MWAA__DB__POSTGRES_SSLMODE": "require",
    "MWAA__DB__CREDENTIALS": '{"username": "airflow_user", "password": "pw"}',
    "MWAA_AIRFLOW_COMPONENT": "scheduler",
}


def test_get_metadata_url_derived_from_mwaa_db_env_vars():
    """The metadata URL must come from MWAA__DB__*, not from the Airflow conn var.

    Regression test: sourcing it from AIRFLOW__DATABASE__SQL_ALCHEMY_CONN made
    _get_metadata_url() return None in every process, which silently disabled
    token injection.
    """
    from mwaa.config.rds_iam_patch import _get_metadata_url

    with patch.dict("os.environ", _RDS_PROXY_ENV, clear=True):
        _clear_metadata_url_cache()
        try:
            url = _get_metadata_url()
            assert url is not None
            assert url.host == "test.rds.amazonaws.com"
            assert url.database == "AirflowMetadata"
            assert url.port == 5432
        finally:
            _clear_metadata_url_cache()


def test_get_metadata_url_returns_none_on_missing_config():
    """A misconfigured environment must not raise into the do_connect listener."""
    from mwaa.config.rds_iam_patch import _get_metadata_url

    with patch.dict("os.environ", {}, clear=True):
        _clear_metadata_url_cache()
        try:
            assert _get_metadata_url() is None
        finally:
            _clear_metadata_url_cache()


def test_end_to_end_metadata_db_detected_without_airflow_conn_var():
    """The patch must recognise the metadata DB using only MWAA__DB__* vars."""
    import mwaa.config.rds_iam_patch

    with patch.dict("os.environ", _RDS_PROXY_ENV, clear=True):
        _clear_metadata_url_cache(mwaa.config.rds_iam_patch)
        cparams = {
            "host": "test.rds.amazonaws.com",
            "dbname": "AirflowMetadata",
            "port": 5432,
            "user": "airflow_user",
        }
        assert (
            mwaa.config.rds_iam_patch._is_accessing_metadata_db(
                "postgresql", [], cparams
            )
            is True
        )


def test_install_rds_iam_patch_not_installed_local_runner():
    """Test patch not installed when running as the local runner."""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict(
        "os.environ", {**_RDS_PROXY_ENV, "MWAA_LOCAL_RUNNER": "true"}, clear=True
    ):
        _reset_patch_state()
        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            mock_listen.assert_not_called()


def test_install_rds_iam_patch_is_idempotent():
    """Installing twice in one process must register only one listener."""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict("os.environ", _RDS_PROXY_ENV, clear=True):
        _reset_patch_state()
        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            install_rds_iam_patch()
            install_rds_iam_patch()
            mock_listen.assert_called_once()


def test_airflow_local_settings_installs_the_patch():
    """airflow_local_settings.py is the hook Airflow loads in every process.

    Regression test for the dropped override: without this file the do_connect
    listener is never registered in the scheduler/worker/api-server processes,
    which are separate Popen children of the entrypoint.
    """
    import importlib.util
    import os

    settings_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..", "..",
            "images", "airflow", "3.3.0", "airflow_local_settings.py",
        )
    )
    assert os.path.exists(settings_path), f"missing override: {settings_path}"

    with patch(
        "mwaa.config.rds_iam_patch.install_rds_iam_patch"
    ) as mock_install:
        spec = importlib.util.spec_from_file_location(
            "mwaa_test_airflow_local_settings", settings_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        mock_install.assert_called_once()

    # Must not leak names into the airflow.settings namespace.
    assert module.__all__ == []


# --- Coverage for the customer-settings loading and the listener body ---

_LS_VERSION = "3.3.0"


def _local_settings_file():
    import os

    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..", "..",
            "images", "airflow", _LS_VERSION, "airflow_local_settings.py",
        )
    )
    assert os.path.exists(path), f"missing override: {path}"
    return path


def _exec_local_settings(airflow_home=None):
    """Execute the shipped airflow_local_settings.py with the patch stubbed out."""
    import importlib.util

    env = {"AIRFLOW_HOME": airflow_home} if airflow_home else {}
    with patch.dict("os.environ", env), patch(
        "mwaa.config.rds_iam_patch.install_rds_iam_patch"
    ):
        spec = importlib.util.spec_from_file_location(
            "mwaa_test_local_settings_cov", _local_settings_file()
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def _write_customer_file(home, subdir, body):
    import os

    os.makedirs(os.path.join(home, subdir), exist_ok=True)
    with open(os.path.join(home, subdir, "airflow_local_settings.py"), "w") as f:
        f.write(body)


def test_customer_settings_in_dags_are_executed(tmp_path):
    """A customer file in dags/ must be loaded, so its side effects apply."""
    marker = tmp_path / "dags_ran"
    _write_customer_file(
        str(tmp_path),
        "dags",
        f"open({str(marker)!r}, 'w').write('ran')\n",
    )

    _exec_local_settings(str(tmp_path))

    assert marker.exists(), "dags/airflow_local_settings.py was not executed"


def test_customer_settings_in_plugins_are_executed(tmp_path):
    """A customer file in plugins/ must be loaded too."""
    marker = tmp_path / "plugins_ran"
    _write_customer_file(
        str(tmp_path),
        "plugins",
        f"open({str(marker)!r}, 'w').write('ran')\n",
    )

    _exec_local_settings(str(tmp_path))

    assert marker.exists(), "plugins/airflow_local_settings.py was not executed"


def test_broken_customer_settings_propagate(tmp_path):
    """A customer file that fails to load must surface, not be swallowed.

    Matches what Airflow does natively when a local settings file raises.
    """
    _write_customer_file(str(tmp_path), "plugins", "raise ValueError('customer bug')\n")

    with pytest.raises(ValueError, match="customer bug"):
        _exec_local_settings(str(tmp_path))


def test_patch_install_failure_propagates():
    """A failure installing the RDS IAM patch must not be silently ignored."""
    import importlib.util

    with patch(
        "mwaa.config.rds_iam_patch.install_rds_iam_patch",
        side_effect=RuntimeError("patch install blew up"),
    ):
        spec = importlib.util.spec_from_file_location(
            "mwaa_test_local_settings_fail", _local_settings_file()
        )
        module = importlib.util.module_from_spec(spec)
        with pytest.raises(RuntimeError, match="patch install blew up"):
            spec.loader.exec_module(module)


def test_do_connect_listener_injects_the_token():
    """Exercise the registered listener body, not just its registration."""
    from mwaa.config import rds_iam_patch as m

    captured = {}

    def fake_listen(target, name, fn):
        captured["fn"] = fn

    env = {
        "USE_IAM_CREDENTIALS": "true",
        "MWAA__DB__POSTGRES_HOST": "proxy.rds.amazonaws.com",
        "MWAA__DB__POSTGRES_PORT": "5432",
        "MWAA__DB__POSTGRES_DB": "AirflowMetadata",
        "MWAA__DB__POSTGRES_SSLMODE": "require",
        "MWAA__DB__CREDENTIALS": '{"username": "airflow_user", "password": "pw"}',
        "MWAA_AIRFLOW_COMPONENT": "scheduler",
    }

    with patch.dict("os.environ", env, clear=True), patch(
        "sqlalchemy.event.listen", side_effect=fake_listen
    ):
        _reset_patch_state()
        m.install_rds_iam_patch()

    assert "fn" in captured, "listener was not registered"

    token_url = (
        "postgresql+psycopg2://airflow_user:tok"
        "@proxy.rds.amazonaws.com:5432/AirflowMetadata?sslmode=require"
    )
    cparams = {
        "host": "proxy.rds.amazonaws.com",
        "dbname": "AirflowMetadata",
        "port": 5432,
        "user": "airflow_user",
        "stale": "should be cleared",
    }

    with patch.dict("os.environ", env, clear=True), patch.object(
        m.RDSIAMCredentialProvider, "get_token", return_value="tok"
    ), patch.object(
        m.RDSIAMCredentialProvider, "create_db_connection_url", return_value=token_url
    ):
        _clear_metadata_url_cache(m)
        captured["fn"]("postgresql", None, [], cparams)

    # cparams was rebuilt from the IAM URL, and the pre-existing key is gone.
    assert "stale" not in cparams
    assert cparams["password"] == "tok"
    assert cparams["host"] == "proxy.rds.amazonaws.com"
    # 'dbname'/'database' must not both be present (psycopg2 rejects that).
    assert not ("dbname" in cparams and "database" in cparams)


def test_do_connect_listener_ignores_other_databases():
    """Connections that are not the metadata DB must be left untouched."""
    from mwaa.config import rds_iam_patch as m

    captured = {}
    env = {
        "USE_IAM_CREDENTIALS": "true",
        "MWAA__DB__POSTGRES_HOST": "proxy.rds.amazonaws.com",
        "MWAA__DB__POSTGRES_PORT": "5432",
        "MWAA__DB__POSTGRES_DB": "AirflowMetadata",
        "MWAA__DB__POSTGRES_SSLMODE": "require",
        "MWAA__DB__CREDENTIALS": '{"username": "airflow_user", "password": "pw"}',
        "MWAA_AIRFLOW_COMPONENT": "scheduler",
    }

    with patch.dict("os.environ", env, clear=True), patch(
        "sqlalchemy.event.listen", side_effect=lambda t, n, fn: captured.update(fn=fn)
    ):
        _reset_patch_state()
        m.install_rds_iam_patch()

    other = {"host": "someone-elses-db", "dbname": "other", "port": 5432, "user": "x"}
    before = dict(other)

    with patch.dict("os.environ", env, clear=True), patch.object(
        m.RDSIAMCredentialProvider, "get_token"
    ) as get_token:
        _clear_metadata_url_cache(m)
        captured["fn"]("postgresql", None, [], other)
        get_token.assert_not_called()

    assert other == before


def test_metadata_detection_tolerates_unparseable_cargs():
    """A malformed DSN in cargs must not raise out of the listener."""
    from mwaa.config import rds_iam_patch as m

    with patch.object(m, "_get_metadata_url") as mock_url:
        mock_url.return_value = MagicMock(
            host="h", database="d", port=5432, username="airflow_user"
        )
        # Not a URL at all; the cargs branch must swallow the parse error and
        # fall through to the cparams check.
        assert m._is_accessing_metadata_db("postgresql", ["not-a-dsn"], {}) is False


def test_dblock_connect_uses_iam_token_when_enabled():
    """dblock's maintenance connection must use IAM when the feature is on."""
    from mwaa.utils import dblock

    env = {
        "USE_IAM_CREDENTIALS": "true",
        "MWAA__DB__POSTGRES_SSLMODE": "require",
    }
    with patch.dict("os.environ", env, clear=True), patch.object(
        dblock, "create_engine"
    ) as create_engine, patch(
        "mwaa.config.rds_iam_credentials.RDSIAMCredentialProvider.get_token",
        return_value="tok",
    ), patch(
        "mwaa.config.rds_iam_credentials.RDSIAMCredentialProvider."
        "create_db_connection_url",
        return_value="postgresql+psycopg2://airflow_user:tok@h:5432/d",
    ):
        dblock._connect_with_retry()

    assert create_engine.call_args.args[0].endswith("@h:5432/d")


def test_dblock_connect_uses_static_credentials_when_disabled():
    """With the flag off it must fall back to the static connection string."""
    from mwaa.utils import dblock

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}, clear=True), \
            patch.object(dblock, "create_engine") as create_engine, \
            patch.object(
                dblock, "get_db_connection_string", return_value="postgresql://static"
            ):
        dblock._connect_with_retry()

    assert create_engine.call_args.args[0] == "postgresql://static"
