"""Tests for mwaa.validation.compat on Airflow 2.10.1."""

import importlib
from unittest.mock import patch

from mwaa.validation import compat


def test_make_dagbag_uses_this_versions_dagbag(tmp_path):
    """Airflow 2 keeps DagBag in airflow.models.dagbag."""
    dag_file = tmp_path / "a.py"
    dag_file.write_text("")
    dagbag = compat.make_dagbag(str(dag_file))
    assert type(dagbag).__module__ == "airflow.models.dagbag"
    assert dagbag.dags == {}


def test_plugin_import_errors_loads_plugins_first():
    """Plugins are loaded, then plugins_manager.import_errors is returned."""
    with patch.object(compat.plugins_manager, "ensure_plugins_loaded") as ensure, \
            patch.object(compat.plugins_manager, "import_errors", {"/p/bad.py": "boom"}):
        assert compat.plugin_import_errors() == {"/p/bad.py": "boom"}
    ensure.assert_called_once_with()


def test_reset_orm_after_fork_reconfigures_the_orm():
    """Airflow 2 has no fork hook, so the worker builds its own engine."""
    with patch.object(compat.settings, "configure_orm") as configure:
        compat.reset_orm_after_fork()
    configure.assert_called_once_with()


def test_resolve_category_returns_the_recorded_class():
    """DagBag in Airflow 2 keeps the real warning class."""
    assert compat.resolve_category(DeprecationWarning, "/d/a.py", 3, "old", None, "/d/a.py") == "DeprecationWarning"


def test_list_dag_files_honors_safe_mode_and_airflowignore(tmp_path):
    """Only files Airflow would parse are listed."""
    (tmp_path / "dag_file.py").write_text('"""An airflow dag."""\n')
    (tmp_path / "helper.py").write_text('"""No keywords here."""\n')
    (tmp_path / "ignored_dag.py").write_text('"""An airflow dag."""\n')
    (tmp_path / ".airflowignore").write_text("ignored_dag.py\n")
    assert compat.list_dag_files(str(tmp_path)) == [str(tmp_path / "dag_file.py")]


def test_safe_mode_follows_airflow_config(monkeypatch):
    """safe_mode reads [core] dag_discovery_safe_mode."""
    monkeypatch.setenv("AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE", "False")
    assert compat.safe_mode() is False
    monkeypatch.setenv("AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE", "True")
    assert compat.safe_mode() is True


def test_warm_up_modules_are_importable():
    """Every warm-up module exists in this Airflow version."""
    for module in compat.WARM_UP_MODULES:
        importlib.import_module(module)


def test_warm_up_providers_loads_nothing():
    """Airflow 2 has no provider code to load before parsing."""
    assert compat.warm_up_providers() == {}
