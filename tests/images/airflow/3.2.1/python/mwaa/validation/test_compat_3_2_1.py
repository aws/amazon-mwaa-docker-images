"""Tests for mwaa.validation.compat on Airflow 3.2.1."""

import importlib
import inspect
from types import SimpleNamespace
from unittest.mock import patch

from mwaa.validation import compat


def _dagbag(entries, dag_file="/d/a.py"):
    return SimpleNamespace(captured_warnings={dag_file: tuple(entries)})


def _resolve(category, filename, lineno, message, dagbag, dag_file="/d/a.py"):
    return compat.resolve_category(category, filename, lineno, message, dagbag, dag_file)


def test_make_dagbag_uses_this_versions_dagbag(tmp_path):
    """Airflow 3.2 and later keep DagBag in airflow.dag_processing.dagbag."""
    dag_file = tmp_path / "a.py"
    dag_file.write_text("")
    dagbag = compat.make_dagbag(str(dag_file))
    assert type(dagbag).__module__ == "airflow.dag_processing.dagbag"
    assert dagbag.dags == {}


def test_dagbag_still_accepts_include_examples():
    """Airflow 3.2 still has include_examples, so make_dagbag turns examples off explicitly."""
    from airflow.dag_processing.dagbag import DagBag

    assert "include_examples" in inspect.signature(DagBag.__init__).parameters


def test_plugin_import_errors_loads_plugins_first():
    """Plugins are loaded, then plugins_manager.get_import_errors() is returned."""
    with patch.object(compat.plugins_manager, "ensure_plugins_loaded") as ensure, \
            patch.object(compat.plugins_manager, "get_import_errors", return_value={"/p/bad.py": "boom"}):
        assert compat.plugin_import_errors() == {"/p/bad.py": "boom"}
    ensure.assert_called_once_with()


def test_reset_orm_after_fork_relies_on_airflows_fork_hook():
    """Airflow 3 disposes the engine in an at-fork hook, so nothing else is needed."""
    with patch("airflow.settings.configure_orm") as configure:
        assert compat.reset_orm_after_fork() is None
    configure.assert_not_called()


def test_resolve_category_reads_the_type_airflow_captured():
    """A re-emitted UserWarning gets the class the importer recorded."""
    message = "The `airflow.operators.bash.BashOperator` attribute is deprecated."
    dagbag = _dagbag([f"/d/a.py:4: airflow.utils.deprecation_tools.DeprecatedImportWarning: {message}"])
    assert _resolve(UserWarning, "/d/a.py", 4, message, dagbag) == "DeprecatedImportWarning"


def test_resolve_category_handles_colons_in_the_path_and_message():
    """Matching uses the exact prefix and suffix, so colons do not confuse it."""
    dagbag = _dagbag(["/d/odd:name.py:7: DeprecationWarning: use a: b instead: now"], "/d/odd:name.py")
    assert _resolve(UserWarning, "/d/odd:name.py", 7, "use a: b instead: now", dagbag, "/d/odd:name.py") == "DeprecationWarning"


def test_resolve_category_matches_a_missing_line_number():
    """Airflow re-emits a warning without a line number with lineno 0."""
    assert _resolve(UserWarning, "/d/a.py", 0, "soon", _dagbag(["/d/a.py:None: FutureWarning: soon"])) == "FutureWarning"


def test_resolve_category_keeps_user_warning_without_a_match():
    """A real UserWarning, or one with no captured entry, stays UserWarning."""
    assert _resolve(UserWarning, "/d/a.py", 4, "other", _dagbag(["/d/a.py:4: FutureWarning: soon"])) == "UserWarning"


def test_resolve_category_keeps_other_classes():
    """Warnings raised outside the importer already have their real class."""
    assert _resolve(DeprecationWarning, "/d/a.py", 4, "soon", _dagbag(["/d/a.py:4: FutureWarning: soon"])) == "DeprecationWarning"


def test_resolve_category_without_a_dagbag():
    """If the DagBag could not be built, the recorded class is used."""
    assert _resolve(UserWarning, "/d/a.py", 4, "soon", None) == "UserWarning"


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


def test_warm_up_providers_loads_the_task_decorators():
    """warm_up_providers returns the decorators @task looks up, including python."""
    assert "python" in compat.warm_up_providers()
