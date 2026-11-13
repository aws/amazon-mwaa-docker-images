"""Tests for parsing one DAG file in mwaa.validation.parse_dags."""

import json
import os
import sys

import pytest

from mwaa.validation import parse_dags

# Airflow 3.2+: the Airflow 2 operator import path warns at the importing line.
AIRFLOW_DEPRECATION_DAG = '"""An airflow dag file."""\nfrom airflow.operators.bash import BashOperator  # noqa: F401\n'
AIRFLOW_DEPRECATION_CATEGORY = "DeprecatedImportWarning"
AIRFLOW_DEPRECATION_LINE = 2

BROKEN_DAG = '"""An airflow dag file."""\nimport missing_module_for_mwaa_test  # noqa: F401\n'


@pytest.fixture(scope="module", autouse=True)
def _warm_up():
    parse_dags.warm_up()


def test_good_dag_parses_without_warnings(write_dag, dags_folder, good_dag_source):
    """A clean DAG has its id, no error and no warnings."""
    path = write_dag("good.py", good_dag_source)
    assert parse_dags.parse_one_file(path, str(dags_folder)) == {
        "file": "good.py",
        "dag_ids": ["good"],
        "import_error": None,
        "import_error_traceback": None,
        "warnings": [],
    }


def test_missing_module_is_an_import_error(write_dag, dags_folder):
    """An import failure is summarized and the traceback is kept."""
    entry = parse_dags.parse_one_file(write_dag("broken.py", BROKEN_DAG), str(dags_folder))
    assert entry["dag_ids"] == []
    assert entry["import_error"] == "ModuleNotFoundError: No module named 'missing_module_for_mwaa_test'"
    assert "Traceback" in entry["import_error_traceback"]


def test_system_exit_is_an_import_error(write_dag, dags_folder):
    """DAG code that calls sys.exit does not stop validation."""
    path = write_dag("exits.py", '"""An airflow dag file."""\nimport sys\n\nsys.exit(3)\n')
    assert parse_dags.parse_one_file(path, str(dags_folder))["import_error"] == "SystemExit: 3"


def test_warning_with_stacklevel_2_points_at_the_dag_file(write_dag, dags_folder):
    """A helper that warns with stacklevel=2, like days_ago, is reported at the caller's line."""
    write_dag(
        "old_helpers.py",
        '"""Helpers."""\nimport warnings\n\n\ndef old_function():\n'
        '    """Warn the caller."""\n'
        '    warnings.warn("old_function is deprecated", DeprecationWarning, stacklevel=2)\n',
    )
    path = write_dag("caller.py", '"""An airflow dag file."""\nfrom old_helpers import old_function\n\nold_function()\n')
    assert parse_dags.parse_one_file(path, str(dags_folder))["warnings"] == [
        {"category": "DeprecationWarning", "message": "old_function is deprecated", "lineno": 4, "location": "caller.py:4"}
    ]


def test_warning_raised_outside_the_dag_file_has_no_lineno(write_dag, dags_folder):
    """A warning that points into another file keeps its location but has no lineno."""
    write_dag("noisy_helper.py", '"""Helper."""\nimport warnings\n\nwarnings.warn("noisy helper", FutureWarning)\n')
    path = write_dag("uses_noisy.py", '"""An airflow dag file."""\nimport noisy_helper  # noqa: F401\n')
    assert parse_dags.parse_one_file(path, str(dags_folder))["warnings"] == [
        {"category": "FutureWarning", "message": "noisy helper", "lineno": None, "location": "noisy_helper.py:4"}
    ]


def test_repeated_warning_is_reported_once(write_dag, dags_folder):
    """The same warning from the same place is reported once."""
    path = write_dag(
        "loop.py",
        '"""An airflow dag file."""\nimport warnings\n\nfor _ in range(3):\n'
        '    warnings.warn("again", DeprecationWarning)\n',
    )
    assert parse_dags.parse_one_file(path, str(dags_folder))["warnings"] == [
        {"category": "DeprecationWarning", "message": "again", "lineno": 5, "location": "loop.py:5"}
    ]


def test_airflow_deprecation_is_reported_against_the_dag_file(write_dag, dags_folder):
    """This Airflow version's own deprecation warning keeps its real category and the DAG line."""
    path = write_dag("airflow_deprecation.py", AIRFLOW_DEPRECATION_DAG)
    warnings = parse_dags.parse_one_file(path, str(dags_folder))["warnings"]
    assert [(w["category"], w["lineno"], w["location"]) for w in warnings] == [
        (AIRFLOW_DEPRECATION_CATEGORY, AIRFLOW_DEPRECATION_LINE, f"airflow_deprecation.py:{AIRFLOW_DEPRECATION_LINE}")
    ]


def test_display_path(tmp_path):
    """Paths inside the base folder are shortened; others are kept as given."""
    base = tmp_path / "dags"
    base.mkdir()
    assert parse_dags.display_path(str(base / "sub" / "a.py"), str(base)) == os.path.join("sub", "a.py")
    assert parse_dags.display_path("/elsewhere/a.py", str(base)) == "/elsewhere/a.py"
    assert parse_dags.display_path("<frozen importlib._bootstrap>", str(base)) == "<frozen importlib._bootstrap>"


def test_display_path_keeps_a_symlinks_own_name(tmp_path):
    """A symlink inside the folder is reported under its own name, not its target's."""
    base = tmp_path / "dags"
    (base / "real").mkdir(parents=True)
    (base / "real" / "target.py").write_text("")
    (base / "link.py").symlink_to(base / "real" / "target.py")
    assert parse_dags.display_path(str(base / "link.py"), str(base)) == "link.py"


def test_display_path_with_a_symlinked_base_folder(tmp_path):
    """A base folder reached through a symlink still shortens paths given by their real location."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "a.py").write_text("")
    alias = tmp_path / "alias"
    alias.symlink_to(real)
    assert parse_dags.display_path(str(real / "a.py"), str(alias)) == "a.py"


def test_worker_timeout_follows_dagbag_import_timeout(monkeypatch):
    """The outer timeout is Airflow's dagbag_import_timeout plus a margin."""
    monkeypatch.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "7")
    assert parse_dags.worker_timeout_seconds() == 7 + parse_dags.WORKER_TIMEOUT_MARGIN_SECONDS


def test_collect_plugin_import_errors(monkeypatch, tmp_path):
    """Plugin errors are keyed by path relative to the plugins folder and summarized."""
    plugins = tmp_path / "plugins"
    errors = {
        str(plugins / "b.py"): "No module named 'missing_plugin_dependency'",
        "some.entrypoint.module": "Traceback (most recent call last):\n  File \"x\", line 1\nValueError: bad",
    }
    monkeypatch.setattr(parse_dags.compat, "plugin_import_errors", lambda: errors)
    assert parse_dags.collect_plugin_import_errors(str(plugins)) == [
        {"file": "b.py", "error": "No module named 'missing_plugin_dependency'"},
        {"file": "some.entrypoint.module", "error": "ValueError: bad"},
    ]


def test_main_writes_plugins_and_dags(monkeypatch, write_dag, dags_folder, good_dag_source, tmp_path):
    """main lists the DAG files Airflow would parse and writes one JSON result."""
    write_dag("good.py", good_dag_source)
    write_dag("broken.py", BROKEN_DAG)
    write_dag("notes.py", '"""No keywords here."""\n')
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(parse_dags.compat, "plugin_import_errors", lambda: {str(tmp_path / "plugins" / "bad.py"): "boom"})
    monkeypatch.setattr(
        parse_dags, "parse_all",
        lambda files, folder, timeout: ([{"file": os.path.basename(f)} for f in files], "fork"),
    )
    output = tmp_path / "out.json"
    argv = ["--dags-folder", str(dags_folder), "--plugins-folder", str(tmp_path / "plugins"), "--output", str(output)]
    assert parse_dags.main(argv) == 0
    assert json.loads(output.read_text()) == {
        "plugins": {"import_errors": [{"file": "bad.py", "error": "boom"}]},
        "dags": [{"file": "broken.py"}, {"file": "good.py"}],
        "parse_mode": "fork",
    }
    assert str(dags_folder) in sys.path


def test_main_one_file_mode(monkeypatch, write_dag, dags_folder, good_dag_source, tmp_path):
    """--one parses a single file in this process and writes its entry."""
    path = write_dag("good.py", good_dag_source)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(parse_dags.compat, "plugin_import_errors", lambda: {})
    output = tmp_path / "entry.json"
    assert parse_dags.main(["--dags-folder", str(dags_folder), "--one", path, "--output", str(output)]) == 0
    assert json.loads(output.read_text())["dag_ids"] == ["good"]
