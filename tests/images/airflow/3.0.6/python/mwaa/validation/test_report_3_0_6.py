"""Tests for mwaa.validation.report."""

import pytest

from mwaa.validation.report import (
    EXIT_OK,
    EXIT_REQUIREMENTS_FAILED,
    EXIT_VALIDATION_FAILED,
    PARSE_MODE_FORK,
    REQUIREMENTS_FAILED,
    REQUIREMENTS_OK,
    REQUIREMENTS_SKIPPED,
    SCHEMA_VERSION,
    build_report,
    error_summary,
    exit_code,
    format_summary,
    no_dag_files_hint,
)


def _dag(file, dag_ids=(), import_error=None, warnings=()):
    return {
        "file": file,
        "dag_ids": list(dag_ids),
        "import_error": import_error,
        "import_error_traceback": None,
        "warnings": list(warnings),
    }


def _warning(category="DeprecationWarning", lineno=3):
    return {"category": category, "message": "old", "lineno": lineno, "location": f"x.py:{lineno}"}


def _report(**kwargs):
    fields = {"airflow_version": "3.3.1", "python_version": "3.12.16", "requirements_status": REQUIREMENTS_OK}
    fields.update(kwargs)
    return build_report(**fields)


def test_report_has_the_stable_top_level_fields():
    """The field names and defaults are a contract with callers."""
    report = _report()
    assert list(report) == [
        "schema_version", "airflow_version", "python_version", "error", "parse_mode",
        "requirements", "plugins", "dags", "summary",
    ]
    assert report["schema_version"] == SCHEMA_VERSION == 1
    assert report["airflow_version"] == "3.3.1"
    assert report["python_version"] == "3.12.16"
    assert report["error"] is None
    assert report["parse_mode"] == PARSE_MODE_FORK
    assert report["requirements"] == {"status": "ok", "error": None}
    assert report["plugins"] == {"import_errors": []}
    assert report["dags"] == []
    assert report["summary"] == {"files": 0, "dags": 0, "import_errors": 0, "plugin_import_errors": 0, "warnings": 0}


def test_summary_counts_and_sorting():
    """Summary counts come from the lists; files and plugin errors are sorted by name."""
    report = _report(
        plugin_import_errors=[{"file": "z.py", "error": "boom"}, {"file": "a.py", "error": "boom"}],
        dags=[
            _dag("b.py", ["b1", "b2"], warnings=[_warning(), _warning(lineno=4)]),
            _dag("a.py", import_error="ModuleNotFoundError: No module named 'imp'"),
            _dag("c.py", ["c"], warnings=[_warning()]),
        ],
    )
    assert report["summary"] == {"files": 3, "dags": 3, "import_errors": 1, "plugin_import_errors": 2, "warnings": 3}
    assert [dag["file"] for dag in report["dags"]] == ["a.py", "b.py", "c.py"]
    assert [entry["file"] for entry in report["plugins"]["import_errors"]] == ["a.py", "z.py"]


@pytest.mark.parametrize("kwargs,expected", [
    ({}, EXIT_OK),
    ({"dags": [_dag("a.py", ["a"], warnings=[_warning()])]}, EXIT_OK),
    ({"requirements_status": REQUIREMENTS_SKIPPED}, EXIT_OK),
    ({"dags": [_dag("a.py", import_error="boom")]}, EXIT_VALIDATION_FAILED),
    ({"plugin_import_errors": [{"file": "p.py", "error": "boom"}]}, EXIT_VALIDATION_FAILED),
    ({"error": "DAG parser exited with code 1"}, EXIT_VALIDATION_FAILED),
    ({"requirements_status": REQUIREMENTS_FAILED, "dags": [_dag("a.py", import_error="boom")]}, EXIT_REQUIREMENTS_FAILED),
])
def test_exit_code(kwargs, expected):
    """Requirements failure wins; any import error or top-level error is 1; warnings never count."""
    assert exit_code(_report(**kwargs)) == expected


@pytest.mark.parametrize("text,expected", [
    (None, None),
    ("", None),
    ("  \n", None),
    ("No module named 'x'", "No module named 'x'"),
    ("  indented only\n", "indented only"),
    (
        "Traceback (most recent call last):\n"
        '  File "/d/a.py", line 1, in <module>\n'
        "    import imp\n"
        "ModuleNotFoundError: No module named 'imp'\n",
        "ModuleNotFoundError: No module named 'imp'",
    ),
    (
        "Traceback (most recent call last):\n"
        '  File "/d/a.py", line 5, in <module>\n'
        "    session.query(DagRun).first()\n"
        'sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not translate host name "postgres"\n'
        "\n"
        "(Background on this error at: https://sqlalche.me/e/14/e3q8)\n",
        'sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not translate host name "postgres"',
    ),
])
def test_error_summary(text, expected):
    """The summary is the exception line, not trailing noise."""
    assert error_summary(text) == expected


def test_format_summary():
    """The stdout summary lists failures, then warnings, then the report path and exit code."""
    report = _report(
        plugin_import_errors=[{"file": "bad_plugin.py", "error": "No module named 'x'"}],
        dags=[
            _dag("good.py", ["good"]),
            _dag("imp_dag.py", import_error="ModuleNotFoundError: No module named 'imp'"),
            _dag("old.py", ["old"], warnings=[_warning("DeprecatedImportWarning"), _warning("FutureWarning")]),
        ],
    )
    assert format_summary(report, "/r.json") == "\n".join([
        "test-dags: Airflow 3.3.1, Python 3.12.16",
        "requirements: ok",
        "plugins: 1 import error",
        "  bad_plugin.py: No module named 'x'",
        "dags: 3 files, 2 DAGs, 1 import error, 2 warnings",
        "  FAIL imp_dag.py: ModuleNotFoundError: No module named 'imp'",
        "  WARN old.py: DeprecatedImportWarning, FutureWarning",
        "report: /r.json",
        "exit code: 1",
    ])


def test_format_summary_shows_requirement_and_top_level_errors():
    """A failed install and a parser error are both visible in the summary."""
    lines = format_summary(
        _report(requirements_status=REQUIREMENTS_FAILED, requirements_error="pip install failed", error="boom"),
        "/r.json",
    ).splitlines()
    assert "requirements: failed (pip install failed)" in lines
    assert "error: boom" in lines
    assert "plugins: 0 import errors" in lines
    assert lines[-1] == "exit code: 2"


def test_format_summary_explains_no_dag_files():
    """When parsing found no files, the summary says why that can happen."""
    lines = format_summary(_report(), "/r.json", "/home/me/dags").splitlines()
    assert lines[lines.index("dags: 0 files, 0 DAGs, 0 import errors, 0 warnings") + 1] == "  " + no_dag_files_hint("/home/me/dags")
    assert "No DAG files found in /home/me/dags." in no_dag_files_hint("/home/me/dags")


@pytest.mark.parametrize("fields", [
    {"dags": [{"file": "good.py", "dag_ids": ["good"], "import_error": None, "import_error_traceback": None, "warnings": []}]},
    {"requirements_status": REQUIREMENTS_FAILED, "requirements_error": "pip install failed"},
    {"error": "DAG parser exited with code 1"},
])
def test_format_summary_has_no_hint_when_files_were_found_or_parsing_did_not_run(fields):
    """The hint appears only when parsing ran and found nothing."""
    assert "No DAG files found" not in format_summary(_report(**fields), "/r.json", "/home/me/dags")
