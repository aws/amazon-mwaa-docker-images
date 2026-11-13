"""
Build the test-dags report and decide the exit code.

This module imports nothing from Airflow, so it can be used and tested on its own.
The field names are a contract with callers that parse the report. Add fields only;
never rename or remove one without bumping SCHEMA_VERSION.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

SCHEMA_VERSION = 1

EXIT_OK = 0
EXIT_VALIDATION_FAILED = 1
EXIT_REQUIREMENTS_FAILED = 2

REQUIREMENTS_OK = "ok"
REQUIREMENTS_FAILED = "failed"
REQUIREMENTS_SKIPPED = "skipped"

PARSE_MODE_FORK = "fork"
PARSE_MODE_SUBPROCESS = "subprocess"


def error_summary(text: Optional[str]) -> Optional[str]:
    """
    Return the line that names the exception in an error message or traceback.

    For a traceback this is the first unindented line after the last stack frame,
    for example ``ModuleNotFoundError: No module named 'imp'``. A message that ends
    with extra lines, like SQLAlchemy's "Background on this error" link, still
    yields the exception line. Text without stack frames yields its first
    non-empty line.

    :param text: An error message or a formatted traceback.
    :returns: The summary line, or None if the text is empty.
    """
    if not text or not text.strip():
        return None
    lines = text.strip().splitlines()
    frame_indexes = [i for i, line in enumerate(lines) if line.startswith("  File ")]
    start = frame_indexes[-1] + 1 if frame_indexes else 0
    for line in lines[start:]:
        if line.strip() and not line[0].isspace():
            return line.strip()
    return next(line.strip() for line in reversed(lines) if line.strip())


def compute_summary(
    plugin_import_errors: List[Dict[str, Any]], dags: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Count files, DAGs, errors and warnings for the report's summary section.

    :param plugin_import_errors: The report's plugins.import_errors list.
    :param dags: The report's dags list.
    :returns: The summary dictionary.
    """
    return {
        "files": len(dags),
        "dags": sum(len(dag["dag_ids"]) for dag in dags),
        "import_errors": sum(1 for dag in dags if dag["import_error"]),
        "plugin_import_errors": len(plugin_import_errors),
        "warnings": sum(len(dag["warnings"]) for dag in dags),
    }


def build_report(
    *,
    airflow_version: str,
    python_version: str,
    requirements_status: str,
    requirements_error: Optional[str] = None,
    plugin_import_errors: Optional[List[Dict[str, Any]]] = None,
    dags: Optional[List[Dict[str, Any]]] = None,
    parse_mode: str = PARSE_MODE_FORK,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Assemble the report dictionary.

    :param airflow_version: The image's Airflow version.
    :param python_version: The image's Python version.
    :param requirements_status: One of REQUIREMENTS_OK, REQUIREMENTS_FAILED or
      REQUIREMENTS_SKIPPED.
    :param requirements_error: A short description of a requirements failure.
    :param plugin_import_errors: Entries with "file" and "error" keys.
    :param dags: One entry per DAG file, as produced by mwaa.validation.parse_dags.
    :param parse_mode: PARSE_MODE_FORK or PARSE_MODE_SUBPROCESS.
    :param error: A message when validation could not run to the end.
    :returns: The report, ready to be serialized as JSON.
    """
    plugin_errors = sorted(plugin_import_errors or [], key=lambda entry: entry["file"])
    dag_entries = sorted(dags or [], key=lambda entry: entry["file"])
    return {
        "schema_version": SCHEMA_VERSION,
        "airflow_version": airflow_version,
        "python_version": python_version,
        "error": error,
        "parse_mode": parse_mode,
        "requirements": {"status": requirements_status, "error": requirements_error},
        "plugins": {"import_errors": plugin_errors},
        "dags": dag_entries,
        "summary": compute_summary(plugin_errors, dag_entries),
    }


def exit_code(report: Dict[str, Any]) -> int:
    """
    Return the process exit code for a report. Warnings never change it.

    :param report: A report produced by build_report.
    :returns: EXIT_REQUIREMENTS_FAILED, EXIT_VALIDATION_FAILED or EXIT_OK.
    """
    if report["requirements"]["status"] == REQUIREMENTS_FAILED:
        return EXIT_REQUIREMENTS_FAILED
    if (
        report["error"]
        or report["plugins"]["import_errors"]
        or any(dag["import_error"] for dag in report["dags"])
    ):
        return EXIT_VALIDATION_FAILED
    return EXIT_OK


def _count(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


def format_summary(report: Dict[str, Any], report_path: str) -> str:
    """
    Format a short human-readable summary of a report for stdout.

    :param report: A report produced by build_report.
    :param report_path: Where the report was written.
    :returns: The summary text, without a trailing newline.
    """
    requirements = report["requirements"]
    summary = report["summary"]
    plugin_errors = report["plugins"]["import_errors"]
    lines = [f"test-dags: Airflow {report['airflow_version']}, Python {report['python_version']}"]
    requirements_line = f"requirements: {requirements['status']}"
    if requirements["error"]:
        requirements_line += f" ({requirements['error']})"
    lines.append(requirements_line)
    if report["error"]:
        lines.append(f"error: {report['error']}")
    lines.append(f"plugins: {_count(len(plugin_errors), 'import error')}")
    lines.extend(f"  {entry['file']}: {entry['error']}" for entry in plugin_errors)
    lines.append(
        f"dags: {_count(summary['files'], 'file')}, {_count(summary['dags'], 'DAG')}, "
        f"{_count(summary['import_errors'], 'import error')}, "
        f"{_count(summary['warnings'], 'warning')}"
    )
    for dag in report["dags"]:
        if dag["import_error"]:
            lines.append(f"  FAIL {dag['file']}: {dag['import_error']}")
    for dag in report["dags"]:
        if dag["warnings"]:
            categories = ", ".join(sorted({warning["category"] for warning in dag["warnings"]}))
            lines.append(f"  WARN {dag['file']}: {categories}")
    lines.append(f"report: {report_path}")
    lines.append(f"exit code: {exit_code(report)}")
    return "\n".join(lines)
