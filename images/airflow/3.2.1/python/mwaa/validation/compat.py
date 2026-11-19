"""
Airflow 3.2.1 specifics for the test-dags command.

Everything in mwaa.validation that depends on the Airflow version lives in this
module. The other modules are the same in every image.
"""

from __future__ import annotations

from typing import Any, Dict, List

from airflow import plugins_manager
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.dag_processing.importers.base import get_importer_registry

# Modules Airflow's DAG processor has imported before it parses a file.
WARM_UP_MODULES = ["airflow.models.baseoperator", "airflow.models.dag", "airflow.sdk"]


def warm_up_providers() -> Dict[str, Any]:
    """
    Load the providers' task decorators, as the first ``@task`` in a DAG file does.

    Airflow loads them lazily, so without this each forked worker would load them again
    while it parses its DAG file, and their import-time warnings would be charged to it.

    :returns: The task decorators, by name.
    """
    from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime

    return ProvidersManagerTaskRuntime().taskflow_decorators


def safe_mode() -> bool:
    """Return Airflow's [core] dag_discovery_safe_mode setting."""
    return conf.getboolean("core", "dag_discovery_safe_mode")


def list_dag_files(dags_folder: str) -> List[str]:
    """
    List the files Airflow would parse in dags_folder, honoring .airflowignore.

    :param dags_folder: The DAGs folder.
    """
    return sorted(get_importer_registry().list_dag_files(dags_folder, safe_mode=safe_mode()))


def make_dagbag(dag_file: str) -> DagBag:
    """
    Create an empty DagBag for one file; the caller runs process_file.

    :param dag_file: The DAG file.
    """
    return DagBag(dag_folder=dag_file, include_examples=False, collect_dags=False)


def plugin_import_errors() -> Dict[str, str]:
    """Load plugins and return their import errors, keyed by file or module."""
    plugins_manager.ensure_plugins_loaded()
    return dict(plugins_manager.get_import_errors())


def reset_orm_after_fork() -> None:
    """
    Do nothing: Airflow 3 registers an at-fork hook that disposes the ORM engine.

    See ``airflow.settings.configure_orm``.
    """


def resolve_category(
    category: type, filename: str, lineno: int, message: str, dagbag: Any, dag_file: str
) -> str:
    """
    Return the short class name of a recorded warning.

    In this version the DAG importer records each warning's real class only as text in
    ``DagBag.captured_warnings``, in the form ``"{path}:{line}: {type}: {message}"``,
    and re-emits the warning as UserWarning. For a recorded UserWarning, find the
    entry with the same path, line and message and take the type from it. The prefix
    and suffix are matched exactly, so colons in paths or messages do not matter.
    """
    if category is not UserWarning or dagbag is None:
        return category.__name__
    line_numbers = [str(lineno)] + (["None"] if lineno == 0 else [])
    suffix = f": {message}"
    for entry in dagbag.captured_warnings.get(dag_file, ()):
        for line_number in line_numbers:
            prefix = f"{filename}:{line_number}: "
            if (
                entry.startswith(prefix)
                and entry.endswith(suffix)
                and len(entry) > len(prefix) + len(suffix)
            ):
                return entry[len(prefix) : len(entry) - len(suffix)].rsplit(".", 1)[-1]
    return category.__name__
