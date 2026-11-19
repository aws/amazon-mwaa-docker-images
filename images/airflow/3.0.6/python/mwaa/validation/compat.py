"""
Airflow 3.0.6 specifics for the test-dags command.

Everything in mwaa.validation that depends on the Airflow version lives in this
module. The other modules are the same in every image.
"""

from __future__ import annotations

from typing import Any, Dict, List

from airflow import plugins_manager
from airflow.configuration import conf
from airflow.models.dagbag import DagBag
from airflow.utils.file import list_py_file_paths

# Modules Airflow's DAG processor has imported before it parses a file.
WARM_UP_MODULES = ["airflow.models.baseoperator", "airflow.models.dag", "airflow.sdk"]


def warm_up_providers() -> Dict[str, Any]:
    """
    Load the providers' task decorators, as the first ``@task`` in a DAG file does.

    Airflow loads them lazily, so without this each forked worker would load them again
    while it parses its DAG file, and their import-time warnings would be charged to it.
    On Airflow 3.0.6 this also loads the provider hooks, which import flask_appbuilder and
    flask_sqlalchemy, so their Flask deprecation warnings would otherwise be charged to
    the first DAG file in each worker that uses ``@task``.

    :returns: The task decorators, by name.
    """
    from airflow.providers_manager import ProvidersManager

    return ProvidersManager().taskflow_decorators


def safe_mode() -> bool:
    """Return Airflow's [core] dag_discovery_safe_mode setting."""
    return conf.getboolean("core", "dag_discovery_safe_mode")


def list_dag_files(dags_folder: str) -> List[str]:
    """
    List the files Airflow would parse in dags_folder, honoring .airflowignore.

    :param dags_folder: The DAGs folder.
    """
    return sorted(list_py_file_paths(dags_folder, safe_mode=safe_mode()))


def make_dagbag(dag_file: str) -> DagBag:
    """
    Create an empty DagBag for one file; the caller runs process_file.

    :param dag_file: The DAG file.
    """
    return DagBag(dag_folder=dag_file, include_examples=False, collect_dags=False)


def plugin_import_errors() -> Dict[str, str]:
    """Load plugins and return their import errors, keyed by file or module."""
    plugins_manager.ensure_plugins_loaded()
    return dict(plugins_manager.import_errors)


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

    DagBag in this version records warnings with their real class, so the recorded
    category is already correct.
    """
    return category.__name__
