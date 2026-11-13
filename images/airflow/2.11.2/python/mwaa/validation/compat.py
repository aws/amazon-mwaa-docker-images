"""
Airflow 2.11.2 specifics for the test-dags command.

Everything in mwaa.validation that depends on the Airflow version lives in this
module. The other modules are the same in every image.
"""

from __future__ import annotations

from typing import Any, Dict, List

from airflow import plugins_manager, settings
from airflow.configuration import conf
from airflow.models.dagbag import DagBag
from airflow.utils.file import list_py_file_paths

# Modules Airflow's DAG processor has imported before it parses a file.
WARM_UP_MODULES = ["airflow.models.baseoperator", "airflow.models.dag"]


def safe_mode() -> bool:
    """Return Airflow's [core] dag_discovery_safe_mode setting."""
    return conf.getboolean("core", "dag_discovery_safe_mode")


def list_dag_files(dags_folder: str) -> List[str]:
    """
    List the files Airflow would parse in dags_folder, honoring .airflowignore.

    :param dags_folder: The DAGs folder.
    """
    return sorted(
        list_py_file_paths(dags_folder, safe_mode=safe_mode(), include_examples=False)
    )


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
    """Give a forked worker its own ORM engine, as Airflow 2's DagFileProcessor does."""
    settings.configure_orm()


def resolve_category(
    category: type, filename: str, lineno: int, message: str, dagbag: Any, dag_file: str
) -> str:
    """
    Return the short class name of a recorded warning.

    DagBag in this version records warnings with their real class, so the recorded
    category is already correct.
    """
    return category.__name__
