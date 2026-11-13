"""Fixtures for the mwaa.validation tests."""

import sys

import pytest

GOOD_DAG_SOURCE = '''"""A plain airflow DAG with one task."""

import pendulum

try:
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task

with DAG(dag_id="good", start_date=pendulum.datetime(2024, 1, 1), schedule=None):

    @task
    def noop():
        """Do nothing."""

    noop()
'''


@pytest.fixture
def dags_folder(tmp_path, monkeypatch):
    """Return an empty DAGs folder on sys.path; forget modules imported from it afterwards."""
    folder = tmp_path / "dags"
    folder.mkdir()
    monkeypatch.syspath_prepend(str(folder))
    yield folder
    for name, module in list(sys.modules.items()):
        if (getattr(module, "__file__", None) or "").startswith(str(folder)):
            del sys.modules[name]


@pytest.fixture
def write_dag(dags_folder):
    """Return a function that writes a file into the DAGs folder and returns its path."""

    def write(name, source):
        path = dags_folder / name
        path.write_text(source)
        return str(path)

    return write


@pytest.fixture
def good_dag_source():
    """Return the source of a DAG that parses without warnings on every Airflow version."""
    return GOOD_DAG_SOURCE
