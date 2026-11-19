"""A DAG file that imports imp, which Python 3.12 removed."""

import imp  # noqa: F401

import pendulum

try:
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task

with DAG(dag_id="imp_dag", start_date=pendulum.datetime(2024, 1, 1), schedule=None):

    @task
    def noop():
        """Do nothing."""

    noop()
