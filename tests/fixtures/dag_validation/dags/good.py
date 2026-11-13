"""A plain DAG with one task. It parses without warnings on every Airflow version here."""

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
