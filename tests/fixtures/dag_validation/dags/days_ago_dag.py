"""A DAG file that uses days_ago, deprecated in Airflow 2 and removed in Airflow 3."""

from airflow.utils.dates import days_ago

try:
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task

with DAG(dag_id="days_ago_dag", start_date=days_ago(1), schedule=None):

    @task
    def noop():
        """Do nothing."""

    noop()
