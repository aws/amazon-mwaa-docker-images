"""A DAG file that queries the Airflow metadata database while it is parsed."""

import pendulum
from airflow import settings
from airflow.models import DagRun

try:
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task

session = settings.Session()
LATEST_RUN = session.query(DagRun).first()

with DAG(dag_id="db_at_parse", start_date=pendulum.datetime(2024, 1, 1), schedule=None):

    @task
    def noop():
        """Do nothing."""

    noop()
