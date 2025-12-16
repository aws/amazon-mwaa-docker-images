"""A sample DAG."""

# Python imports
import logging
from datetime import datetime, timedelta

# Airflow imports.
from airflow import DAG
from airflow.decorators import task

logger = logging.getLogger(__name__)

with DAG(
    dag_id="hello_world_dag",
    schedule=timedelta(minutes=1),
    dagrun_timeout=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    @task(task_id="print_task")
    def hello_world() -> str:
        """print_task prints a Hello World message."""
        message = "Hello, World!"
        logger.info(message)
        return message

    hello_world()


if __name__ == "__main__":
    dag.cli()
