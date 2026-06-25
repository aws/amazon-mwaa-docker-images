"""A sample DAG."""

# Python imports
from datetime import datetime, timedelta

# Airflow imports.
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="hello_world_dag",
    schedule=timedelta(minutes=1),
    dagrun_timeout=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    @task(task_id="print_task")
    def hello_world() -> None:
        """print_task prints a Hello World message."""
        print("Hello, World!")

    hello_world()


if __name__ == "__main__":
    dag.cli()
