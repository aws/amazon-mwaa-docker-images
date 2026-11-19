"""A DAG file that imports BashOperator from its Airflow 2 module path."""

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="old_import_dag", start_date=pendulum.datetime(2024, 1, 1), schedule=None):
    BashOperator(task_id="echo", bash_command="echo hello")
