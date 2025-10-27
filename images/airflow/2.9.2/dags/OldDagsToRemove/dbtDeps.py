from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    #DbtRunOperator,
    #DbtSeedOperator,
    DbtDepsOperator
)

from airflow.operators.bash import BashOperator

with DAG(
    'dbtDeps',
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Load dbt Deps',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'WIP'],
) as dag:
    dbt_deps = DbtDepsOperator(
        task_id="dbt_deps",
        project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        target="test",
        profile="system_test-analytics_airflow_dbt"
    )
    success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
    )

    dbt_deps >> success 
    