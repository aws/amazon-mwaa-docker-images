from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtSeedOperator
)

from airflow.operators.bash import BashOperator

with DAG(
    'dbtSeed', 
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Load seeds',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'WIP'],
) as dag:
    dbt_seed = DbtSeedOperator(
        task_id="dbt_seed",
        project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profile="system_test-analytics_airflow_dbt",
        target="test",
        vars={"source_schema":"CHM4", "target_schema":"DM", "start_date":"2023-02-21", "end_date":"2023-02-21"},
    )
    success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
    )

    dbt_seed >> success 



    # dummy comment as the --size-only arg on aws s3 sync --dryrun --delete --size-only is not taking pushing the changes if the size didn't change