from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtRunOperationOperator
)

from airflow.operators.bash import BashOperator

with DAG(
    'dbt_Run_Operator', 
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Create procedure',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'WIP'],
) as dag:
    dbt_run_op = DbtRunOperationOperator(
        task_id="dbt_run_op",
        project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profile="system_test-analytics_airflow_dbt",
        target="test",
        vars={"source_schema":"CHM4", "target_schema":"DM", "start_date":"2023-02-21", "end_date":"2023-02-21"},
        macro='test_proc',
    )
    success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
    )

    dbt_run_op >> success 



    # dummy comment as the --size-only arg on aws s3 sync --dryrun --delete --size-only is not taking pushing the changes if the size didn't change