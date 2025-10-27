from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtTestOperator
)

from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    "bi_test_operator",
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Tests on RDF AUTH Model in System Test environment.',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'WIP'],
) as dag:
    dbt_test_op = DbtTestOperator(
        task_id="dbt_test_op",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    #    project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #    profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        target="test",
        vars={"source_schema":"SOFI", "target_schema":"DM", "start_date":"2023-02-21", "end_date":"2023-02-21"}
   )
    success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
    )

    dbt_test_op >> success 