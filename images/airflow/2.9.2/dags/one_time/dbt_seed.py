from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow_dbt_python.operators.dbt import DbtSeedOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime, timedelta
from typing import Dict
from pendulum import datetime
from snowflake import connector as sfconn
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'

dag=DAG(
    dag_id="dbt_seed",
    description="DAG to run dbt seeds",
    start_date= days_ago(1),
    schedule_interval=None,
)

dbt_seed = DbtSeedOperator(
    task_id="dbt_seed",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    dag=dag,
)

dbt_seed
