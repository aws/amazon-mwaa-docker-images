from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import DbtRunOperator

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
dag_name=Variable.get("dag_name")
core_name=Variable.get("core_name")

print ("model name:" + dag_name)
print ("core name:" + core_name)

dag=DAG(
    dag_id="dbt_run_op",
    description="Generic dag to run a dbt model",
    start_date= days_ago(1),
    schedule_interval=None,
    catchup=False,
)
#running onetime load for onedebit, updsted the core_name
run_dbt_model_task1 = DbtRunOperator(
    task_id="run_dbt_model_task1",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    select=["dim_account"],
    vars={"source_schema": "ONEDEBIT", "target_schema":"DM", "start_date":"2000-12-12", "end_date":"2024-02-04"},
    dag=dag,
)

run_dbt_model_task2 = DbtRunOperator(
    task_id="run_dbt_model_task2",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    select=["dim_account"],
    vars={"source_schema": "SOFI", "target_schema":"DM", "start_date":"2000-12-12", "end_date":"2024-02-04"},
    dag=dag,
)

run_dbt_model_task3 = DbtRunOperator(
    task_id="run_dbt_model_task3",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    select=["dim_account"],
    vars={"source_schema": "WYNDHAM", "target_schema":"DM", "start_date":"2000-12-12", "end_date":"2024-02-04"},
    dag=dag,
)

run_dbt_model_task4 = DbtRunOperator(
    task_id="run_dbt_model_task4",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    select=["dq_src_tgt_results"],
    vars={"core_schema": "BAMBU","source_schema": "BAMBU", "target_schema":"DM", "start_date":"2025-02-26", "end_date":"2025-02-27", "is_first_time_run":"Y"},
    dag=dag,
)

# RUNNING ONE TIME SETTLEMENT FILE
#run_dbt_model_task1 >> run_dbt_model_task2>> run_dbt_model_task3
run_dbt_model_task4