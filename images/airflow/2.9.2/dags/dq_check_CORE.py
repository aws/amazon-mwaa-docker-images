from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow_dbt_python.operators.dbt import DbtRunOperator

from datetime import datetime, timedelta
from typing import Dict
from snowflake import connector as sfconn
import os
import pendulum
import logging

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
}

snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'

sql_dq_complete=""" SELECT COUNT(1) FROM 
                    (
                        SELECT 
                              COUNT(1) schema_cnt
                            , SUM(CASE WHEN is_dq_success='Y' THEN 1 ELSE 0 END) dq_success_schema_cnt
                            , SUM(CASE WHEN is_dq_success='Y' THEN 0 ELSE 1 END) dq_failure_schema_cnt
                            , LISTAGG(CASE WHEN is_dq_success='Y' THEN NULL ELSE schema_name END, ', ') pending_dq_schema_list
                        FROM AI_DATAMART.DM.vw_dq_schema_results_today 
                        WHERE 1=1
                        AND schema_type='CORE'
                    )
                    WHERE 1=1
                    AND dq_failure_schema_cnt = 0 -- Check if all schemas have passed DQ
                """

def is_date(string):
    try:
        datetime.strptime(string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def is_timestamp(string):
    try:
        datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
        return True
    except ValueError:
        return False

def get_datetime(StParam:str):
    DtParam = None
    if is_timestamp(StParam):
        DtParam = datetime.strptime(StParam, '%Y-%m-%d  %H:%M:%S')
    elif is_date(StParam):
        DtParam = datetime.strptime(StParam, '%Y-%m-%d')
    return DtParam

def get_running_dates(**kwargs):
    
    dag_run_conf = kwargs['dag_run'].conf
    default_args = kwargs['dag'].default_args

    params = {**default_args, **dag_run_conf} if dag_run_conf else default_args
    truncate = params.get('truncate', None)

    if not truncate:
        truncate = True
    else:
        if not isinstance(truncate, bool):
            truncate = not (truncate.lower() == 'false')
    
    start_date = params.get('start_date', None)
    if start_date:
        start_date = get_datetime(start_date)
    end_date = params.get('end_date', None)
    if end_date:
        end_date = get_datetime(end_date)
    running_date = get_datetime(params.get('running_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    if start_date == None and end_date == None:
        today = running_date
        yesterday = (running_date - timedelta(days=1))
        start_date = get_datetime(yesterday.strftime('%Y-%m-%d %H:%M:%S'))
        end_date =  get_datetime(today.strftime('%Y-%m-%d 23:59:59'))
    if start_date is not None and end_date is None:
        end_date =  get_datetime(start_date.strftime('%Y-%m-%d 23:59:59'))
    if truncate == True:
        start_date = get_datetime(start_date.strftime('%Y-%m-%d 00:00:00'))
        end_date = get_datetime(end_date.strftime('%Y-%m-%d 23:59:59'))
    current_date = datetime.now()

    kwargs['ti'].xcom_push(key='current_date', value=current_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='running_date', value=running_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='start_date', value=start_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='end_date', value=end_date.strftime('%Y-%m-%d'))

def check_dq_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete)
    return result[0] > 0 # return true if query returns 1 else 0

def decide_next_task1(**kwargs):
    dq_complete=kwargs['task_instance'].xcom_pull(task_ids='check_dq_complete_task')
    if dq_complete:
        return 'exit_task'
    else:
        return 'run_dbt_model'

dag=DAG(
    default_args=default_args,
    dag_id="dq_check_CORE",
    description="DQ DAG for All clients",
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),
    schedule_interval="5,20,35,50 1-15 * * *",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['BI','DQ'],
)

get_dates_task = PythonOperator(
    task_id='get_running_dates',
    python_callable=get_running_dates,
    provide_context=True,
    dag=dag
)

check_dq_complete_task=PythonOperator(
    task_id='check_dq_complete_task',
    python_callable=check_dq_complete,
    dag=dag
)

decide_next_task1_task=BranchPythonOperator(
    task_id='decide_next_task1_task',
    python_callable=decide_next_task1,
    provide_context=True,
    dag=dag
)

# Run dq_src_tgt_results_pci for the specific client and date range
run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command="""
    # Sync files from S3 to tmp directory
    aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
    # Navigate to dbt project directory
    cd /tmp/dbt_project &&
    # Run dbt for specific client
    dbt run \
        --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
        --project-dir /tmp/dbt_project \
        --profiles-dir /tmp/dbt_project \
        --target test \
        --threads 10 \
        --vars '{\
            "schema_type":"CORE", \
            "target_schema":"DM", \
                "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
            }' \
        --select "dq_src_tgt_results_multiple_clients"
    """,
    dag=dag,
    priority_weight = 4
)

exit_task=PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("Exiting"),
    dag=dag
)

get_dates_task >> check_dq_complete_task
check_dq_complete_task >> decide_next_task1_task
decide_next_task1_task >> [run_dbt_model, exit_task]

#Dummy change to trigger CI for schedule changes