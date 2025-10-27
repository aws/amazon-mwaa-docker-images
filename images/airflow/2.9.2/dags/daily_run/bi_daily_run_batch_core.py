from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
from typing import Dict
from snowflake import connector as sfconn
import os
import pendulum
import logging

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")
snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'
#getting count of clients cores that are active in schema_info and do not dq failures in galileo_raw or secure_db for today
sql_dq_complete=""" SELECT COUNT(*) FROM 
            (select * 
            from ai_datamart.dm.schema_info s
            where batch_id = 'core' and is_active = 'Y'
            and schema_name not in
                (
                select distinct d.schema_name
                from ai_datamart.dm.vw_dq_schema_results_today d
                where d.schema_name = s.schema_name
                and is_dq_success = 'N'
                )
            )
                """
sql_shared_models_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.sys_load_status
                    WHERE UPPER(schema_name)= 'SHARED'
                    AND start_time>=CURRENT_DATE 
                    AND status='COMPLETED'
                """
sql_daily_models_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.sys_load_status
                    WHERE BATCH_ID='core'
                    AND start_time>=CURRENT_DATE 
                    AND status='COMPLETED'
                """


sql_insert_load_start="""insert into AI_DATAMART.AUDIT.sys_load_status(load_type, batch_id, schema_name, run_dt, start_time)
                select 'DAILY_ETL', batch_id, schema_name, current_date-1 as run_dt, current_timestamp as start_time 
                from ai_datamart.dm.schema_info s
                where batch_id = 'core' and is_active = 'Y'
                and schema_name not in
                (
                    select distinct d.schema_name
                    from ai_datamart.dm.vw_dq_schema_results_today d
                    where d.schema_name = s.schema_name
                    and is_dq_success = 'N'
                )
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

def check_dq_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete)
    return result[0] > 0 # return true if query returns 1 else 0

def check_shared_models_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_shared_models_complete)
    return result[0] > 0

def insert_daily_load_start():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    snowflake_hook.get_first(sql_insert_load_start)
    return 1

def check_daily_models_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_daily_models_complete)
    return result[0] > 0

def decide_next_step3(**kwargs):
    models_complete=kwargs['task_instance'].xcom_pull(task_ids='check_dq_complete_task')
    if models_complete:
        return 'insert_daily_load_start_task'
    else:
        return 'exit_task'

def decide_next_step2(**kwargs):
    models_complete=kwargs['task_instance'].xcom_pull(task_ids='check_shared_models_complete_task')
    if models_complete:
        return 'check_dq_complete_task'
    else:
        return 'exit_task'

def decide_next_step1(**kwargs):
    models_complete=kwargs['task_instance'].xcom_pull(task_ids='check_daily_models_complete_task')
    if models_complete:
        return 'exit_task'
    else:
        return 'check_shared_models_complete_task'

def decide_next_step4(**kwargs):
    models_complete=kwargs['task_instance'].xcom_pull(task_ids='insert_daily_load_start_task')
    if models_complete:
        return 'daily_models'
    else:
        return 'exit_task'    


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

with DAG(
    "bi_daily_run_batch_core",
    default_args = {
        'owner': 'BI',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    },
    description='Runs BI daily ELT loads for Batch Core.',
    schedule_interval= "14-59/15 1-15 * * *",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    start_date=datetime(2025, 4, 1, tzinfo=local_tz),
    tags=['BI','DAILY_RUN','TEMPLATE'],
) as dag:

    get_dates_task = PythonOperator(
        task_id='get_running_dates',
        python_callable=get_running_dates,
        provide_context=True,
        dag=dag
    )

    # Sync files from S3 to worker and then execute dbt
    daily_models = BashOperator(
        task_id='daily_models',
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
                "batch_id":"core", \
                "target_schema":"DM", \
                "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
                }' \
            --select +tag:"daily"\
            --exclude tag:"deprecated" tag:"galileo" tag:"dq_monthly_test"
        """,
        dag=dag,
        priority_weight = 4
    )

    # Data tests for daily models tag:daily (exclude deprecated and galileo)
    data_tests = BashOperator(
        task_id='data_tests',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        # Run dbt for specific client
        dbt test \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --threads 10 \
            --vars '{\
                "batch_id":"core", \
                "target_schema":"DM", \
                "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
                }' \
            --select +tag:"daily"\
            --exclude tag:"deprecated" tag:"galileo" tag:"dq_monthly_test"
        """,
        dag=dag,
        priority_weight = 4
    )
    
    check_dq_complete_task=PythonOperator(
    task_id='check_dq_complete_task',
    python_callable=check_dq_complete,
    dag=dag
    )

    check_shared_models_complete_task=PythonOperator(
    task_id='check_shared_models_complete_task',
    python_callable=check_shared_models_complete,
    dag=dag
    )

    check_daily_models_complete_task=PythonOperator(
    task_id='check_daily_models_complete_task',
    python_callable=check_daily_models_complete,
    dag=dag
    )

    insert_daily_load_start_task=PythonOperator(
    task_id='insert_daily_load_start_task',
    python_callable=insert_daily_load_start,
    dag=dag
    )

    decide_next_step_task1=BranchPythonOperator(
    task_id='decide_next_step_task1',
    python_callable=decide_next_step1,
    provide_context=True,
    dag=dag
    )

    decide_next_step_task2=BranchPythonOperator(
    task_id='decide_next_step_task2',
    python_callable=decide_next_step2,
    provide_context=True,
    dag=dag
    )

    decide_next_step_task3=BranchPythonOperator(
    task_id='decide_next_step_task3',
    python_callable=decide_next_step3,
    provide_context=True,
    dag=dag
    )

    decide_next_step_task4=BranchPythonOperator(
    task_id='decide_next_step_task4',
    python_callable=decide_next_step4,
    provide_context=True,
    dag=dag
    )
    
    exit_task=PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("Exiting"),
    dag=dag
    )

    mark_daily_models_complete = SnowflakeOperator(
    task_id='mark_daily_models_complete',
    sql="""UPDATE ai_datamart.audit.sys_load_status s
            SET status = 'COMPLETED', end_time = CURRENT_TIMESTAMP(0)
            where schema_name NOT IN
                (
                    select distinct schema_name from
                    ai_datamart.dm.dq_integration_tests_results
                    where (diff_cnt<>0 or diff_amt<>0)
                    and run_date >= current_date
                    and dq_start_date = current_date-1
                    and dq_end_date = current_date
                ) 
            AND start_time>=current_date
            AND BATCH_ID='core'
        """,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)
    
    # Non-production environment ETL2 status is not updated to failed based on dq_integration_tests_results
    mark_daily_core_models_failure = SnowflakeOperator(
    task_id='mark_daily_core_models_failure',
    sql="""UPDATE ai_datamart.audit.sys_load_status s
            -- SET status = 'FAILED', end_time = CURRENT_TIMESTAMP(0)
            SET status = '{% if var.value.environment_name.lower() == 'production' %}FAILED{% else %}COMPLETED{% endif %}', end_time = CURRENT_TIMESTAMP(0)
            where schema_name IN
                (
                    select distinct schema_name from
                    ai_datamart.dm.dq_integration_tests_results
                    where (diff_cnt<>0 or diff_amt<>0)
                    and run_date >= current_date
                    and dq_start_date = current_date-1
                    and dq_end_date = current_date
                ) 
            AND start_time>=current_date
            AND BATCH_ID='core'
        """,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)

    get_dates_task >> check_daily_models_complete_task
    check_daily_models_complete_task >> decide_next_step_task1
    decide_next_step_task1 >> [check_shared_models_complete_task,exit_task]
    check_shared_models_complete_task >> decide_next_step_task2
    decide_next_step_task2 >> [check_dq_complete_task,exit_task]
    check_dq_complete_task >> decide_next_step_task3
    decide_next_step_task3 >> [insert_daily_load_start_task,exit_task]
    insert_daily_load_start_task >> decide_next_step_task4
    decide_next_step_task4 >> [daily_models,exit_task]
    daily_models >> mark_daily_models_complete >> mark_daily_core_models_failure >> data_tests
    