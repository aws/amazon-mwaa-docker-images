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

sql_dq_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.dbt_results_log
                    WHERE model_name='DQ_PCI'
                    AND UPPER(schema_name)= 'IVR'
                    AND start_ts>=CURRENT_DATE 
                    AND status='COMPLETED'
                """

sql_src_data_present="""
                    SELECT CASE WHEN SUM(CASE WHEN cnt IS NULL THEN 1 ELSE 0 END)=0 THEN 1 ELSE 0 END
                    FROM 
                    ( 
                        SELECT table_name
                        FROM AI_DATAMART.DM.DQ_TABLES
                        WHERE schema_type = (SELECT schema_type FROM AI_DATAMART.DM.SCHEMA_INFO WHERE schema_name='IVR')
                    ) a, 
                    (
                        SELECT * FROM GALILEO_RAW.CORE.DQ_DMS_QUERIES_RESULT
                        WHERE dt=CURRENT_DATE-1 
                        AND UPPER(core) = 'IVR'
                    ) b
                    WHERE a.table_name = b.source_table(+)
                """
sql_dq_match="""
                SELECT CASE WHEN SUM(NVL(b.diff_cnt, 1)) = 0 AND SUM(NVL(b.diff_amt, 1)) = 0 THEN 1 ELSE 0 END dq_pass
                FROM 
                (
                    SELECT table_name
                    FROM AI_DATAMART.DM.DQ_TABLES
		    WHERE schema_type = (SELECT schema_type FROM AI_DATAMART.DM.SCHEMA_INFO WHERE schema_name='IVR')
                ) a, 
                (
                    SELECT * FROM AI_DATAMART.DM.DQ_SRC_TGT_RESULTS_PCI 
                    WHERE dq_date=CURRENT_DATE-1
                    AND UPPER(schema_name) = 'IVR'
                ) b
                WHERE a.table_name = b.table_name(+)
                """

sql_dq_3_retries="""SELECT COUNT(*) dq_retries
                FROM AI_DATAMART.AUDIT.dbt_results_log a
                WHERE model_name='dq_src_tgt_results_pci'
                AND start_ts>=CURRENT_DATE
                AND UPPER(schema_name) = 'IVR'
                AND status='COMPLETED'
                AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log a
                    WHERE model_name='DQ_PCI'
                    AND start_ts>=CURRENT_DATE
                    AND UPPER(schema_name) = 'IVR'
                    AND status='COMPLETED')
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
        end_date =  get_datetime(yesterday.strftime('%Y-%m-%d 23:59:59'))
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

def check_src_data_present():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_src_data_present)
    return result[0] > 0 # return true if query returns 1 else 0

def check_2am():
    now=datetime.now(local_tz)
    hr=now.hour
    if hr > 1:
        logging.error(f"No source DQ data for IVR in GALILEO_RAW.CORE.DQ_DMS_QUERIES_RESULT at this time.")

def check_dq_match():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_match)
    return result[0] > 0 # return true if query returns 1 else 0

def decide_next_task1(**kwargs):
    dq_complete=kwargs['task_instance'].xcom_pull(task_ids='check_dq_complete_task')
    if dq_complete:
        return 'exit_task'
    else:
        return 'check_src_data_present_task'

def decide_next_task2(**kwargs):
    src_data_present=kwargs['task_instance'].xcom_pull(task_ids='check_src_data_present_task')
    if src_data_present:
        return 'run_dbt_model'
    else:
        return 'check_2am_task'

def decide_next_task3(**kwargs):
    check_dq_match=kwargs['task_instance'].xcom_pull(task_ids='check_dq_match_task')
    if check_dq_match:
        return 'mark_dq_complete'
    else:
        return 'check_3_retries_task'

def check_3_retries():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_3_retries)
    count=result[0]
    if count > 3:
        logging.error(f"DQ has been re-tried {count} times for IVR. Source-target numbers don't match.")

dag=DAG(
    default_args=default_args,
    dag_id="dq_check_IVR_PCI",
    description="PCI DQ DAG for IVR",
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),
    schedule_interval="6-59/15 1-15 * * *",
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

check_src_data_present_task=PythonOperator(
    task_id='check_src_data_present_task',
    python_callable=check_src_data_present,
    dag=dag
)

decide_next_task1_task=BranchPythonOperator(
    task_id='decide_next_task1_task',
    python_callable=decide_next_task1,
    provide_context=True,
    dag=dag
)

decide_next_task2_task=BranchPythonOperator(
    task_id='decide_next_task2_task',
    python_callable=decide_next_task2,
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
            "source_schema":"IVR", \
            "target_schema":"DM", \
                "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
            }' \
        --select "dq_src_tgt_results_pci"
    """,
    dag=dag,
    priority_weight = 4
)

decide_next_task3_task=BranchPythonOperator(
    task_id='decide_next_task3_task',
    python_callable=decide_next_task3,
    provide_context=True,
    dag=dag
)

check_dq_match_task=PythonOperator(
    task_id='check_dq_match_task',
    python_callable=check_dq_match,
    dag=dag
)

check_3_retries_task=PythonOperator(
    task_id='check_3_retries_task',
    python_callable=check_3_retries,
    dag=dag
)

check_2am_task=PythonOperator(
    task_id='check_2am_task',
    python_callable=check_2am,
    dag=dag
)

exit_task=PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("Exiting"),
    dag=dag
)

mark_dq_complete = SnowflakeOperator(
    task_id='mark_dq_complete',
    sql="""INSERT INTO AI_DATAMART.AUDIT.dbt_results_log (log_id, schema_name, model_name, status, start_ts, end_ts)
            VALUES (AI_DATAMART.AUDIT.dbt_results_log_seq.nextval, 'IVR', 'DQ_PCI', 'COMPLETED', CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0))
        """,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)

get_dates_task >> check_dq_complete_task
check_dq_complete_task >> decide_next_task1_task
decide_next_task1_task >> [check_src_data_present_task, exit_task]
check_src_data_present_task >> decide_next_task2_task
decide_next_task2_task >> [run_dbt_model, check_2am_task]
run_dbt_model >> check_dq_match_task >> decide_next_task3_task
decide_next_task3_task >> [mark_dq_complete, check_3_retries_task]
