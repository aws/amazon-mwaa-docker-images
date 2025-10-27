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
}

snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'

sql_dq_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.dbt_results_log
                    WHERE model_name='DQ'
                    AND UPPER(schema_name)= 'IVR'
                    AND start_ts>=CURRENT_DATE 
                    AND status='COMPLETED'
                """

sql_src_data_present="""
                    SELECT CASE WHEN SUM(CASE WHEN cnt IS NULL THEN 1 ELSE 0 END)=0 THEN 1 ELSE 0 END
                    FROM 
                    ( 
                        SELECT table_name
                        FROM AI_DATAMART.DM.DQ_TABLE_INFO
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
                    FROM AI_DATAMART.DM.DQ_TABLE_INFO
		    WHERE schema_type = (SELECT schema_type FROM AI_DATAMART.DM.SCHEMA_INFO WHERE schema_name='IVR')
                ) a, 
                (
                    SELECT * FROM AI_DATAMART.DM.DQ_SRC_TGT_RESULTS 
                    WHERE dq_date=CURRENT_DATE-1
                    AND UPPER(schema_name) = 'IVR'
                ) b
                WHERE a.table_name = b.table_name(+)
                """

sql_dq_3_retries="""SELECT COUNT(*) dq_retries
                FROM AI_DATAMART.AUDIT.dbt_results_log a
                WHERE model_name='dq_src_tgt_results'
                AND start_ts>=CURRENT_DATE
                AND UPPER(schema_name) = 'IVR'
                AND status='COMPLETED'
                AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log a
                    WHERE model_name='DQ'
                    AND start_ts>=CURRENT_DATE
                    AND UPPER(schema_name) = 'IVR'
                    AND status='COMPLETED')
                """

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
    dag_id="dq_check_IVR",
    description="DQ DAG for IVR",
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),
    schedule_interval="6-59/15 1-15 * * *",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['BI','DQ'],
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

# Run dq_src_tgt_results for the specific client and date range
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
        --vars '{"core_name":"IVR"}' \
        --select "dq_src_tgt_results"
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
            VALUES (AI_DATAMART.AUDIT.dbt_results_log_seq.nextval, 'IVR', 'DQ', 'COMPLETED', CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0))
        """,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)

check_dq_complete_task >> decide_next_task1_task
decide_next_task1_task >> [check_src_data_present_task, exit_task]
check_src_data_present_task >> decide_next_task2_task
decide_next_task2_task >> [run_dbt_model, check_2am_task]
run_dbt_model >> check_dq_match_task >> decide_next_task3_task
decide_next_task3_task >> [mark_dq_complete, check_3_retries_task]
