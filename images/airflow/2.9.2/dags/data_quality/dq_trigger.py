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

interval=timedelta(minutes=5)

sql_dq_complete="""SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END dq_complete
            FROM AI_DATAMART.DM.SCHEMA_INFO a
            WHERE is_active='Y'
            AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log
                            WHERE model_name='DQ'
                            AND client=a.schema_name
                            AND in_ts=CURRENT_DATE 
                            AND status='COMPLETED'
            )"""
sql_src_data_present="""SELECT COUNT(*)
            FROM 
            ( 
                SELECT b.schema_name, SUM(NVL(cnt, 0)) CNT
                FROM 
                ( 
                    SELECT a.table_name, a.dq_dt_col, c.schema_name
                    FROM AI_DATAMART.DM.DQ_TABLE_INFO a, AI_DATAMART.DM.SCHEMA_INFO c
                    WHERE c.is_active='Y'
                ) b, (SELECT * FROM GALILEO_RAW.CORE.DQ_DMS_QUERIES_RESULT
                        WHERE dt=CURRENT_DATE-1 ) d
                WHERE b.table_name = d.source_table(+)
                AND b.schema_name = d.core(+)
                AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log
                            WHERE model_name='DQ'
                            AND client=b.schema_name
                            AND in_ts=CURRENT_DATE 
                            AND status='COMPLETED')
                GROUP BY b.schema_name
                HAVING SUM(NVL(cnt, 0))>0
            )"""
sql_dq_match="""SELECT count(*) 
            FROM (SELECT b.schema_name, SUM(NVL(d.diff_cnt, 1)) diff_cnt
                FROM (
                    SELECT a.table_name, a.dq_dt_col, c.schema_name
                    FROM AI_DATAMART.DM.DQ_TABLE_INFO a, AI_DATAMART.DM.SCHEMA_INFO c
                    WHERE c.is_active='Y'
                ) b, (SELECT * FROM AI_DATAMART.DM.DQ_SRC_TGT_RESULTS 
                        WHERE dq_date=CURRENT_DATE-1 ) d
                WHERE b.table_name = d.table_name(+)
                AND b.schema_name = d.schema_name(+)
                AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log
                            WHERE model_name='DQ'
                            AND client=b.schema_name
                            AND in_ts=CURRENT_DATE 
                            AND status='COMPLETED')
                GROUP BY b.schema_name
            ) e
            WHERE diff_cnt=0"""

sql_dq_3_retries="""SELECT COUNT(*) dq_retries
                FROM AI_DATAMART.AUDIT.dbt_results_log
                WHERE model_name='DQ'
                AND in_ts=CURRENT_DATE 
                AND status='COMPLETED'"""

def check_dq_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete)
    return result[0] > 0 # return true if query returns 1 else 0

def check_src_data_present():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_src_data_present)
    return result[0] > 0 # return true if query returns 1 else 0

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
        return 'exit_task'

def decide_next_task3(**kwargs):
    check_dq_match=kwargs['task_instance'].xcom_pull(task_ids='check_dq_match_task')
    if check_dq_match:
        return 'mark_dq_complete'
    else:
        return 'exit_task'

def check_3_retries():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_3_retries)
    count=result[0]
    if count > 3:
        logging.error(f"DQ has been re-tried {count} times.")

dag=DAG(
    dag_id="dq_completion_check",
    description="DAG in charge of checking if source & target data match",
    start_date= days_ago(1),
    schedule_interval='*/15 * * * *',
    catchup=False,
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

run_dbt_model = DbtRunOperator(
    task_id="run_dbt_model",
    project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
    threads=10,
    target="test",
    select=["dq_src_tgt_results"],
    dag=dag,
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
exit_task=PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("Exiting"),
    dag=dag
)

mark_dq_complete = SnowflakeOperator(
    task_id='mark_dq_complete',
    sql="""INSERT INTO AI_DATAMART.AUDIT.dbt_results_log (client, in_ts, model_name, status, start_ts, end_ts)
            SELECT schema_name, CURRENT_DATE, 'DQ', 'COMPLETED', CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0)
            FROM (SELECT b.schema_name, SUM(NVL(d.diff_cnt, 1)) diff_cnt
                FROM (
                    SELECT a.table_name, a.dq_dt_col, c.schema_name
                    FROM AI_DATAMART.DM.DQ_TABLE_INFO a, AI_DATAMART.DM.SCHEMA_INFO c
                    WHERE c.is_active='Y'
                ) b, (SELECT * FROM AI_DATAMART.DM.DQ_SRC_TGT_RESULTS 
                        WHERE dq_date=CURRENT_DATE-1 ) d
                WHERE b.table_name = d.table_name(+)
                AND b.schema_name = d.schema_name(+)
                AND NOT EXISTS (SELECT * FROM AI_DATAMART.AUDIT.dbt_results_log
                            WHERE model_name='DQ'
                            AND client=b.schema_name
                            AND in_ts=CURRENT_DATE 
                            AND status='COMPLETED')
                GROUP BY b.schema_name
            ) e
            WHERE diff_cnt=0""",
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)


check_dq_complete_task >> decide_next_task1_task
decide_next_task1_task >> [check_src_data_present_task, exit_task]
check_src_data_present_task >> decide_next_task2_task
decide_next_task2_task >> [run_dbt_model, exit_task]
run_dbt_model >> check_dq_match_task >> decide_next_task3_task
decide_next_task3_task >> [mark_dq_complete, exit_task]
check_3_retries_task
