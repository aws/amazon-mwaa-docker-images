from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'

# Define AWS Batch job parameters
submit_job = Variable.get("submit_job")
job_definition = Variable.get("snow_oracle_job_def") 
job_queue = Variable.get("snow_oracle_job_queue") 
rdf_files_s3 = Variable.get("dw_oracle_bucket") 
rdf_client = "121"

sql_ivr_daily_models_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.dbt_results_log
                    WHERE model_name='IVR_DAILY_MODELS'
                    AND UPPER(schema_name)= 'GENESYS'
                    AND start_ts>=CURRENT_DATE 
                    AND status='COMPLETED'
                """

sql_dq_complete=""" SELECT COUNT(*) FROM AI_DATAMART.AUDIT.dbt_results_log
                    WHERE model_name='DQ'
                    AND UPPER(schema_name)= 'GENESYS'
                    AND start_ts>=CURRENT_DATE 
                    AND status='COMPLETED'
                """
sql_ingest_update="""MERGE INTO AI_DATAMART.AUDIT.s3_upload_log AS l
        USING (
            SELECT 
            ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(p.file_name, '/'), 1, 255), '/') AS file_name,
            p.status AS ingest_status,
            p.row_count AS load_count,
            DATE_TRUNC('second', TO_TIMESTAMP_NTZ(p.last_load_time)) AS ingest_time
        FROM (
            SELECT *
            FROM table(galileo_raw.information_schema.copy_history(TABLE_NAME => 'GALILEO_RAW.GENESYS.IVR_USERS_AGGREGATES', START_TIME => DATEADD(days, -10, CURRENT_TIMESTAMP())))
            UNION ALL
            SELECT *
            FROM table(galileo_raw.information_schema.copy_history(TABLE_NAME => 'GALILEO_RAW.GENESYS.IVR_CONVERSATIONS', START_TIME => DATEADD(days, -10, CURRENT_TIMESTAMP())))
            UNION ALL
            SELECT *
            FROM table(galileo_raw.information_schema.copy_history(TABLE_NAME => 'GALILEO_RAW.GENESYS.IVR_PARTICIPANT_ATTRIBUTES', START_TIME => DATEADD(days, -10, CURRENT_TIMESTAMP())))
            ) p
    WHERE p.last_load_time >= DATEADD(days, -1, CURRENT_TIMESTAMP())
        ) AS src
        ON ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(l.s3_key, '/'), 2, 255), '/') = src.file_name
        WHEN MATCHED THEN
            UPDATE SET 
                l.ingest_Status = src.ingest_status,
                l.ingest_time = src.ingest_time,
                l.ingest_count = src.load_count"""

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

def check_ivr_daily_models_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_ivr_daily_models_complete)
    return result[0] > 0

def check_dq_complete():
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete)
    return result[0] > 0 # return true if query returns 1 else 0

def decide_next_step1(**kwargs):
    ivr_models_complete=kwargs['task_instance'].xcom_pull(task_ids='check_ivr_daily_models_complete_task')
    if ivr_models_complete:
        return 'exit_task'
    else:
        return 'check_dq_complete_task'

def decide_next_step2(**kwargs):
    dq_complete=kwargs['task_instance'].xcom_pull(task_ids='check_dq_complete_task')
    if dq_complete:
        return 'ivr_daily_models'
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

def submit_aws_batch_job(**kwargs):
    # Define the job name
    job_name = f'rdf_job_{rdf_client}'

    # Define the parameters for the AWS Batch job
    parameter_value = [rdf_client, "-b", rdf_files_s3]
    print(parameter_value)

    # Submit the AWS Batch job using the BatchOperator
    aws_batch_op = BatchOperator(
        task_id=f'aws_batch_task_{rdf_client}',
        job_name=job_name,
        job_definition=job_definition,
        job_queue=job_queue,
        parameters={},
        wait_for_completion=False,
        overrides={
            'command': parameter_value  # custom command with arguments
        },
        dag=dag,
    )
    
    # Execute the job submission
    aws_batch_op.execute(context=kwargs)

def should_submit_job(**kwargs):
    if submit_job.lower() == 'true':
        return 'submit_aws_batch_job'
    else:
        return 'skip_batch_job'

with DAG(
    "ivr_daily_run",
    default_args = {
        'owner': 'BI',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    },
    description='Runs daily ELT loads for Genesys IVR',
    schedule_interval= "29,59 0-2 * * *",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    tags=['BI','IVR_DAILY_RUN'],
) as dag:

    get_dates_task = PythonOperator(
        task_id='get_running_dates',
        python_callable=get_running_dates,
        provide_context=True,
        dag=dag
    )

    ivr_daily_models = DbtRunOperator(
        task_id="ivr_daily_models",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        threads=10,
        target="test",
        select=["+tag:daily_ivr"],
        exclude=["tag:deprecated tag:galileo"],
        vars={"target_schema":"GENESYS", "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"},
        dag=dag,
        priority_weight = 4
   )

    check_ivr_daily_models_complete_task=PythonOperator(
    task_id='check_ivr_daily_models_complete_task',
    python_callable=check_ivr_daily_models_complete,
    dag=dag
    )

    check_dq_complete_task=PythonOperator(
    task_id='check_dq_complete_task',
    python_callable=check_dq_complete,
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

    exit_task=PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("Exiting"),
    dag=dag
    )

    mark_ivr_daily_models_complete = SnowflakeOperator(
    task_id='mark_ivr_daily_models_complete',
    sql="""INSERT INTO AI_DATAMART.AUDIT.dbt_results_log (log_id, schema_name, model_name, status, start_ts, end_ts)
            VALUES (AI_DATAMART.AUDIT.dbt_results_log_seq.nextval, 'GENESYS', 'IVR_DAILY_MODELS', 'COMPLETED', CURRENT_TIMESTAMP(0), CURRENT_TIMESTAMP(0))
        """,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=should_submit_job,
        provide_context=True,
        dag=dag
)

    skip_batch_job = DummyOperator(
        task_id='skip_batch_job',
        dag=dag
)

    submit_aws_batch_job_task = PythonOperator(
    task_id='submit_aws_batch_job',
    python_callable=submit_aws_batch_job,
    provide_context=True,
    dag=dag,
)
    mark_ingest_update_complete = SnowflakeOperator(
    task_id='mark_ingest_update_complete',
    sql=sql_ingest_update,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag,
)

    get_dates_task >> check_ivr_daily_models_complete_task >> decide_next_step_task1
    decide_next_step_task1 >> [check_dq_complete_task,exit_task]
    check_dq_complete_task >> decide_next_step_task2
    decide_next_step_task2 >> [ivr_daily_models,exit_task]
    ivr_daily_models >> mark_ivr_daily_models_complete >> mark_ingest_update_complete >> branch_task
    branch_task >> [submit_aws_batch_job_task, skip_batch_job]