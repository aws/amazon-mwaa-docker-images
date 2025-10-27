from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import json

# Variables that need to be set up for the DAG to execute properly
# Aws_batch_definition - ARN for the AWS batch environment
# aws_batch_job_queue - ARN for the AWS batch job queue
# rdf_files_s3 - S3 bucket name 
# ********************************************************************** #
# ad_hoc_rdf_files_conf - Files to generate. Set frequency to onetime if you want files to NOT be sent to clients
# Sample request
# {
#   "files_info": [
#     {
#       "client": "BI-experian",
#       "file_id": "1",
#       "start_date": "2025-03-11",
#       "end_date": "2025-03-12",
#       "frequency": "onetime"
#     },
#     {
#       "client": "BI-rhood",
#       "file_id": "2",
#       "start_date": "2025-03-11",
#       "end_date": "2025-03-12",
#       "frequency": "onetime"
#     },
#     {
#       "client": "BI-rhood",
#       "file_id": "3",
#       "start_date": "2025-03-11",
#       "end_date": "2025-03-12",
#       "frequency": "onetime"
#     }
#   ]
# }
# ********************************************************************** #

# Define default arguments
default_args = {
    'owner': 'BI',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ad_hoc_rdf_file_generation_multiple_requests',
    default_args=default_args,
    description='DAG to run ad-hoc RDF file generation (multiple requests)',
    schedule_interval=None,
    is_paused_upon_creation=False,
)

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

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

def get_datetime(StParam: str):
    if is_timestamp(StParam):
        return datetime.strptime(StParam, '%Y-%m-%d %H:%M:%S')
    elif is_date(StParam):
        return datetime.strptime(StParam, '%Y-%m-%d')
    return None

# Define AWS Batch job details
job_definition = Variable.get("Aws_batch_definition")
job_queue = Variable.get("aws_batch_job_queue")
rdf_files_s3 = Variable.get("rdf_files_s3")

def create_batch_operator(file_info, index):
    """ Creates an AWS Batch task dynamically """
    # Format dates in MM/DD/YYYY format for rdf.py
    start_date_mm_dd_yyyy = file_info['start_date'].strftime('%m/%d/%Y')
    end_date_mm_dd_yyyy = file_info['end_date'].strftime('%m/%d/%Y')
    
    parameter_value = [
        file_info['client'],
        "-f",
        file_info['file_id'],
        "-s",
        start_date_mm_dd_yyyy,
        "-e",
        end_date_mm_dd_yyyy,
        '-b',
        rdf_files_s3,
        '--frequency',
        file_info['frequency']
    ]

    return BatchOperator(
        task_id=f'aws_batch_task_ad_hoc_{index}',
        job_name=f'rdf_job_ad_hoc_{index}',
        job_definition=job_definition,
        job_queue=job_queue,
        parameters={},
        wait_for_completion=False,
        overrides={'command': parameter_value},
        dag=dag
    )

def create_snowflake_operator(file_info, index):
    """ Creates a Snowflake insert task dynamically """
    sql_insert_query = f""" 
        ALTER SESSION SET TIMEZONE = 'MST';
        INSERT INTO rdf_config.rdf_config.rdf_file_gen_track 
        (File_id, Client, in_ts, job_id, start_ts, end_ts, Gen_start_ts)
        VALUES 
        ({file_info['file_id']}, '{file_info['client']}', current_timestamp, 'rdf_job_{index}', current_timestamp, current_timestamp, current_timestamp)
    """

    return SnowflakeOperator(
        task_id=f'snowflake_insert_task_ad_hoc_{index}',
        sql=sql_insert_query,
        snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user",
        autocommit=True,
        dag=dag,
    )

# Extract parameters at DAG parse time
dag_run_conf = Variable.get("ad_hoc_rdf_files_conf")
params = json.loads(dag_run_conf) if isinstance(dag_run_conf, str) else dag_run_conf

files_info = params.get('files_info', [])
if isinstance(files_info, str):
    files_info = json.loads(files_info)  # Convert string to dict if necessary

# Ensure start_date and end_date are converted
for file_info in files_info:
    file_info['start_date'] = get_datetime(file_info['start_date'])
    file_info['end_date'] = get_datetime(file_info['end_date'])

# Start Task
start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "Starting DAG Execution"',
    dag=dag
)

batch_tasks = []
snowflake_tasks = []

for index, file_info in enumerate(files_info):
    batch_task = create_batch_operator(file_info, index)
    snowflake_task = create_snowflake_operator(file_info, index)
    
    batch_tasks.append(batch_task)
    snowflake_tasks.append(snowflake_task)

    start_task >> batch_task >> snowflake_task

# Success and Failure Tasks
success = BashOperator(
    task_id='print_success',
    bash_command='echo "Success"',
    trigger_rule='all_success',
    dag=dag
)

failure = BashOperator(
    task_id='print_failure',
    bash_command='echo "Failure"',
    trigger_rule='one_failed',
    dag=dag
)

for snowflake_task in snowflake_tasks:
    snowflake_task >> [success, failure]
