from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.models import Variable
import boto3
import pendulum


# Define the default arguments for the DAG
default_args = {
    'owner': 'BI',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the file generation DAG
dag = DAG(
    'batch_job_terminate',
    default_args=default_args,
    description='DAG to run ad-hoc terminating the stuck aws batch jobs',
    schedule_interval=None,  # This DAG is not scheduled independently
    is_paused_upon_creation=False,
)

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

def get_running_parameters(**kwargs):
    
    dag_run_conf = kwargs['dag_run'].conf
    default_args = kwargs['dag'].default_args

    params = {**default_args, **dag_run_conf} if dag_run_conf else default_args

    # client = params.get('client', None)
    # if client:
    #     # client = client
    #     print(client)
    # file_id = params.get('file_id', None)
    # if file_id:
    #     # file_id = file_id
    #     print(file_id)
    job_id=params.get('job_id', None)
    if job_id:
        print ("job _id: " + job_id)
    reason=params.get('job_id', None)
    if reason:
        print ("reason:" +  reason)
    terminate_aws_batch_job(job_id, reason)
 
# define aws batch job details variables and get values
job_name = f'rdf_job_terminate'
job_definition = Variable.get("Aws_batch_definition") 
job_queue = Variable.get("aws_batch_job_queue") 
rdf_files_s3=Variable.get("rdf_files_s3") 

def terminate_aws_batch_job(job_id, reason):
    client = boto3.client('batch')
    try:
        response = client.terminate_job(jobId=job_id, reason=reason)
        print(f"Termination response: {response}")
        return response
    except client.exceptions.ClientError as e:
        print(f"Failed to terminate job {job_id}: {str(e)}")
        raise

# insert_values="({{ ti.xcom_pull(task_ids='get_parameters', key='file_id') }},'{{ ti.xcom_pull(task_ids='get_parameters', key='client') }}',sysdate(),'rdf_job_1',sysdate(),sysdate(),sysdate())"
# sql_insert_query=f" insert into rdf_config_clone_bhebbal_mydb.rdf_config.rdf_file_gen_track (File_id,Client, in_ts,job_id,start_ts,end_ts,Gen_start_ts) VALUES{insert_values}"

# # Define the snowflake operator to execute sql query to insert data
# snowflake_insert_op=SnowflakeOperator(
#             task_id ='snowflake_insert_task_ad_hoc',
#             sql=sql_insert_query,
#             snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user",
#             autocommit=True, #enable auto commit
#             dag=dag,
#         )

# Define the Batch operator to call the rdf file job
python_op = PythonOperator(
    task_id='get_parameters',
    python_callable=get_running_parameters,
    provide_context=True,
    dag=dag,
)

success = BashOperator(
        task_id='print_success',
        bash_command='echo "Success"',
        trigger_rule = 'all_success',
        dag=dag
    )
failure = BashOperator(
        task_id='print_failure',
        bash_command='echo "failure"',
        trigger_rule = 'one_failed',
        dag=dag
    )

# Set task dependencies
python_op >>  success
python_op >> failure