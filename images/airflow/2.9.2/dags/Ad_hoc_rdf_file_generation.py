from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.models import Variable
import pendulum


# Define the default arguments for the DAG
default_args = {
    'owner': 'BI',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the file generation DAG
dag = DAG(
    'ad_hoc_rdf_file_generation',
    default_args=default_args,
    description='DAG to run ad-hoc rdf file generation with runtime parameters',
    schedule_interval=None,  # This DAG is not scheduled independently
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

def get_datetime(StParam:str):
    DtParam = None
    if is_timestamp(StParam):
        DtParam = datetime.strptime(StParam, '%Y-%m-%d  %H:%M:%S')
    elif is_date(StParam):
        DtParam = datetime.strptime(StParam, '%Y-%m-%d')
    return DtParam

def get_running_parameters(**kwargs):
    
    dag_run_conf = kwargs['dag_run'].conf
    default_args = kwargs['dag'].default_args

    params = {**default_args, **dag_run_conf} if dag_run_conf else default_args

    client = params.get('client', None)
    if client:
        # client = client
        print(client)
    file_id = params.get('file_id', None)
    if file_id:
        # file_id = file_id
        print(file_id)
    start_date = params.get('start_date', None)
    if start_date:
        start_date = get_datetime(start_date)
        print(start_date)
    end_date = params.get('end_date', None)
    if end_date:
        end_date = get_datetime(end_date)
        print(end_date)
    running_date = get_datetime(params.get('running_date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    if start_date == None and end_date == None:
        today = running_date
        yesterday = (running_date - timedelta(days=1))
        start_date = get_datetime(yesterday.strftime('%Y-%m-%d %H:%M:%S'))
        end_date =  get_datetime(today.strftime('%Y-%m-%d 23:59:59'))
    if start_date is not None and end_date is None:
        end_date =  get_datetime(start_date.strftime('%Y-%m-%d 23:59:59'))
    frequency = params.get('frequency', None)
    if frequency:
        # frequency = frequency
        print(frequency)

    # Format dates as MM/DD/YYYY for rdf.py
    start_date_mm_dd_yyyy = start_date.strftime('%m/%d/%Y')
    end_date_mm_dd_yyyy = end_date.strftime('%m/%d/%Y')

    kwargs['ti'].xcom_push(key='client', value=client)
    kwargs['ti'].xcom_push(key='file_id', value=file_id)
    kwargs['ti'].xcom_push(key='running_date', value=running_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='start_date', value=start_date_mm_dd_yyyy)
    kwargs['ti'].xcom_push(key='end_date', value=end_date_mm_dd_yyyy)
    kwargs['ti'].xcom_push(key='frequency', value=frequency)
 
# define aws batch job details variables and get values
job_name = f'rdf_job_ad_hoc'
job_definition = Variable.get("Aws_batch_definition") 
job_queue = Variable.get("aws_batch_job_queue") 
rdf_files_s3=Variable.get("rdf_files_s3") 

parameter_value=["{{ ti.xcom_pull(task_ids='get_parameters', key='client') }}",
                 "-f", str("{{ ti.xcom_pull(task_ids='get_parameters', key='file_id') }}"),
                 "-s", "{{ ti.xcom_pull(task_ids='get_parameters', key='start_date') }}",
                 "-e", "{{ ti.xcom_pull(task_ids='get_parameters', key='end_date') }}",
                 '-b', rdf_files_s3,
                 '--frequency', "{{ ti.xcom_pull(task_ids='get_parameters', key='frequency') }}"]
print(parameter_value)

#submit batch job request for the file_id, and client using the batchoperator
aws_batch_op = BatchOperator(
            task_id=f'aws_batch_task_ad_hoc',
            job_name=job_name,
            job_definition=job_definition,
            job_queue=job_queue,
            parameters={},
            wait_for_completion=False,
            overrides={
            'command' : parameter_value
            },
            dag=dag
        )

insert_values="({{ ti.xcom_pull(task_ids='get_parameters', key='file_id') }},'{{ ti.xcom_pull(task_ids='get_parameters', key='client') }}',current_timestamp,'rdf_job_1',current_timestamp,current_timestamp,current_timestamp)"
sql_insert_query=f" ALTER SESSION SET TIMEZONE = 'MST'; insert into rdf_config.rdf_config.rdf_file_gen_track (File_id,Client, in_ts,job_id,start_ts,end_ts,Gen_start_ts) VALUES{insert_values}"

# Define the snowflake operator to execute sql query to insert data
snowflake_insert_op=SnowflakeOperator(
            task_id ='snowflake_insert_task_ad_hoc',
            sql=sql_insert_query,
            snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user",
            autocommit=True, #enable auto commit
            dag=dag,
        )

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
python_op >> aws_batch_op >> snowflake_insert_op
snowflake_insert_op >> success
snowflake_insert_op >> failure

