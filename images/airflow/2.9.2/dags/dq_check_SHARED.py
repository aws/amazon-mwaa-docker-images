from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum

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

# Hardcoded schema list for now
schema_list = ['TPP', 'WSL', 'GALILEO', 'IVR', 'OPS']

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
    
# Check if DQ is complete for all shared schemas.
def check_dq_complete():
    sql_dq_complete="""
        SELECT COUNT(1) FROM 
        (
            SELECT 
                COUNT(1) schema_cnt
                , SUM(CASE WHEN is_dq_success='Y' THEN 1 ELSE 0 END) dq_success_schema_cnt
                , SUM(CASE WHEN is_dq_success='Y' THEN 0 ELSE 1 END) dq_failure_schema_cnt
                , LISTAGG(CASE WHEN is_dq_success='Y' THEN NULL ELSE schema_name END, ', ') pending_dq_schema_list
            FROM AI_DATAMART.DM.vw_dq_schema_results_today 
            WHERE 1=1
            AND schema_type!='CORE'
        )
        WHERE 1=1
        AND dq_failure_schema_cnt = 0 -- Check if all schemas have passed DQ
    """
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete)
    return result[0] > 0 # return true if query returns 1 else 0
    # return False

# Function to check DQ for a schema
def check_dq_for_schema(schema, **kwargs):
    sql_dq_complete_schema = f"""
        SELECT COUNT(1) FROM 
        (
            SELECT 
                  COUNT(1) schema_cnt
                , SUM(CASE WHEN is_dq_success='Y' THEN 1 ELSE 0 END) dq_success_schema_cnt
                , SUM(CASE WHEN is_dq_success='Y' THEN 0 ELSE 1 END) dq_failure_schema_cnt
                , LISTAGG(CASE WHEN is_dq_success='Y' THEN NULL ELSE schema_name END, ', ') pending_dq_schema_list
            FROM AI_DATAMART.DM.vw_dq_schema_results_today 
            WHERE 1=1
            AND schema_type!='CORE'
            AND schema_name = '{schema}'
        )
        WHERE 1=1
        AND dq_failure_schema_cnt = 0 -- Check if all schemas have passed DQ
    """
    snowflake_hook=BaseHook.get_hook(conn_id=snowflake_conn_id)
    result=snowflake_hook.get_first(sql_dq_complete_schema)
    return result[0] > 0 # return true if query returns 1 else 0
    # print(f"Checking DQ for schema {schema}...")
    # return True

# Function to dynamically create DQ check and dbt tasks for each schema
def create_dbt_tasks_with_dq_check(schema_list, dag):
    tasks = []
    for schema in schema_list:
        # Task to check DQ for the schema
        dq_check_task_schema = PythonOperator(
            task_id=f"check_dq_{schema.lower()}",
            python_callable=check_dq_for_schema,
            op_kwargs={'schema': schema},
            provide_context=True,
            dag=dag
        )

        # Task to decide whether to run dbt model or exit out
        decide_next_task_schema = BranchPythonOperator(
            task_id=f"decide_next_task_{schema.lower()}",
            python_callable=lambda schema=schema, **kwargs: f"exit_task_{schema.lower()}" if kwargs['ti'].xcom_pull(task_ids=f"check_dq_{schema.lower()}") else f"run_dbt_model_{schema.lower()}",
            op_kwargs={'schema': schema},
            provide_context=True,
            dag=dag
        )

        # Run dq_src_tgt_results_multiple_clients for the specific shared schema and date range
        dbt_task = BashOperator(
            task_id=f"run_dbt_model_{schema.lower()}",
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
                        "schema_type":"{{params.schema}}", \
                        "target_schema":"DM", \
                            "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                            "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
                        }' \
                    --select "dq_src_tgt_results_multiple_clients"
                """,
            params={'schema': schema},
            dag=dag
        )
        
        # Exit task
        exit_task2 = PythonOperator(
            task_id=f"exit_task_{schema.lower()}",
            python_callable=lambda: print("No schemas to process. Exiting."),
            dag=dag
        )
        
        # Add dependency: DQ check must complete before running dbt
        dq_check_task_schema >> decide_next_task_schema
        decide_next_task_schema >> [dbt_task, exit_task2]

        # Add both tasks to the list
        tasks.append((dq_check_task_schema, dbt_task))
    return tasks

# DAG definition
dag = DAG(
    default_args=default_args,
    dag_id="dq_check_SHARED",
    description="DQ DAG for running dbt models for shared schemas that has pending DQ. Generates tasks based on schemas defined on schema_list",
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),
    schedule_interval="0,15,30,45 1-15 * * *",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['BI', 'DQ'],
)

# Task to check if DQ is complete
check_dq_complete_task = PythonOperator(
    task_id='check_dq_complete_task',
    python_callable=check_dq_complete,
    dag=dag
)

# Task to decide the next task
decide_next_task1_task = BranchPythonOperator(
    task_id='decide_next_task1_task',
    python_callable=lambda **kwargs: 'exit_task' if kwargs['ti'].xcom_pull(task_ids='check_dq_complete_task') else 'get_schema_list_task',
    provide_context=True,
    dag=dag
)

# Task to get the running dates
get_dates_task = PythonOperator(
    task_id='get_running_dates',
    python_callable=get_running_dates,
    provide_context=True,
    dag=dag
)

# Task to get the schema list
get_schema_list_task = PythonOperator(
    task_id='get_schema_list_task',
    python_callable=lambda **kwargs: kwargs['ti'].xcom_push(key='schema_list', value=schema_list),
    provide_context=True,
    dag=dag
)

# Register all possible dbt tasks with DQ checks in the DAG during parsing
dbt_tasks_with_dq_checks = create_dbt_tasks_with_dq_check(schema_list=schema_list, dag=dag)

# Exit task
exit_task = PythonOperator(
    task_id='exit_task',
    python_callable=lambda: print("No schemas to process. Exiting."),
    dag=dag
)

# Set task dependencies
get_dates_task >> check_dq_complete_task
check_dq_complete_task >> decide_next_task1_task
decide_next_task1_task >> [get_schema_list_task, exit_task]

# Add dynamically created DQ check and dbt tasks to the DAG
for dq_check_task_schema, dbt_task in dbt_tasks_with_dq_checks:
    get_schema_list_task >> dq_check_task_schema
    
#Dummy change to trigger CI for schedule changes