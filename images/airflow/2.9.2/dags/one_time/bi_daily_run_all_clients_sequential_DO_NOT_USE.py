from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook


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

# Hardcoded schema list for now
schema_list = ['SODAHEALTH', 'APLAZO', 'GLNTP', 'CLAIR', 'ZENDA', 'TPAY', 'MONZO', 'QUBE', 'BVINE', 'GROUPO', 'RIVET', 'PLAYFARE', 'LEVEL20', 'NOMAD', 'FINZEO', 'BUCKONE', 'KREDIO', 'BAMBUMX', 'KAPITALMX', 'WALGREENS', 'JASSBY', 'TOAST', 'KIKOFF', 'EVE', 'UPHOLD', 'PROPEL', 'DONDE', 'TABPAY', 'ENTRATA', 'WISDOMTREE', 'FINTONIC', 'MILKIPAY', 'NESWELL', 'MYPOS', 'TEMPOPAY', 'CHARLIEPENSION', 'BOBTAIL', 'DAVECOM', 'KITTRELL', 'MOCAFI', 'PAYC', 'ASPIRATION', 'REMIT', 'LILIB', 'EDGEMARKETS', 'NEOFIE', 'PAYTIENT', 'EVEREE', 'TOTEM', 'RAIS', 'BUSHEL', 'BITPAY', 'BLUEP', 'PAYSAFE', 'MESHPAY', 'MEZU', 'TOMO', 'BROXEL', 'NEXIS', 'COBEEMX', 'MONEYGRAM', 'ZURP', 'BOLDCOL', 'SACBEVISA', 'WALDOCARD', 'LEMONADE', 'WYNDHAM', 'MESHPAYCAD', 'BRIDGE', 'FINEXIO', 'GLBLREWARDS', 'CPAY', 'NORTHONE', 'RIZE', 'SNAP', 'FLEXIA', 'BERK', 'BERKELYCAD', 'MLION', 'OXYGEN', 'BILLHWY', 'DAILYPAY', 'RCC', 'POMELO', 'RHO', 'ORBI', 'KUDZU', 'REMESSA', 'COMUN', 'FINTECHEXP', 'VMU', 'CREDITGENIE', 'INDRIVE', 'LEVL', 'KEETAMX', 'VIRAAL', 'IRONVEST', 'TREMENDOUS', 'ALLDIG', 'ARGENTINAMKT', 'BORO', 'CHILEMKT', 'CREDIT', 'DIVIDENZMX', 'ECH0', 'FUNDBOX', 'GENIAL', 'GLM', 'GLOBAL66', 'GSETTER', 'IDT', 'IVENTURE', 'KABCASH', 'KEETA', 'KLAR', 'LEMONADEUS', 'MAJORITY', 'PAYMERANG', 'PERUMKT', 'POCKET', 'RAPID', 'REVPP', 'SACBE', 'SACPAY', 'TILL', 'UALA', 'UALACOL', 'URUGUAYMKT', 'XP', 'SPEEDYCASH', 'GSX', 'PRECASH', 'IBKR', 'CAIBKR', 'KOHO', 'ARIBA', 'TFSBILLPAY', 'TRIBAL', 'COMPOUND', 'COGNI', 'MBANQ', 'PRIPAY', 'BALANCE', 'PAPERCH', 'BAANX', 'GREEN', 'TWISE', 'HRBLOCK', 'HUTSY', 'YOUWORLD', 'TROWE', 'LEAPFIN', 'PAYAWARE', 'SAV', 'CLIQ', 'CHKBOOK', 'PLATA', 'BHN', 'AMSCOT', 'RBC', 'KNAPSACK', 'MULTIMKT', 'TRIUMPHPAY', 'AVENUE', 'MODAK', 'UPWARDLI', 'EXPERIAN', 'SMI', 'SMITX', 'ONEDEBIT', 'SOFI']

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
    
# Function to dynamically create dbt run tasks for each schema
def create_dbt_tasks(schema_list, dag):
    tasks = []
    for schema in schema_list:
        
        # Task to run dbt for the schema
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
                        "source_schema":"{{params.schema}}", \
                        "target_schema":"DM", \
                        "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", \
                        "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"\
                        }' \
                    --select +tag:"daily"\
                    --exclude tag:"deprecated" tag:"galileo"
                """,
            params={'schema': schema},
            dag=dag
        )

        # Add both tasks to the list
        tasks.append(dbt_task)
    return tasks

# DAG definition
dag = DAG(
    default_args=default_args,
    dag_id="daily_run_all_clients_sequentially_DO_NOT_USE",
    description="Do NOT USE, daily_run_all_clients_sequentially_DO_NOT_USE",
    start_date=datetime(2024, 5, 1, tzinfo=local_tz),
    schedule_interval=None,
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    tags=['BI', 'DAILY_RUN'],
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
dbt_tasks = create_dbt_tasks(schema_list=schema_list, dag=dag)

# Set task dependencies
get_dates_task >> get_schema_list_task

# Add dynamically created DQ check and dbt tasks to the DAG
for dbt_task in dbt_tasks:
    get_schema_list_task >> dbt_task
    