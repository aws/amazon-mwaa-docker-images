from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator,
    DbtDepsOperator,
    DbtSeedOperator
)

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

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
        yestarday = (running_date - timedelta(days=1))
        start_date = get_datetime(yestarday.strftime('%Y-%m-%d %H:%M:%S'))
        end_date =  get_datetime(yestarday.strftime('%Y-%m-%d 23:59:59'))
    if start_date is not None and end_date is None:
        end_date =  get_datetime(start_date.strftime('%Y-%m-%d 23:59:59'))
    if truncate == True:
        start_date = get_datetime(start_date.strftime('%Y-%m-%d 00:00:00'))
        end_date = get_datetime(end_date.strftime('%Y-%m-%d 23:59:59'))
    current_date = datetime.now()

    kwargs['ti'].xcom_push(key='current_date', value=current_date.strftime('%Y-%m-%d  %H:%M:%S'))
    kwargs['ti'].xcom_push(key='running_date', value=running_date.strftime('%Y-%m-%d  %H:%M:%S'))
    kwargs['ti'].xcom_push(key='start_date', value=start_date.strftime('%Y-%m-%d  %H:%M:%S'))
    kwargs['ti'].xcom_push(key='end_date', value=end_date.strftime('%Y-%m-%d  %H:%M:%S'))
 
with DAG(
    'business_intelligence_poc',
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    },
    description='Runs BI pipeline in System Test environment.',
    schedule_interval= timedelta(minutes=15),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI','POC', 'WIP'],
) as dag:
    # deps = DbtDepsOperator(
    #     task_id="deps",
    #     project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #     profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #     target="test",
    #     profile="system_test-analytics_airflow_dbt"
    # )
    # dbt_seed = DbtSeedOperator(
    #     task_id="dbt_seed",
    #     project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #     profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #     profile="system_test-analytics_airflow_dbt",
    #     target="test",
    #     vars={"source_schema":"CHM4", "target_schema":"DM", "start_date":"2023-02-21", "end_date":"2023-02-21"},
    # )

    get_dates_task = PythonOperator(
        task_id='get_running_dates',
        python_callable=get_running_dates,
        provide_context=True,
        dag=dag
    )

    daily_models = DbtRunOperator(
        task_id="daily_models",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
    #    project_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
    #    profiles_dir="s3://analytics-and-insights-performance-airflow/airflow_dags/analytics_analytics-and-insights-airflow_dbt/",
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        threads=10,
        target="test",
        select=["+tag:daily"],
        exclude=["tag:deprecated"],
        vars={"source_schema":"CHM4", "target_schema":"DM", "start_date": "{{ ti.xcom_pull(task_ids='get_running_dates', key='start_date') }}", "end_date":"{{ ti.xcom_pull(task_ids='get_running_dates', key='end_date') }}"},
        dag=dag
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
    # dbt_seed >> 
    get_dates_task >> daily_models 
    daily_models >> success
    daily_models >> failure  
    
    
    # dummy comment as the --size-only arg on aws s3 sync --dryrun --delete --size-only is not taking pushing the changes if the size didn't change