"""
DAG to manually run a single dbt model using Airflow.
This DAG reads the dbt model name, source schema, target schema, start date, and end date from Airflow variables and runs the specified dbt model using the DbtRunOperator.
Airflow Variables:
- dbt_model_selection: Model selection criteria
    - e.g. dim_prod and all dependent models syntax would by dim_prod+
    - e.g. any models with tags as daily syntax would by tag:daily
    - Add more examples here
- start_date: The start date for DBT execution, defaults to yesterday
- end_date: The end date for DBT execution, defaults to today
- include_schema_list: Schemas from where to read from. For example, 'SOFI, BAMBU, SMI'. Enhance marco to take ALL as a value
- exclude_schema_list: Schemas to exclude from the execution. For example, 'SOFI, BAMBU, SMI'. Currently not used
- target_schema: Schema to load data into


Usage:
1. Set the required Airflow variables (dbt_model_selection, target_schema, include_schema_list, 
        exclude_schema_list, start_date, end_date, is_full_load_run, is_first_time_run) in the Airflow UI or CLI.
2. Trigger the DAG manually from the Airflow UI or CLI.

"""

from airflow import DAG
from airflow.models import Variable
from airflow_dbt_python.operators.dbt import DbtRunOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Retrieve from Airflow Variables
dbt_model_selection = Variable.get("dbt_model_selection")
target_schema = Variable.get("target_schema")
include_schema_list = Variable.get("include_schema_list")
exclude_schema_list = Variable.get("exclude_schema_list")
start_date = Variable.get("start_date")
end_date = Variable.get("end_date")
is_full_load_run = Variable.get("is_full_load_run")
is_first_time_run = Variable.get("is_first_time_run")
batch_id = Variable.get("batch_id")

with DAG(
    "manual_run_dbt_model_full_load",
    default_args = {
        'owner': 'BI',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    },
    description='Runs dbt model set in variables using the source , target schema and start, end date variables .',
    schedule_interval= None,
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    start_date=datetime(2024, 5, 1),
    tags=['BI','MANUAL_RUN'],
) as dag:

    s3_path='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt'
    
    sync_s3 = BashOperator(
        task_id="sync_s3",
        bash_command=f"aws s3 sync {s3_path} /tmp/dbt_project --exact-timestamps",
    )
        
    run_one_model = DbtRunOperator(
        task_id="run_one_model",
        project_dir='/tmp/dbt_project/',
        profiles_dir='/tmp/dbt_project/',
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        threads=10,
        target="test",
        select=[dbt_model_selection],
        vars={"exclude_schema_list":exclude_schema_list, "target_schema":target_schema, "start_date": start_date, "end_date":end_date, "is_full_load_run":is_full_load_run, "is_first_time_run":is_first_time_run},
        dag=dag,
        priority_weight = 4
   )

    sync_s3 >> run_one_model