"""
DAG to run a first time setup dbt models using Airflow.
This DAG reads the source schema, target schema, start date, and end date from Airflow variables and runs the specified dbt model using the DbtRunOperator.
Airflow Variables:
- start_date: The start date for DBT execution, defaults to yesterday
- end_date: The end date for DBT execution, defaults to today
- source_schema: Schema from where to read
- target_schema: Schema to load data into

Usage:
1. Set the required Airflow variables (start_date, end_date, source_schema, target_schema).
2. Trigger the DAG manually from the Airflow UI or CLI.

"""

from airflow import DAG
from airflow.models import Variable
from airflow_dbt_python.operators.dbt import DbtRunOperator
from datetime import datetime, timedelta

# Retrieve from Airflow Variables
source_schema = Variable.get("source_schema")
target_schema = Variable.get("target_schema")
start_date = Variable.get("start_date")
end_date = Variable.get("end_date")

with DAG(
    "bi_first_time_run",
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
    
    first_time_run_models = DbtRunOperator(
        task_id="first_time_run_models",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        threads=10,
        target="test",
        select=["tag:first_time_run"],
        vars={"source_schema":source_schema, "target_schema":target_schema, "start_date": start_date, "end_date":end_date, "is_first_time_run":"Y"},
        dag=dag,
        priority_weight = 4
   )

    first_time_run_models