from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.models import Variable

with DAG(
    'bi_create_artifact_metadata',
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    },
    description='Creates artifact tables and views to store dbt run results.',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'OneTimeSetup'],
) as dag:
    create_metedata = DbtRunOperator(
        task_id="create_metadata",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        threads=10,
        target="test",
        select=["dbt_artifacts"],
   )

    create_metedata 
#dummy change to trigger sync