from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtSeedOperator, DbtRunOperationOperator
from airflow.models import Variable
#dummy change to trigger sync
with DAG(
    'CompileProcs',
    default_args={
        'depends_on_past': False,
        'email': ['dw-snowflake-alerts-aaaanpjol2cqnch64knhcxhfjy@sofi.org.slack.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    },
    description='Compiling fuctions and procedures',
    schedule_interval= None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['BI', 'CompileProcs'],
) as dag:
    compile_procs = DbtRunOperationOperator(
        task_id="compile_procs",
        project_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://'+Variable.get('s3_bucket')+'/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
        target="test",
        vars={"target_schema":"DM"},
        macro='create_deploy_procs',
    )
    compile_procs
