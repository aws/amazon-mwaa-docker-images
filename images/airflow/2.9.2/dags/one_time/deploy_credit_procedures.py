from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow_dbt_python.operators.dbt import DbtRunOperationOperator

with DAG(
    'DeployCreditProcedures',
    default_args={
        'depends_on_past': False,
        'email': ['dw-snowflake-alerts-aaaanpjol2cqnch64knhcxhfjy@sofi.org.slack.com', 'g_lending@galileo-fintech.opsgenie.net'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    },
    description='Deploy credit-related stored procedures (sp_export_statements_to_s3)',
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['SECURED_CREDIT', 'OneTimeSetup'],
) as dag:
    
    deploy_credit_procs = DbtRunOperationOperator(
        task_id='deploy-credit-procedures',
        project_dir='s3://' + Variable.get('s3_bucket') + '/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profiles_dir='s3://' + Variable.get('s3_bucket') + '/airflow_dags/analytics_analytics-and-insights-airflow_dbt/',
        profile=Variable.get('environment_name') + '-analytics_airflow_dbt',
        target='credit_statements_target',
        macro='deploy_credit_procs',
        dag=dag,
    )

