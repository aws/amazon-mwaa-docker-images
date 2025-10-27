from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

with DAG(
    'sample_get_dbt_password_from_secrets',
    default_args={
        'depends_on_past': True,
        'email': ['smuppireddy@galileo-ft.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='Test DAG to get dbt password from secrets',
    schedule_interval= None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Test', 'BI']
) as dag:

    # Get password from snowflake connection
    snowflake_conn_id='scrt_conn_snowflake_conn_ai_dbt_user'
    snowflake_hook = BaseHook.get_connection(conn_id=snowflake_conn_id)
    dbt_pass = snowflake_hook.password
    
    # Sync files from S3 to worker and then execute dbt. Set password in environment variable, and keep other environment variables as is.
    daily_models = BashOperator(
        task_id='daily_models',
        env = {'ANALYTICS_PASSWORD_FROM_SECRET_MANAGER': dbt_pass},
        append_env=True,
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
            --target my_test \
            --threads 10 \
            --vars '{\
                "exclude_schema_list":"ONEDEBIT", \
                "include_schema_list":"CREDIT", \
                "target_schema":"DM", \
                "start_date": "2025-09-22", \
                "end_date":"2025-09-23"\
                }' \
            --select dim_prod.sql
        """,
        dag=dag,
        priority_weight = 4
    )
    
    daily_models