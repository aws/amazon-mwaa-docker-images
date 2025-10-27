from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pendulum

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

with DAG(
    "bi_dedup_data_full_refresh",
    default_args = {
        'owner': 'BI',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    },
    description='Runs Full Refresh for Dedup models for both secure DB and Galileo DB.',
    schedule_interval= None,
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    start_date=datetime(2025, 4, 1, tzinfo=local_tz),
    tags=['BI','DEDUP'],
) as dag:

    # Sync files from S3 to worker and then execute dbt
    gal_raw_dedup_full_refresh = BashOperator(
        task_id='gal_raw_dedup_full_refresh',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run --full-refresh \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test_galileo_raw_dedup \
            --threads 8 \
            --select tag:"auto_dedup_galileo_raw"\
            --exclude tag:"deprecated" 
        """,
        dag=dag,
        priority_weight = 1
    )

    gal_raw_dedup_full_refresh