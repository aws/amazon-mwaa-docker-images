from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

with DAG(
    'InitialSetup',
    default_args={
        'depends_on_past': False,
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
    
    # Set up audit tables to store dbt run results for AI_DATAMART (on run end post hook data)
    audit_setup = BashOperator(
        task_id='audit_setup',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --threads 8 \
            --select "dbt_artifacts"
        """,
        dag=dag,
    )

    # Set up audit tables to store dbt run results for GALILEO_DEDUP (on run end post hook data)
    audit_setup_galileo_dedup_db = BashOperator(
        task_id='audit_setup_galileo_dedup_db',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test_galileo_raw_dedup \
            --threads 8 \
            --select "dbt_artifacts"
        """,
        dag=dag,
    )
    
    # Set up audit tables to store dbt run results for SECUREDB_DEDUP (on run end post hook data)
    audit_setup_securdb_dedup = BashOperator(
        task_id='audit_setup_securdb_dedup',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test_securedb_dedup \
            --threads 8 \
            --select "dbt_artifacts"
        """,
        dag=dag,
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt seed \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --threads 8 
        """,
        dag=dag,
    )    

    dbt_log_table = BashOperator(
        task_id='dbt_log_table',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --threads 8 \
            --select +tag:"log"
        """,
        dag=dag,
    )   

    onetime_views = BashOperator(
        task_id='onetime_views',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --threads 8 \
            --select +tag:"onetime"
        """,
        dag=dag,
    )   

    alert_framework_metadata = BashOperator(
        task_id='alert_framework_metadata',
        bash_command="""
        # Sync files from S3 to tmp directory
        aws s3 sync --delete s3://{{ var.value.s3_bucket }}/airflow_dags/analytics_analytics-and-insights-airflow_dbt /tmp/dbt_project --exact-timestamps &&
        # Navigate to dbt project directory
        cd /tmp/dbt_project &&
        dbt run-operation create_alert_metadata \
            --profile {{ var.value.environment_name }}-analytics_airflow_dbt \
            --project-dir /tmp/dbt_project \
            --profiles-dir /tmp/dbt_project \
            --target test \
            --vars '{\
                "target_schema":"DM", \
                }' 
        """,
        dag=dag,
    ) 
    
    audit_setup >> audit_setup_galileo_dedup_db >> audit_setup_securdb_dedup >> dbt_seed >> dbt_log_table >> onetime_views >> alert_framework_metadata