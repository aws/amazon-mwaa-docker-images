from datetime import timedelta, datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Variable
import pendulum
import logging

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Get logger
logger = logging.getLogger(__name__)

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")
source_schema = "SOFI"
dag_schedule = "0 18 * * *"
s3_bucket = "statements-credit-pd"
batch_size = 50000

def get_dbt_params(**kwargs):
    dag_run_conf = kwargs['dag_run'].conf
    default_args = kwargs['dag'].default_args

    params = {**default_args, **dag_run_conf} if dag_run_conf else default_args
    close_date = params.get('close_date', "")
    if close_date:
        close_date = datetime.strptime(close_date, '%Y-%m-%d')
    else:
        close_date = datetime.now() - timedelta(days=1)
    end_date = close_date
    start_date = end_date - timedelta(days=30)
    print(f"running statement generation for {source_schema} for close date {close_date} and activities"
          f"between {start_date} and {end_date}.")
    kwargs['ti'].xcom_push(key='close_date', value=close_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='start_date', value=start_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='end_date', value=end_date.strftime('%Y-%m-%d'))
    kwargs['ti'].xcom_push(key='source_schema', value=source_schema)


with DAG(
    f"statement_daily_run_{source_schema}",
    default_args={
        'owner': 'SECURED_CREDIT',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'running_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    },
    description=f"Runs Credit Statement Generation for {source_schema}.",
    schedule_interval=dag_schedule,
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True,
    start_date=datetime(2025, 4, 1, tzinfo=local_tz),
    tags=['SECURED_CREDIT', 'DAILY_RUN', 'STATEMENTS'],
) as dag:
    get_dbt_params = PythonOperator(
        task_id='get_dbt_params',
        python_callable=get_dbt_params,
        provide_context=True,
        dag=dag
    )

    @task_group(group_id="statement_generation_dbt")
    def statement_generation_dbt_models():
        s3_path = 's3://' + Variable.get('s3_bucket') + '/airflow_dags/analytics_analytics-and-insights-airflow_dbt'

        sync_s3_dbt_project = BashOperator(
            task_id="sync_s3_dbt_project",
            bash_command=f"aws s3 sync {s3_path} /tmp/dbt_project --exact-timestamps",
        )

        run_control_date = DbtRunOperator(
            task_id="control_date_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            full_refresh=True,
            target="credit_statements_target",
            select=["control_date"],
            vars={"close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                  "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
                  },
            dag=dag,
        )

        run_closing_credit_billing_cycle_model = DbtRunOperator(
            task_id="closing_credit_billing_cycle_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["closing_credit_billing_cycles"],
            full_refresh=True,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
                },
            dag=dag,
        )

        run_fees_ytd_model = DbtRunOperator(
            task_id="fees_ytd_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["fees_ytd"],
            full_refresh=True,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
            },
            dag=dag,
        )

        run_fees_in_cycle_model = DbtRunOperator(
            task_id="fees_in_cycle_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["fees_in_cycle"],
            full_refresh=True,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
            },
            dag=dag,
        )

        run_authorized_user_accounts_model = DbtRunOperator(
            task_id="authorized_user_accounts_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["authorized_user_accounts"],
            full_refresh=True,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
            },
            dag=dag,
        )

        run_all_trans_history_model = DbtRunOperator(
            task_id="all_trans_history_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["all_trans_history"],
            full_refresh=True,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
            },
            dag=dag,
        )

        run_statement_generation_model = DbtRunOperator(
            task_id="statement_generation_model",
            project_dir='/tmp/dbt_project/',
            profiles_dir='/tmp/dbt_project/',
            profile=Variable.get('environment_name')+'-analytics_airflow_dbt',
            threads=10,
            target="credit_statements_target",
            select=["account_statement"],
            full_refresh=False,
            vars={
                "close_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}",
                "start_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='start_date') }}",
                "end_date": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='end_date') }}",
                "source_schema": "{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}",
            },
            dag=dag,
        )

        sync_s3_dbt_project >> run_control_date >> run_closing_credit_billing_cycle_model
        run_closing_credit_billing_cycle_model >> run_fees_ytd_model >> run_statement_generation_model
        run_closing_credit_billing_cycle_model >> run_fees_in_cycle_model >> run_statement_generation_model
        run_closing_credit_billing_cycle_model >> run_authorized_user_accounts_model >> run_statement_generation_model
        run_closing_credit_billing_cycle_model >> run_all_trans_history_model >> run_statement_generation_model

        return run_statement_generation_model

    # Export statements to S3 using Snowpark stored procedure
    export_statements_to_s3 = SnowflakeOperator(
        task_id='export_statements_to_s3',
        sql=f"""
            CALL public.sp_export_statements_to_s3(
                '{{{{ ti.xcom_pull(task_ids='get_dbt_params', key='close_date') }}}}'::DATE,
                '{{{{ ti.xcom_pull(task_ids='get_dbt_params', key='source_schema') }}}}',
                'dm',
                'STATEMENT_STAGE',
                '{s3_bucket}',
                {batch_size}
            );
        """,
        snowflake_conn_id='credit-snowflake-dbt-password-user',
        autocommit=True,
        dag=dag
    )

    # Define task dependencies
    statement_gen_task = statement_generation_dbt_models()
    get_dbt_params >> statement_gen_task >> export_statements_to_s3
