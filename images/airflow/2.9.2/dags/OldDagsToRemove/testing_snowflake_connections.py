from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pendulum

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

with DAG(
    "snowflake_connections_test",
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Testing Snowflake Connections',
    schedule_interval= "*/5 7 * * *",
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    catchup=False
) as dag:
    # snowflake_conn_id='snowflake_conn_ai_dbt_user'
    snowflake_operator = SnowflakeOperator(
        task_id='snowflake_operator_task',
        sql="select count(*) from galileo_raw.galileo.bin_info where prod_id=284;",
        snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user",
        dag=dag,
)

snowflake_operator