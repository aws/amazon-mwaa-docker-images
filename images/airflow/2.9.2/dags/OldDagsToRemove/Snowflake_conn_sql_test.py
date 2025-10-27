from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.operators.bash_operator import BashOperator
from snowflake import connector as sfconn
import os

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'SQLQueryTest',
    default_args=default_args,
    description='DAG to execute a SQL query in Snowflake and get the details rdf files ready for file generation',
    schedule_interval=None,  
)

# snowflake_conn_id = "scrt_conn_snowflake_conn_ai_dbt_user"

    # sql_query2 = "select Count(1) cnt from rdf_config.rdf_config.rdf_client_sched"
    # sql_query3 = "select Count(1) cnt from rdf_config.rdf_config.rdf_file_gen_track"
    # sql_query4 = "select Count(1) cnt from rdf_config.rdf_config.rdf_client_options"
    # sql_query5 = "select Count(1) cnt from rdf_config.rdf_config.rdf_client_programs"
    # sql_query6 = "select Count(1) cnt from ai_datamart.audit.fct_dbt__model_executions"
    # sql_query7 = "select Count(1) cnt from ai_datamart.audit.fct_dbt__invocations"
    # sql_query8 = "select Count(1) cnt from ai_datamart.dm.dim_prod"



def execute_sql1():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user_tf")

    sql_query1 = "select client, Count(1) cnt from rdf_config.rdf_config.rdf_file group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql2():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select client, Count(1) cnt from rdf_config.rdf_config.rdf_client_sched group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql3():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select client, Count(1) cnt from rdf_config.rdf_config.rdf_client_options group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql4():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select client, Count(1) cnt from rdf_config.rdf_config.rdf_file_gen_track group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql5():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select client, Count(1) cnt from rdf_config.rdf_config.rdf_client_programs group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql6():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select status, Count(1) cnt from ai_datamart.audit.fct_dbt__model_executions group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql7():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select dbt_command, Count(1) cnt from ai_datamart.audit.fct_dbt__invocations group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql8():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select schema_name, Count(1) cnt from ai_datamart.dm.dim_prod group by 1"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql9():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select * from ai_datamart.dm.pending_rdf_files_drop"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

def execute_sql10():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    sql_query1 = "select Count(1) cnt from ai_datamart.dm.pending_rdf_files_drop"
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query1)
    rows = cursor.fetchall()
    return rows

execute_sql1 = PythonOperator(
    task_id='sql1',
    python_callable=execute_sql1,
    dag=dag,
)

execute_sql2 = PythonOperator(
    task_id='sql2',
    python_callable=execute_sql2,
    dag=dag,
)

execute_sql3 = PythonOperator(
    task_id='sql3',
    python_callable=execute_sql3,
    dag=dag,
)

execute_sql4 = PythonOperator(
    task_id='sql4',
    python_callable=execute_sql4,
    dag=dag,
)

execute_sql5 = PythonOperator(
    task_id='sql5',
    python_callable=execute_sql5,
    dag=dag,
)

execute_sql6 = PythonOperator(
    task_id='sql6',
    python_callable=execute_sql6,
    dag=dag,
)

execute_sql7 = PythonOperator(
    task_id='sql7',
    python_callable=execute_sql7,
    dag=dag,
)

execute_sql8 = PythonOperator(
    task_id='sql8',
    python_callable=execute_sql8,
    dag=dag,
)

execute_sql9 = PythonOperator(
    task_id='sql9',
    python_callable=execute_sql9,
    dag=dag,
)

execute_sql10 = PythonOperator(
    task_id='sql10',
    python_callable=execute_sql10,
    dag=dag,
)

# Set task dependencies
execute_sql1
execute_sql2
# execute_sql3
# execute_sql4
# execute_sql5
# execute_sql6
# execute_sql7
# execute_sql8
# execute_sql9
# execute_sql10
