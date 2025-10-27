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
import pendulum
import logging

# Get logger
logger = logging.getLogger(__name__)

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'rdf_file_trigger',
    default_args=default_args,
    description='DAG to execute a SQL query in Snowflake and get the details rdf files ready for file generation',
    schedule_interval=timedelta(minutes=5),
    concurrency=1,
    max_active_runs=1,
    catchup=False,  
)

def execute_sql_query_return_rows():
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")

    # Updated query to include category and priority from rdf_file_priority
    sql_query = """
                SELECT f.file_id, f.client, f.run_freq, f.start_date, f.end_date, 
                       f.pending_count, f.complete_count, 
                       COALESCE(p.category, 'medium') AS category,
                       COALESCE(p.priority, 1) AS priority
                FROM ai_datamart.dm.files_ready_to_process f
                LEFT JOIN ai_datamart.dm.rdf_file_priority p
                  ON f.file_id = p.file_id
                  AND f.client = p.client
                ORDER BY priority DESC
                """
    # this cursor to execute query and get all rows for file generation ready
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()

    # return all the rows 
    return rows

# define aws batch job details variables and get values
job_definition_default = Variable.get("Aws_batch_definition") 
# Handle default values in a more compatible way
try:
    job_definition_small = Variable.get("Aws_batch_definition_small")
except (KeyError, ValueError):
    job_definition_small = job_definition_default

try:
    job_definition_large = Variable.get("Aws_batch_definition_large") 
except (KeyError, ValueError):
    job_definition_large = job_definition_default

job_queue = Variable.get("aws_batch_job_queue") 
rdf_files_s3 = Variable.get("rdf_files_s3")  #"galileo-bi-rdf-files-perf"

def execute_aws_batch(**kwargs):
    # This function executes AWS Batch for each row in the Snowflake result
    result = kwargs['ti'].xcom_pull(task_ids='execute_sql_query_return_rows_task')
    logger.info(f"Retrieved {result} files to process")

    for row in result:
        # Process row and create AWS Batch job parameters dynamically
        file_id = row[0]
        client = row[1]
        start_date = row[3]
        end_date = row[4]
        category = row[7]
        priority = row[8]
        
        # Format the dates from Snowflake to MM/DD/YYYY format
        # If start_date or end_date is None, default to yesterday/today
        start_date_str = None
        end_date_str = None
        
        # Use start_date from database if available
        if start_date:
            # Check if start_date is already a string
            if isinstance(start_date, str):
                # If it's already a string, use it directly or parse it if needed
                try:
                    # Try to parse if it's in a common format
                    dt = datetime.strptime(start_date, '%Y-%m-%d')
                    start_date_str = dt.strftime('%m/%d/%Y')
                except ValueError:
                    # If parsing fails, just use it as is - it might already be in the right format
                    start_date_str = start_date
                logger.info(f"Using start date (string) from database: {start_date_str}")
            else:
                # It's a datetime object
                start_date_str = start_date.strftime('%m/%d/%Y')
                logger.info(f"Using start date from database: {start_date_str}")
        else:
            # Default to yesterday if no start_date provided
            yesterday = datetime.now(local_tz) - timedelta(days=1)
            start_date_str = yesterday.strftime('%m/%d/%Y')
            logger.info(f"No start date in database, using yesterday: {start_date_str}")
            

        if end_date:
            if isinstance(end_date, str):
                try:
                    dt = datetime.strptime(end_date, '%Y-%m-%d')
                    end_date_str = dt.strftime('%m/%d/%Y')
                except ValueError:
                    end_date_str = end_date
                logger.info(f"Using end date (string) from database: {end_date_str}")
            else:
                # It's a datetime object
                end_date_str = end_date.strftime('%m/%d/%Y')
                logger.info(f"Using end date from database: {end_date_str}")
        else:
            # Default to today if no end_date provided
            today = datetime.now(local_tz)
            end_date_str = today.strftime('%m/%d/%Y')
            logger.info(f"No end date in database, using today: {end_date_str}")
        
        job_name = f'rdf_job_{client}_{file_id}'
        
        # Select job definition based on category
        if category.lower() == 'small':
            selected_job_definition = job_definition_small
        elif category.lower() == 'large':
            selected_job_definition = job_definition_large
        else:  # medium or any other category
            selected_job_definition = job_definition_default
            
        parameter_value = [
            client, 
            "-f", str(file_id), 
            "-s", start_date_str,
            "-e", end_date_str,
            "-b", rdf_files_s3
        ]
        
        logger.info(f"Submitting job with definition: {selected_job_definition}, priority: {priority}")
        logger.info(f"Command parameters: {parameter_value}")

        # Calculate valid priority value (1-100)
        batch_priority = min(max(int(priority), 1), 100)
        
        # Submit batch job request using the appropriate job definition
        aws_batch_op = BatchOperator(
            task_id=f'aws_batch_task_{file_id}',
            job_name=job_name,
            job_definition=selected_job_definition,
            job_queue=job_queue,
            parameters={},
            wait_for_completion=False,
            overrides={
                'command': parameter_value
            },
            dag=dag
        )
        
        aws_batch_op.execute(context=kwargs)
        
        # Get the job ID from XCom
        aws_batch_job_id = kwargs['ti'].xcom_pull(task_ids=f'aws_batch_task_{file_id}')
        
        if aws_batch_job_id:
            logger.info(f"Successfully submitted AWS Batch job ID: {aws_batch_job_id} for file_id: {file_id}, client: {client}")
        else:
            logger.warning(f"Job submitted but no job ID available for file_id: {file_id}, client: {client}")
            aws_batch_job_id = 'unknown'
        
        # Include category, priority and AWS batch job ID in the tracking record
        insert_values = f"({file_id},'{client}',current_timestamp,'{job_name}',current_timestamp,current_timestamp,current_timestamp,'{category}',{priority},'{aws_batch_job_id}')"
        sql_insert_query = f"""
            ALTER SESSION SET TIMEZONE = 'MST'; 
            INSERT INTO rdf_config.rdf_config.rdf_file_gen_track 
            (File_id, Client, in_ts, job_id, start_ts, end_ts, Gen_start_ts, category, priority, aws_batch_job_id) 
            VALUES {insert_values}
        """

        # Define the snowflake operator to execute sql query to insert data
        snowflake_insert_op = SnowflakeOperator(
            task_id=f'snowflake_insert_task{file_id}',
            sql=sql_insert_query,
            snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user",
            autocommit=True, #enable auto commit
            dag=dag,
        )

        # Execute the function to insert data into rdf_file_gen_track table
        snowflake_insert_op.execute(context=kwargs)

# Define the PythonOperator to execute the SQL query and process the results
execute_sql_operator_column_names = PythonOperator(
    task_id='execute_sql_query_return_rows_task',
    python_callable=execute_sql_query_return_rows,
    dag=dag,
)

# Define the Batch operator to call the rdf file job
aws_batch_op = PythonOperator(
    task_id='execute_aws_batch',
    python_callable=execute_aws_batch,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
execute_sql_operator_column_names >> aws_batch_op