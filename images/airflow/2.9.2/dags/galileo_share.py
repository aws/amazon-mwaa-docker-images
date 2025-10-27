from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import pendulum
import logging
import boto3

# Debug log: Starting DAG initialization
logging.info("Initializing galileo_share DAG")

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")
snowflake_conn_id = 'scrt_conn_snowflake_conn_ai_dbt_user'

# Define AWS Batch job parameters
job_definition = Variable.get("Aws_batch_definition") 
job_queue = Variable.get("aws_batch_job_queue") 
rdf_files_s3 = Variable.get("rdf_files_s3")

# Debug log: Variables loaded
logging.info(f"AWS Batch configuration loaded - job_def: {job_definition}, job_queue: {job_queue}")

# Constants for job processing

def get_pending_files():
    """
    Query Snowflake view to get pending file generation requests
    Returns:
        list: List of tuples containing (client, file_id, filename)
    """
    logging.info("Querying Snowflake for pending file generation requests")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Query to get pending files from view (replace with your actual view name)
    sql_query = """
        SELECT client, file_id, filename
        FROM AI_DATAMART.DM.PENDING_GAL_SHARE_FILES_TODAY
        ORDER BY client, file_id
    """
    
    cursor = hook.get_conn().cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    
    logging.info(f"Retrieved {len(rows)} pending files for processing")
    
    # Return all the rows 
    return rows

def process_file_submissions(**kwargs):
    """
    Process each file: submit AWS Batch job and insert tracking record
    Uses enhanced approach from hybrid DAG with better error handling
    """
    # Get pending files from previous task
    result = kwargs['ti'].xcom_pull(task_ids='get_pending_files_task')
    if not result:
        logging.warning("No pending files found for processing")
        return {
            "files_processed": 0,
            "jobs_submitted": 0,
            "successful": 0,
            "failed": 0
        }
    
    logging.info(f"Retrieved {len(result)} files to process")

    # Create AWS Batch client (enhanced approach from hybrid DAG)
    batch_client = boto3.client('batch')
    
    # Get Snowflake connection for tracking inserts
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Processing counters
    jobs_submitted = 0
    successful_jobs = 0
    failed_jobs = 0

    for row in result:
        # Process row and create AWS Batch job parameters
        client = row[0]
        file_id = row[1] 
        filename = row[2]
        
        try:
            job_name = f'galileo_share_job_{client}_{file_id}'
            
            # Build command parameters for AWS Batch job
            parameter_value = [
                client,
                "-f", str(file_id),
                "-b", rdf_files_s3,
                "--frequency","onetime"
            ]
            
            logging.info(f"Submitting job for client: {client}, file_id: {file_id}, filename: {filename}")
            logging.info(f"Command parameters: {parameter_value}")

            # Submit AWS Batch job using boto3 client (enhanced approach from hybrid DAG)
            response = batch_client.submit_job(
                jobName=job_name,
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    'command': parameter_value
                }
            )
            
            # Get AWS Batch job ID from response (enhanced approach)
            aws_batch_job_id = response['jobId']
            jobs_submitted += 1
            
            logging.info(f"Submitted AWS Batch job {aws_batch_job_id} for file_id {file_id}")
            
            # Insert tracking record to Snowflake immediately after job submission
            # This prevents duplicate processing if later jobs fail (enhanced error handling from hybrid)
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    try:
                        # Execute as separate statements to avoid multi-statement error (enhanced approach)
                        # First set the timezone
                        cursor.execute("ALTER SESSION SET TIMEZONE = 'MST'")
                        
                        # Then do the insert
                        insert_query = f"""
                            INSERT INTO rdf_config.rdf_config.rdf_file_gen_track 
                            (File_id, Client, in_ts, job_id, start_ts, end_ts, Gen_start_ts, aws_batch_job_id) 
                            VALUES ({file_id},'{client}',current_timestamp,'{job_name}',current_timestamp,current_timestamp,current_timestamp,'{aws_batch_job_id}')
                        """
                        cursor.execute(insert_query)
                        
                        logging.info(f"Successfully inserted tracking record for file_id {file_id}")
                        successful_jobs += 1
                        
                    except Exception as e:
                        logging.error(f"Error inserting tracking record for file_id {file_id}: {str(e)}")
                        # Raise exception to fail the DAG and prevent duplicate job submissions (enhanced error handling)
                        raise Exception(f"Failed to insert job tracking record for file_id {file_id}: {str(e)}")
            
            logging.info(f"Successfully submitted and tracked AWS Batch job ID: {aws_batch_job_id} for file_id: {file_id}, client: {client}")
                
        except Exception as e:
            failed_jobs += 1
            logging.error(f"Error processing file_id {file_id}: {str(e)}")
            # Re-raise to fail the DAG on any error (enhanced error handling)
            raise Exception(f"Failed to process file_id {file_id}: {str(e)}")
    
    # Summary logging (enhanced reporting from hybrid DAG)
    logging.info("========== File Processing Summary ==========")
    logging.info(f"Total pending files: {len(result)}")
    logging.info(f"Jobs submitted: {jobs_submitted}")
    logging.info(f"Successful submissions: {successful_jobs}")
    logging.info(f"Failed submissions: {failed_jobs}")
    logging.info("============================================")
    
    return {
        "files_processed": len(result),
        "jobs_submitted": jobs_submitted,
        "successful": successful_jobs,
        "failed": failed_jobs
    }







# DAG Definition
with DAG(
    "galileo_share",
    default_args={
        'owner': 'BI',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    description='Processes Galileo Share client data and submits AWS Batch job',
    schedule_interval="0 2 * * *",  # Daily at 2 AM MST
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    start_date=datetime(2025, 1, 6, tzinfo=local_tz),
    tags=['BI', 'GALILEO_SHARE'],
) as dag:

    # Debug log: DAG creation
    logging.info("Creating galileo_share DAG tasks")

    # Task to get pending files from Snowflake view
    get_pending_files_task = PythonOperator(
        task_id='get_pending_files_task',
        python_callable=get_pending_files,
        dag=dag,
    )

    # Task to process file submissions and insert tracking records
    process_file_submissions_task = PythonOperator(
        task_id='process_file_submissions_task',
        python_callable=process_file_submissions,
        provide_context=True,
        dag=dag,
    )

    # Define task dependencies
    get_pending_files_task >> process_file_submissions_task

# Debug log: DAG creation complete
logging.info("galileo_share DAG creation complete") 