from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pendulum
import logging
import boto3
import time

# Configure logging
logger = logging.getLogger(__name__)

# Instantiate Pendulum and set timezone to MST.
local_tz = pendulum.timezone("America/Phoenix")

# Constants and configuration
CLIENT_BATCH_SIZE = 20  # Number of clients to process in each batch
FILE_BATCH_SIZE = 50    # Number of files to process in each batch when using file-based processing
MAX_JOBS_PER_RUN = 50   # Maximum number of jobs to submit in a single DAG run
CLIENT_JOB_RATIO = 0.7  # Ratio of jobs allocated to client-based processing (70%)
JOB_SUBMISSION_DELAY = 1  # Delay in seconds between job submissions to stagger them

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
    'rdf_file_trigger_hybrid',
    default_args=default_args,
    description='Hybrid DAG that allows both client-based and file-based processing based on file categories',
    schedule_interval=timedelta(minutes=5),
    concurrency=2,  # concurrency - just enough for client and file paths
    max_active_runs=1,
    catchup=False,
)

# Function to query Snowflake and get clients/files to process
def get_items_to_process(**context):
    """Query Snowflake to get clients and files that need processing"""
    logger.info("Querying Snowflake for clients and files to process")
    
    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")
    
    # Query to get total file counts for each client (including already processed files)
    total_file_count_query = """
        SELECT client, COUNT(file_id) as total_file_count
        FROM rdf_config.rdf_config.rdf_file -- Use the table that has ALL files, not just ready to process
        where client not ilike 'x%'
        GROUP BY client
    """
    
    # Query to get clients with their files and metadata
    sql_query = """
        SELECT 
            f.client,
            f.file_id,
            f.start_date,
            f.end_date,
            COALESCE(p.priority, 1) AS priority,
            COALESCE(p.category, 'medium') AS category
        FROM ai_datamart.dm.files_ready_to_process f
        LEFT JOIN ai_datamart.dm.rdf_file_priority p
          ON f.file_id = p.file_id
          AND f.client = p.client
        ORDER BY COALESCE(p.priority, 1) DESC, f.client
    """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Get total file counts for each client
            cursor.execute(total_file_count_query)
            total_file_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get files ready to process
            cursor.execute(sql_query)
            rows = cursor.fetchall()
    
    # Group files by client
    files_by_client = {}
    for row in rows:
        client = row[0]
        if client not in files_by_client:
            files_by_client[client] = []
        files_by_client[client].append(row)
    
    logger.info(f"Retrieved {len(rows)} files across {len(files_by_client)} clients")
    
    # Organize data into client-based and file-based processing groups
    client_based = {
        'small': [],
        'medium': [],
        'large': []
    }
    
    file_based = {
        'small': [],
        'medium': [],
        'large': []
    }
    
    # Track file_ids to ensure no overlap between processing methods
    client_based_file_ids = set()
    file_based_file_ids = set()
    
    # Determine processing method for each client
    for client, files in files_by_client.items():
        # Check categories of files
        file_categories = [file[5].lower() for file in files]
        has_large_files = any(category == 'large' for category in file_categories)
        has_medium_files = any(category == 'medium' for category in file_categories)
        all_files_are_small = all(category == 'small' for category in file_categories)
        
        # Determine client category (used for job definition selection)
        if has_large_files:
            client_category = 'large'
        elif has_medium_files:
            client_category = 'medium'
        else:
            client_category = 'small'
        
        # Calculate client-level metadata
        file_count = len(files)
        start_date = files[0][2]
        end_date = files[0][3]
        priority = max([file[4] for file in files])
        
        # Check if we're processing all files for this client or just a subset
        total_count = total_file_counts.get(client, file_count)
        processing_all_files = (file_count == total_count)
        
        # If the client has no large files AND we're processing ALL files, use client-based
        # Otherwise, use file-level processing
        if not has_large_files and processing_all_files:
            # Use client-based processing for clients with only small/medium files
            job_def_category = 'medium' if has_medium_files else 'small'
            logger.info(f"Client {client} with {file_count} files assigned to client-based processing (category: {job_def_category})")
            client_data = (client, file_count, start_date, end_date, priority, job_def_category, files)
            client_based[job_def_category].append(client_data)
            
            # Track file_ids for validation
            for file in files:
                client_based_file_ids.add(file[1])
        else:
            # Determine reason for file-based processing
            reason = "contains large files" if has_large_files else "partial processing (only processing some files)"
            logger.info(f"Client {client} with {file_count}/{total_count} files assigned to file-based processing ({reason})")
            
            # Add each file to file-based processing
            for file in files:
                file_id = file[1]
                file_priority = file[4]
                file_category = file[5].lower()
                file_data = (file_id, client, start_date, end_date, file_priority, file_category)
                file_based[file_category].append(file_data)
                
                # Track file_ids for validation
                file_based_file_ids.add(file_id)
    
    # Validate that no file appears in both processing methods
    overlap = client_based_file_ids.intersection(file_based_file_ids)
    if overlap:
        error_msg = f"ERROR: Found {len(overlap)} files assigned to both processing methods: {list(overlap)[:5]}..."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Create batches for client-based processing
    client_batches = {}
    for category, clients in client_based.items():
        client_batches[category] = [clients[i:i+CLIENT_BATCH_SIZE] for i in range(0, len(clients), CLIENT_BATCH_SIZE)]
        logger.info(f"Client-based - Category '{category}': {len(clients)} clients divided into {len(client_batches[category])} batches")
    
    # Create batches for file-based processing
    file_batches = {}
    for category, files in file_based.items():
        file_batches[category] = [files[i:i+FILE_BATCH_SIZE] for i in range(0, len(files), FILE_BATCH_SIZE)]
        logger.info(f"File-based - Category '{category}': {len(files)} files divided into {len(file_batches[category])} batches")
    
    # Log totals for both processing methods
    total_client_based_files = len(client_based_file_ids)
    total_file_based_files = len(file_based_file_ids)
    total_files = total_client_based_files + total_file_based_files
    
    logger.info(f"Processing allocation summary: {total_client_based_files} files via client-based processing, " 
               f"{total_file_based_files} files via file-based processing")
    logger.info(f"Total files to process: {total_files}")
    
    # Store both processing groups
    result = {
        'client_based': client_batches,
        'file_based': file_batches
    }
    
    return result

# Function to set up AWS client
def setup_aws_client(**context):
    """Create and return an AWS Batch client"""
    # Get AWS Batch job details from Airflow variables
    job_definition_default = Variable.get("Aws_batch_definition")
    
    try:
        job_definition_small = Variable.get("Aws_batch_definition_small")
    except (KeyError, ValueError):
        job_definition_small = job_definition_default

    try:
        job_definition_large = Variable.get("Aws_batch_definition_large") 
    except (KeyError, ValueError):
        job_definition_large = job_definition_default

    job_queue = Variable.get("aws_batch_job_queue") 
    rdf_files_s3 = Variable.get("rdf_files_s3")
    
    # Store these values in XCom for downstream tasks
    context['ti'].xcom_push(key='job_definition_default', value=job_definition_default)
    context['ti'].xcom_push(key='job_definition_small', value=job_definition_small)
    context['ti'].xcom_push(key='job_definition_large', value=job_definition_large)
    context['ti'].xcom_push(key='job_queue', value=job_queue)
    context['ti'].xcom_push(key='rdf_files_s3', value=rdf_files_s3)
    
    # Create boto3 client but don't return it directly
    # Instead, create it when needed in the processing functions
    logger.info("AWS Batch configuration set up successfully")
    
    # Return a serializable value instead of the client object
    return {
        "job_definition_default": job_definition_default,
        "job_definition_small": job_definition_small,
        "job_definition_large": job_definition_large,
        "job_queue": job_queue,
        "rdf_files_s3": rdf_files_s3
    }

# Helper function to format dates
def format_date(date_value, timezone):
    """Format date values consistently"""
    if not date_value:
        # Default to yesterday if no date provided
        yesterday = datetime.now(timezone) - timedelta(days=1)
        return yesterday.strftime('%m/%d/%Y')
    
    if isinstance(date_value, str):
        try:
            # Try to parse if it's in a common format
            dt = datetime.strptime(date_value, '%Y-%m-%d')
            return dt.strftime('%m/%d/%Y')
        except ValueError:
            # If parsing fails, just use it as is
            return date_value
    else:
        # It's a datetime object
        return date_value.strftime('%m/%d/%Y')

# Function to process batches (combines client and file based processing)
def process_batches(**context):
    """Process all batches based on the data from get_items_task"""
    ti = context['ti']
    
    # Get batches from get_items_task
    all_batches = ti.xcom_pull(task_ids='get_items_task')
    if not all_batches:
        logger.warning("No batches found from get_items_task")
        return {
            "client_batches_processed": 0,
            "file_batches_processed": 0,
            "clients_processed": 0,
            "files_processed": 0
        }
    
    # Get AWS configuration from previous task
    aws_config = ti.xcom_pull(task_ids='setup_aws_client_task')
    if not aws_config:
        logger.error("AWS configuration not found from setup_aws_client_task")
        return {
            "error": "AWS configuration not found",
            "client_batches_processed": 0,
            "file_batches_processed": 0,
            "clients_processed": 0,
            "files_processed": 0
        }
    
    job_definition_default = aws_config.get('job_definition_default')
    job_definition_small = aws_config.get('job_definition_small')
    job_definition_large = aws_config.get('job_definition_large')
    job_queue = aws_config.get('job_queue')
    rdf_files_s3 = aws_config.get('rdf_files_s3')
    
    # Create AWS Batch client
    batch_client = boto3.client('batch')
    
    # Get Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id="scrt_conn_snowflake_conn_ai_dbt_user")
    
    # Job throttling counters
    jobs_submitted = 0
    client_jobs_submitted = 0
    file_jobs_submitted = 0
    
    # Calculate job limits based on ratio
    max_client_jobs = int(MAX_JOBS_PER_RUN * CLIENT_JOB_RATIO)  # 70% for client jobs
    max_file_jobs = MAX_JOBS_PER_RUN - max_client_jobs  # 30% for file jobs
    
    logger.info(f"Job allocation: {max_client_jobs} client-based jobs (70%), {max_file_jobs} file-based jobs (30%)")
    
    # Process client-based batches
    client_batches = all_batches.get('client_based', {})
    client_batches_processed = 0
    clients_processed = 0
    client_files_processed = 0
    
    # Track results for summary
    results = {
        "client_based": {
            "total": 0,
            "successful": 0,
            "failed": 0,
            "total_files": 0
        },
        "file_based": {
            "total": 0,
            "successful": 0,
            "failed": 0
        }
    }
    
    # Process all client batches
    client_throttled = False
    for category, batches in client_batches.items():
        for batch_index, batch in enumerate(batches):
            if not batch:  # Skip empty batches
                continue
                
            logger.info(f"Processing client batch {batch_index} for category '{category}' with {len(batch)} clients")
            client_batches_processed += 1
            
            # Store successful jobs for bulk insert
            successful_jobs = []
            successful_clients = []
            failed_clients = []
            
            # Process each client in the batch
            for client_data in batch:
                # Check if we've reached the client job limit
                if client_jobs_submitted >= max_client_jobs:
                    logger.warning(f"Reached client job limit of {max_client_jobs}. Moving to file-based processing.")
                    client_throttled = True
                    break
                    
                client = client_data[0]         # Client name
                file_count = client_data[1]     # File count
                start_date = client_data[2]     # Start date
                end_date = client_data[3]       # End date
                priority = client_data[4]       # Priority
                category = client_data[5]       # Category
                files = client_data[6]          # List of files for this client
                
                # Extract file_ids
                file_ids = [file[1] for file in files]
                
                try:
                    # Format dates
                    start_date_str = format_date(start_date, local_tz)
                    end_date_str = format_date(end_date, local_tz)
                    
                    # Add debug logs about dates
                    logger.info(f"Using date range for client {client}: {start_date_str} to {end_date_str}")
                    
                    # Create job parameters
                    job_name = f'rdf_job_{client}'
                    
                    # Select job definition based on category
                    if category.lower() == 'small':
                        selected_job_definition = job_definition_small
                    elif category.lower() == 'large':
                        selected_job_definition = job_definition_large
                    else:  # medium or any other category
                        selected_job_definition = job_definition_default
                        
                    # Do NOT include "-f" parameter to process all files for the client
                    parameter_value = [
                        client, 
                        "-s", start_date_str,
                        "-e", end_date_str,
                        "-b", rdf_files_s3
                    ]
                    
                    logger.info(f"Submitting client job with definition: {selected_job_definition}, priority: {priority}")
                    logger.info(f"Command parameters: {parameter_value}")
                    
                    # Submit AWS Batch job
                    response = batch_client.submit_job(
                        jobName=job_name,
                        jobQueue=job_queue,
                        jobDefinition=selected_job_definition,
                        containerOverrides={
                            'command': parameter_value
                        }
                    )
                    
                    # Increment job counters
                    jobs_submitted += 1
                    client_jobs_submitted += 1
                    
                    # Get job ID
                    aws_batch_job_id = response['jobId']
                    
                    # Add staggered delay between job submissions
                    logger.info(f"Submitted AWS Batch job {aws_batch_job_id} for client {client}")
                    logger.info(f"Sleeping for {JOB_SUBMISSION_DELAY} seconds to stagger job submissions")
                    time.sleep(JOB_SUBMISSION_DELAY)
                    
                    # Immediately insert tracking records to Snowflake for each file
                    # This prevents duplicate processing if later inserts fail
                    with hook.get_conn() as conn:
                        with conn.cursor() as cursor:
                            try:
                                # Build values for all files in this client job
                                values_list = []
                                for file_id in file_ids:
                                    values_list.append(
                                        f"({file_id},'{client}',current_timestamp,'{job_name}',current_timestamp,"
                                        f"current_timestamp,current_timestamp,'{category}',{priority},'{aws_batch_job_id}')"
                                    )
                                
                                # Execute as separate statements to avoid the multi-statement error
                                # First set the timezone
                                cursor.execute("ALTER SESSION SET TIMEZONE = 'MST'")
                                
                                # Then do the insert
                                insert_query = f"""
                                    INSERT INTO rdf_config.rdf_config.rdf_file_gen_track 
                                    (File_id, Client, in_ts, job_id, start_ts, end_ts, Gen_start_ts, category, priority, aws_batch_job_id) 
                                    VALUES {','.join(values_list)}
                                """
                                cursor.execute(insert_query)
                                logger.info(f"Successfully inserted {len(file_ids)} tracking records for client {client}")
                            except Exception as e:
                                logger.error(f"Error inserting tracking records for client {client}: {str(e)}")
                                # Raise exception to fail the DAG and prevent duplicate job submissions
                                raise Exception(f"Failed to insert job tracking records for client {client}: {str(e)}")
                    
                    successful_clients.append((client, len(file_ids)))
                    logger.info(f"Successfully submitted and tracked AWS Batch job ID: {aws_batch_job_id} for client: {client} with {len(file_ids)} files")
                    
                    # Update counts
                    clients_processed += 1
                    client_files_processed += len(file_ids)
                    results["client_based"]["successful"] += 1
                    results["client_based"]["total_files"] += len(file_ids)
                    
                except Exception as e:
                    failed_clients.append(client)
                    logger.error(f"Error submitting job for client {client}: {str(e)}")
                    results["client_based"]["failed"] += 1
                    raise Exception(f"Failed to submit AWS Batch job for client {client}: {str(e)}")
            
            # Remove bulk insert logic since we're now inserting individually
            # Log summary for this batch
            total_files = sum(files for _, files in successful_clients)
            logger.info(f"Client batch summary: Processed {len(batch)} clients, {len(successful_clients)} successful, {len(failed_clients)} failed")
            logger.info(f"Successfully processed {total_files} files across {len(successful_clients)} clients")
            if failed_clients:
                logger.warning(f"Failed clients: {failed_clients}")
            
            results["client_based"]["total"] += len(batch)
            
            # Stop processing more batches if we've reached the client job limit
            if client_throttled:
                break
                
        # Stop processing more categories if we've reached the client job limit
        if client_throttled:
            break
    
    # Process file-based batches
    file_batches = all_batches.get('file_based', {})
    file_batches_processed = 0
    files_processed = 0
    file_throttled = False
    
    # Process all file batches
    for category, batches in file_batches.items():
        for batch_index, batch in enumerate(batches):
            if not batch:  # Skip empty batches
                continue
                
            logger.info(f"Processing file batch {batch_index} for category '{category}' with {len(batch)} files")
            file_batches_processed += 1
            
            # Store successful jobs for bulk insert
            successful_jobs = []
            successful_files = []
            failed_files = []
            
            # Process each file in the batch
            for file_data in batch:
                # Check if we've reached the file job limit or overall job limit
                if file_jobs_submitted >= max_file_jobs or jobs_submitted >= MAX_JOBS_PER_RUN:
                    logger.warning(f"Reached file job limit of {max_file_jobs} or overall limit of {MAX_JOBS_PER_RUN}. Stopping further processing.")
                    file_throttled = True
                    break
                    
                file_id = file_data[0]       # File ID
                client = file_data[1]        # Client name
                start_date = file_data[2]    # Start date
                end_date = file_data[3]      # End date
                priority = file_data[4]      # Priority
                category = file_data[5]      # Category
                
                try:
                    # Format dates
                    start_date_str = format_date(start_date, local_tz)
                    end_date_str = format_date(end_date, local_tz)
                    
                    # Create job parameters
                    job_name = f'rdf_job_{client}_{file_id}'
                    
                    # Select job definition based on category
                    if category.lower() == 'small':
                        selected_job_definition = job_definition_small
                    elif category.lower() == 'large':
                        selected_job_definition = job_definition_large
                    else:  # medium or any other category
                        selected_job_definition = job_definition_default
                        
                    # Include "-f" parameter for specific file processing
                    parameter_value = [
                        client, 
                        "-f", str(file_id),
                        "-s", start_date_str,
                        "-e", end_date_str,
                        "-b", rdf_files_s3
                    ]
                    
                    logger.info(f"Submitting file job with definition: {selected_job_definition}, priority: {priority}")
                    logger.info(f"Command parameters: {parameter_value}")
                    
                    # Submit AWS Batch job
                    response = batch_client.submit_job(
                        jobName=job_name,
                        jobQueue=job_queue,
                        jobDefinition=selected_job_definition,
                        containerOverrides={
                            'command': parameter_value
                        }
                    )
                    
                    # Increment job counters
                    jobs_submitted += 1
                    file_jobs_submitted += 1
                    
                    # Get job ID
                    aws_batch_job_id = response['jobId']
                    
                    # Add staggered delay between job submissions
                    logger.info(f"Submitted AWS Batch job {aws_batch_job_id} for file_id {file_id}, client {client}")
                    logger.info(f"Sleeping for {JOB_SUBMISSION_DELAY} seconds to stagger job submissions")
                    time.sleep(JOB_SUBMISSION_DELAY)
                    
                    # Immediately insert tracking record to Snowflake
                    # This prevents duplicate processing if later inserts fail
                    with hook.get_conn() as conn:
                        with conn.cursor() as cursor:
                            try:
                                # Execute as separate statements to avoid the multi-statement error
                                # First set the timezone
                                cursor.execute("ALTER SESSION SET TIMEZONE = 'MST'")
                                
                                # Then do the insert
                                insert_query = f"""
                                    INSERT INTO rdf_config.rdf_config.rdf_file_gen_track 
                                    (File_id, Client, in_ts, job_id, start_ts, end_ts, Gen_start_ts, category, priority, aws_batch_job_id) 
                                    VALUES ({file_id},'{client}',current_timestamp,'{job_name}',current_timestamp,current_timestamp,current_timestamp,'{category}',{priority},'{aws_batch_job_id}')
                                """
                                cursor.execute(insert_query)
                                logger.info(f"Successfully inserted tracking record for file_id {file_id}")
                            except Exception as e:
                                logger.error(f"Error inserting tracking record for file_id {file_id}: {str(e)}")
                                # Raise exception to fail the DAG and prevent duplicate job submissions
                                raise Exception(f"Failed to insert job tracking record for file_id {file_id}: {str(e)}")
                    
                    successful_files.append(file_id)
                    logger.info(f"Successfully submitted and tracked AWS Batch job ID: {aws_batch_job_id} for file_id: {file_id}, client: {client}")
                    
                    # Update counts
                    files_processed += 1
                    results["file_based"]["successful"] += 1
                    
                except Exception as e:
                    failed_files.append(file_id)
                    logger.error(f"Error submitting job for file_id {file_id}: {str(e)}")
                    results["file_based"]["failed"] += 1
                    raise Exception(f"Failed to submit AWS Batch job for file_id {file_id}: {str(e)}")
            
            # Remove bulk insert logic since we're now inserting individually
            # Log summary for this batch
            logger.info(f"File batch summary: Processed {len(batch)} files, {len(successful_files)} successful, {len(failed_files)} failed")
            if failed_files:
                logger.warning(f"Failed file_ids: {failed_files}")
            
            results["file_based"]["total"] += len(batch)
            
            # Stop processing more batches if we've reached the file job limit
            if file_throttled:
                break
                
        # Stop processing more categories if we've reached the file job limit
        if file_throttled:
            break
    
    # Overall summary
    jobs_throttled = client_throttled or file_throttled
    
    logger.info("========== Processing Summary ==========")
    logger.info(f"Client-based processing: {results['client_based']['total']} clients, {results['client_based']['successful']} successful, {results['client_based']['failed']} failed")
    logger.info(f"Files processed via client-based method: {results['client_based']['total_files']}")
    logger.info(f"File-based processing: {results['file_based']['total']} files, {results['file_based']['successful']} successful, {results['file_based']['failed']} failed")
    logger.info(f"Total files processed: {results['client_based']['total_files'] + results['file_based']['successful']}")
    logger.info(f"Job allocation: {client_jobs_submitted}/{max_client_jobs} client jobs, {file_jobs_submitted}/{max_file_jobs} file jobs")
    logger.info(f"Total jobs submitted: {jobs_submitted}/{MAX_JOBS_PER_RUN}")
    if jobs_throttled:
        logger.info("Job submission was throttled - remaining items will be processed in subsequent runs")
    logger.info("======================================")
    
    return {
        "client_batches_processed": client_batches_processed,
        "file_batches_processed": file_batches_processed,
        "clients_processed": clients_processed,
        "files_processed": files_processed,
        "client_jobs_submitted": client_jobs_submitted,
        "file_jobs_submitted": file_jobs_submitted,
        "total_jobs_submitted": jobs_submitted,
        "jobs_throttled": jobs_throttled,
        "results": results
    }

# Define tasks
get_items_task = PythonOperator(
    task_id='get_items_task',
    python_callable=get_items_to_process,
    dag=dag,
)

setup_aws_client_task = PythonOperator(
    task_id='setup_aws_client_task',
    python_callable=setup_aws_client,
    dag=dag,
)

# Single task to process all batches
process_batches_task = PythonOperator(
    task_id='process_batches',
    python_callable=process_batches,
    dag=dag,
)

# Define dependencies - Simplified flow with single process task
get_items_task >> setup_aws_client_task >> process_batches_task 