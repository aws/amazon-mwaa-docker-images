# stop_dms_tasks_dag.py
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError
import time
from dateutil import parser

# Initialize variables
AWS_REGION = "us-east-1"
DMS_ROLE_ARN = Variable.get("task_automation_dms_role_arn")   

def assume_dms_role():
    try:
        print(f"Attempting to assume role: {DMS_ROLE_ARN}")
        sts_client = boto3.client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=DMS_ROLE_ARN,
            RoleSessionName='DMSOperationSession'
        )
        print("Successfully retrieved temporary credentials")
        
        # Create a DMS client with the temporary credentials
        dms_client = boto3.client(
            'dms',
            region_name=AWS_REGION,
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )
        print("Successfully created DMS client with assumed role credentials")
        return dms_client
        
    except ClientError as e:
        print(f"Error assuming role: {str(e)}")
        raise e

def get_running_parameters(**kwargs):
    """Extract runtime parameters from DAG run configuration"""
    dag_run_conf = kwargs['dag_run'].conf
    default_args = kwargs['dag'].default_args

    params = {**default_args, **dag_run_conf} if dag_run_conf else default_args

    # Get mode parameter with default fallback
    mode = params.get('mode', 'cdc_no_dq')
    
    # Validate mode parameter
    valid_modes = ['all_cdc', 'dq_only', 'cdc_no_dq']
    if mode not in valid_modes:
        print(f"Invalid mode '{mode}'. Using default 'cdc_no_dq'. Valid modes: {valid_modes}")
        mode = 'cdc_no_dq'
    
    print(f"Mode parameter: {mode}")
    
    # Push mode to XCom for use by other tasks
    kwargs['ti'].xcom_push(key='mode', value=mode)
    
    return mode

def _should_include_task(task, mode):
    """Helper function to determine if a task should be included based on mode"""
    task_identifier = task.get('ReplicationTaskIdentifier', '').lower()
    is_cdc_task = 'cdc' in task.get('MigrationType', '').lower()
    is_dq_task = 'dq' in task_identifier
    
    if not is_cdc_task:
        return False
    
    return {
        'all_cdc': True,
        'dq_only': is_dq_task,
        'cdc_no_dq': not is_dq_task
    }.get(mode, False)



def _get_all_replication_tasks(client):
    """Generator that yields all replication tasks, handling pagination automatically"""
    marker = None
    while True:
        if marker:
            response = client.describe_replication_tasks(Marker=marker)
        else:
            response = client.describe_replication_tasks()
        
        # Yield all tasks from this batch
        for task in response['ReplicationTasks']:
            yield task
        
        # Check if there are more pages
        marker = response.get('Marker')
        if not marker:
            break

def get_cdc_tasks_with_status(mode):
    """Get CDC tasks based on the configured mode"""
    try:
        client = assume_dms_role()
        print(f"Retrieving all replication tasks (mode: {mode})")
        
        cdc_tasks = []
        for task in _get_all_replication_tasks(client):
            if _should_include_task(task, mode):
                task_info = {
                    'arn': task['ReplicationTaskArn'],
                    'identifier': task.get('ReplicationTaskIdentifier', '').lower(),
                    'status': task['Status'].lower()
                }
                cdc_tasks.append(task_info)
                print(f"Found CDC task: {task_info['arn']} with status: {task_info['status']}")
        
        print(f"Total CDC tasks found (mode={mode}): {len(cdc_tasks)}")
        return cdc_tasks
        
    except Exception as e:
        print(f"Error getting CDC tasks: {str(e)}")
        raise e

def stop_all_cdc_tasks(**kwargs):
    try:
        # Get mode from XCom
        mode = kwargs['ti'].xcom_pull(task_ids='get_parameters', key='mode')
        
        # Calculate stop position directly
        current_time = datetime.utcnow()
        stop_position = current_time.isoformat() + "Z"  # Current time in ISO format
        
        print("Starting stop_all_cdc_tasks function")
        print(f"Using mode: {mode}")
        print(f"Calculated stop position: {stop_position}")
        
        # Get all CDC tasks with their status based on mode
        cdc_tasks_with_status = get_cdc_tasks_with_status(mode)
        
        # Filter tasks that are in a running state
        running_states = ['running']#['starting', 'running', 'ready', 'modifying']
        tasks_to_stop = [task for task in cdc_tasks_with_status if task['status'] in running_states]
        
        if not tasks_to_stop:
            print("No CDC tasks are currently running. Nothing to stop.")
            # Push empty list to XCom for verification task
            kwargs['ti'].xcom_push(key='stopped_task_arns', value=[])
            return "No running CDC tasks found"
        
        print(f"Found {len(tasks_to_stop)} running CDC tasks that need to be stopped")
        
        # Wait a fixed 60 seconds before proceeding
        print("Waiting 60 seconds before proceeding...")
        time.sleep(60)
        
        stopped_task_arns = []
        
        # Stop all running CDC tasks
        for task in tasks_to_stop:
            try:
                dms_task_arn = task['arn']
                client = assume_dms_role()  # Get a fresh client for each task to avoid token expiration
                print(f"Stopping the DMS task: {dms_task_arn} (current status: {task['status']})")
                client.stop_replication_task(ReplicationTaskArn=dms_task_arn)
                print(f"DMS task {dms_task_arn} stop initiated successfully.")
                stopped_task_arns.append(dms_task_arn)
                
            except Exception as task_error:
                print(f"Error stopping task {dms_task_arn}: {str(task_error)}")
                # Continue with other tasks even if one fails
        
        # Push the list of stopped task ARNs to XCom for verification
        kwargs['ti'].xcom_push(key='stopped_task_arns', value=stopped_task_arns)
        print(f"Pushed {len(stopped_task_arns)} task ARNs to XCom for verification")
       
        return f"Processed {len(tasks_to_stop)} CDC tasks, successfully initiated stop for {len(stopped_task_arns)} tasks"
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"AWS Error - Code: {error_code}, Message: {error_message}")
        raise e
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        raise e

def verify_tasks_stopped(**kwargs):
    """Verify that all stopped tasks have actually reached the 'stopped' status"""
    try:
        # Get the list of stopped task ARNs from XCom
        stopped_task_arns = kwargs['ti'].xcom_pull(task_ids='stop_all_cdc_tasks', key='stopped_task_arns')
        
        if not stopped_task_arns:
            print("No tasks were stopped, verification complete.")
            return "No tasks to verify"
        
        print(f"Verifying status of {len(stopped_task_arns)} stopped tasks")
        
        # Configuration for monitoring
        max_wait_time = 1800  # 30 minutes maximum wait time
        check_interval = 60   # Check every 60 seconds
        start_time = time.time()
        
        client = assume_dms_role()
        
        while True:
            elapsed_time = time.time() - start_time
            
            # Check if we've exceeded the maximum wait time
            if elapsed_time > max_wait_time:
                print(f"Timeout exceeded ({max_wait_time} seconds). Not all tasks have stopped.")
                
                # Get current status of all tasks
                still_running = []
                for task_arn in stopped_task_arns:
                    try:
                        response = client.describe_replication_tasks(
                            Filters=[
                                {
                                    'Name': 'replication-task-arn',
                                    'Values': [task_arn]
                                }
                            ]
                        )
                        if response['ReplicationTasks']:
                            current_status = response['ReplicationTasks'][0]['Status'].lower()
                            if current_status not in ['stopped', 'failed']:
                                still_running.append(f"{task_arn} (status: {current_status})")
                    except Exception as e:
                        print(f"Error checking status of task {task_arn}: {str(e)}")
                        still_running.append(f"{task_arn} (error checking status)")
                
                if still_running:
                    error_msg = f"The following tasks have not stopped after {max_wait_time} seconds:\n" + "\n".join(still_running)
                    print(error_msg)
                    raise Exception(error_msg)
                else:
                    print("All tasks have now stopped successfully!")
                    break
            
            # Check status of all stopped tasks
            all_stopped = True
            task_statuses = []
            
            for task_arn in stopped_task_arns:
                try:
                    # Get fresh client to avoid token expiration on long waits
                    if elapsed_time > 0 and elapsed_time % 900 == 0:  # Refresh every 15 minutes
                        client = assume_dms_role()
                    
                    response = client.describe_replication_tasks(
                        Filters=[
                            {
                                'Name': 'replication-task-arn',
                                'Values': [task_arn]
                            }
                        ]
                    )
                    
                    if response['ReplicationTasks']:
                        current_status = response['ReplicationTasks'][0]['Status'].lower()
                        task_statuses.append(f"{task_arn}: {current_status}")
                        
                        # Consider both 'stopped' and 'failed' as terminal states
                        if current_status not in ['stopped', 'failed']:
                            all_stopped = False
                    else:
                        print(f"Warning: Task {task_arn} not found in describe_replication_tasks response")
                        task_statuses.append(f"{task_arn}: NOT_FOUND")
                        
                except Exception as e:
                    print(f"Error checking status of task {task_arn}: {str(e)}")
                    task_statuses.append(f"{task_arn}: ERROR")
                    all_stopped = False
            
            print(f"Current task statuses (elapsed: {int(elapsed_time)}s):")
            for status in task_statuses:
                print(f"  {status}")
            
            if all_stopped:
                print("All tasks have stopped successfully!")
                return f"All {len(stopped_task_arns)} tasks have stopped successfully"
            
            print(f"Some tasks are still stopping. Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)
            
    except Exception as e:
        print(f"Error in verify_tasks_stopped: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")
        raise e

# Define the default arguments for the DAG
default_args = {
    "owner": "BI",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "dms_stop_tasks",
    default_args=default_args,
    description="DAG to stop CDC tasks - supports modes via runtime parameters: all_cdc, dq_only, cdc_no_dq",
    schedule_interval=None,  # Set to None to disable scheduling - run manually only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    concurrency=1,  # Limit concurrent task execution
    max_active_runs=1,  # Ensure only one instance runs at a time
) as dag:
    
    # Task to extract runtime parameters
    get_parameters = PythonOperator(
        task_id="get_parameters",
        python_callable=get_running_parameters,
        provide_context=True
    )
    
    # Create a task to stop CDC tasks based on parameters
    stop_all_tasks = PythonOperator(
        task_id="stop_all_cdc_tasks",
        python_callable=stop_all_cdc_tasks,
        provide_context=True
    )
    
    # Task to verify that all stopped tasks have actually stopped
    verify_stopped = PythonOperator(
        task_id="verify_tasks_stopped",
        python_callable=verify_tasks_stopped,
        provide_context=True
    )
    
    # Set task dependencies
    get_parameters >> stop_all_tasks >> verify_stopped