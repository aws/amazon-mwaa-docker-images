# dms_weekly_restart_cycle.py
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from datetime import datetime, timedelta

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
    "dms_weekly_restart_cycle",
    default_args=default_args,
    description="Weekly DMS restart cycle - stops and resumes DMS tasks at 9 AM every Monday, then hourly for 3 hours",
    schedule_interval="1 9-11 * * 1",  # At minute 1 past hours 9, 10, and 11 on Monday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Ensure only one instance runs at a time
) as dag:
    
    # Trigger the DMS stop tasks DAG
    trigger_stop_dms = TriggerDagRunOperator(
        task_id="trigger_dms_stop_tasks",
        trigger_dag_id="dms_stop_tasks",
        wait_for_completion=True,  # Wait for the stop DAG to complete
        poke_interval=30,  # Check every 30 seconds
        allowed_states=[DagRunState.SUCCESS],  # Only proceed if stop DAG succeeds
        failed_states=[DagRunState.FAILED],
        conf={"mode": "cdc_no_dq"}  # Pass the mode parameter to stop DAG
    )
    
    # Trigger the DMS resume tasks DAG
    trigger_resume_dms = TriggerDagRunOperator(
        task_id="trigger_dms_resume_tasks",
        trigger_dag_id="dms_resume_tasks",
        wait_for_completion=True,  # Wait for the resume DAG to complete
        poke_interval=30,  # Check every 30 seconds
        allowed_states=[DagRunState.SUCCESS],  # Only proceed if resume DAG succeeds
        failed_states=[DagRunState.FAILED],
        conf={"mode": "cdc_no_dq"}  # Pass the mode parameter to resume DAG
    )
    
    # Set task dependencies
    trigger_stop_dms >> trigger_resume_dms 