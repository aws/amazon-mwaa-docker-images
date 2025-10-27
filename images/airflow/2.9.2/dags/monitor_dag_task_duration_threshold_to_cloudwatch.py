from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from airflow.settings import Session
from datetime import datetime, timedelta
import boto3
import pendulum

local_tz = pendulum.timezone("America/Phoenix")

def emit_metric_to_cloudwatch(**context):
    session = Session()
    now = pendulum.now("UTC")
    threshold_time = now.subtract(minutes=5)
    print(f"Current time (UTC): {now}")

    print(f"Threshold time (UTC): {threshold_time}")

    # Find the gal_raw_dedup task instance that has been running > 50 mins
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == "bi_dedup_data",
            TaskInstance.task_id == "gal_raw_dedup",
            TaskInstance.state == State.RUNNING,
            TaskInstance.start_date <= threshold_time,
        )
        .order_by(TaskInstance.start_date.desc())
        .first()
    )

    if ti:
        print(f"Task {ti.task_id} has been running longer than 5 mins. Start time: {ti.start_date}")

        # Send CloudWatch metric
        cloudwatch = boto3.client("cloudwatch")
        response = cloudwatch.put_metric_data(
            Namespace="MWAA/SLA",
            MetricData=[
                {
                    "MetricName": "GalRawDedupRunningOverTime",
                    "Dimensions": [
                        {"Name": "DAGName", "Value": ti.dag_id},
                        {"Name": "TaskName", "Value": ti.task_id}
                    ],
                    "Timestamp": now,
                    "Value": 1,
                    "Unit": "Count"
                }
            ]
        )
        print("CloudWatch metric sent:", response)
    else:
        print("No long-running gal_raw_dedup task found.")

with DAG(
    dag_id="monitor_dag_task_duration_threshold_to_cloudwatch",
    schedule_interval="46 * * * *",  # run at 51st minute
    start_date=days_ago(1),
    catchup=False,
    tags=["monitoring", "SLA"],
) as dag:
    monitor_task = PythonOperator(
        task_id="check_dag_task_runtime",
        python_callable=emit_metric_to_cloudwatch,
        provide_context=True,
    )