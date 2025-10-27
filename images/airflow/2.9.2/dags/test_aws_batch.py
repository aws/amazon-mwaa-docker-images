from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.models import Variable

default_args = {
    "execution_timeout": timedelta(hours=1),
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
}

dag = DAG(
    dag_id="Submit_batch_job",
    default_args=default_args,
    start_date=datetime(2024, 5, 8),
    # schedule=None,
    is_paused_upon_creation=False,
    tags=["batch_job", 'WIP'],
    schedule_interval = None,
    max_active_runs=1,
    catchup=False
)

job_definition = Variable.get("Aws_batch_definition") 
job_queue = Variable.get("aws_batch_job_queue") 
rdf_files_s3 = Variable.get("rdf_files_s3") 
rdf_client = Variable.get("rdf_client") 

parameter_value=[rdf_client, "-b",rdf_files_s3]

submit_batch_job = BatchOperator(
    task_id="submit_batch_job",
    job_name="test_job",
    job_queue=job_queue,
    job_definition=job_definition,
    wait_for_completion=False,
    overrides={
            'command': parameter_value  # custom command with arguments
        },
    dag=dag,
)

#submit the job to aws batch with s3 bucket
submit_batch_job



