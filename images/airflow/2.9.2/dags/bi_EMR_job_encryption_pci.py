
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

EMR_APPLICATION_ID = Variable.get("emr_serverless_application_id") 
EMR_JOB_ROLE_ARN = Variable.get("emr_serverless_job_role") 
EMR_S3_BUCKET = Variable.get("emr_serverless_bucket") 
EMR_S3_BUCKET_FOLDER = Variable.get("emr_serverless_bucket_folder") 
EMR_S3_DATA_BUCKET = Variable.get("emr_serverless_data_bucket") 
EMR_SCRIPT_PATH = Variable.get("emr_serverless_script_path") 

with DAG(
    dag_id='bi_emr_serverless_job',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=['BI','EMR','ENCRYPTION', 'WIP'],
    catchup=False,
) as dag:

    # Second Spark job for AC_CARD
    ac_card_job_starter = EmrServerlessStartJobOperator(
        task_id="ac_card_start_job",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{EMR_SCRIPT_PATH}",
                "entryPointArguments": [f"{EMR_S3_DATA_BUCKET}", f"{EMR_S3_BUCKET_FOLDER}", "AC_CARD"],
                "sparkSubmitParameters": f"--conf spark.archives=s3://{EMR_S3_BUCKET}/env/emr_serverless_pyspark_env.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{EMR_S3_BUCKET}/logs/"
                }
            },
        }
    )


    # Second Spark job for TP_TRANS
    tp_trans_job_starter = EmrServerlessStartJobOperator(
        task_id="tp_trans_start_job",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{EMR_SCRIPT_PATH}",
                "entryPointArguments": [f"{EMR_S3_DATA_BUCKET}", f"{EMR_S3_BUCKET_FOLDER}", "TP_TRANS"],
                "sparkSubmitParameters": f"--conf spark.archives=s3://{EMR_S3_BUCKET}/env/emr_serverless_pyspark_env.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{EMR_S3_BUCKET}/logs/"
                }
            },
        }
    )

