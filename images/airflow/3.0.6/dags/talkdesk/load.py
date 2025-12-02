import os
from datetime import timedelta

from airflow import DAG

from above.common.constants import (
    DAGS_FOLDER,
    DATALAKE_LOADED_DIR,
    DATALAKE_PREPROCESSED_DIR,
    RAW_DATABASE_NAME,
    S3_CONN_ID,
    S3_DATALAKE_BUCKET,
    SNOWFLAKE_CONN_ID,
    STORAGE_INTEGRATION_NAME
)
from above.common.dag_generators import load_raw_from_s3
from above.common.slack_alert import task_failure_slack_alert

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'start_date': '2024-01-13',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'on_failure_callback': task_failure_slack_alert
}

LOAD_FOLDER: str = os.path.join(DAGS_FOLDER, 'talkdesk/config/load')

for data_source in os.listdir(LOAD_FOLDER):

    if os.path.isdir(os.path.join(LOAD_FOLDER, data_source)):
        dag_id = '{}_load'.format(data_source)

        standard_query_params = {
            'storage_integration_name': STORAGE_INTEGRATION_NAME,
            'external_stage_url': 's3://{}'.format(
                os.path.join(
                    S3_DATALAKE_BUCKET,
                    DATALAKE_PREPROCESSED_DIR, data_source
                )
            ) + '/',
            'database_name': RAW_DATABASE_NAME,
            'schema_name': data_source,
            'stage_name': data_source,
            's3_conn_id': S3_CONN_ID,
            's3_bucket': S3_DATALAKE_BUCKET,
            's3_prefix': os.path.join(
                DATALAKE_PREPROCESSED_DIR, data_source
            ) + '/',
            's3_loaded_dir': os.path.join(
                DATALAKE_LOADED_DIR, data_source
            ) + '/'
        }

        # In general, this will load files from the preprocessed bucket into
        # loaded bucket and into Snowflake, and then delete from preprocessed.
        globals()[dag_id]: DAG = load_raw_from_s3(
            dag_id=dag_id,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            query_params=standard_query_params,
            default_args=default_args,
            config_file_dir=os.path.join(LOAD_FOLDER, data_source),
            sql_template_searchpath=os.path.join(
                'dags/talkdesk', 'sql'
            ),
            tags=['data', 'talkdesk']
        )
