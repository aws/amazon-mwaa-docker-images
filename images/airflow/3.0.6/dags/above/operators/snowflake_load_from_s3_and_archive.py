import logging
import re

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeLoadFromS3AndArchive(BaseOperator):

    def __init__(
            self,
            s3_conn_id,
            s3_bucket,
            s3_prefix,
            s3_loaded_dir,
            snowflake_conn_id,
            stage_name,
            database_name,
            schema_name,
            table_name,
            service_entity=None,
            original_source_system=None,
            s3_key_filter='.*',
            *args,
            **kwargs
    ):
        super(SnowflakeLoadFromS3AndArchive, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_loaded_dir = s3_loaded_dir
        self.snowflake_conn_id = snowflake_conn_id
        self.stage_name = stage_name
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.s3_key_filter = s3_key_filter
        self.service_entity = service_entity
        self.original_source_system = original_source_system

    def execute(self, context):

        r = re.compile(self.s3_key_filter)

        sf_hook = SnowflakeHook(self.snowflake_conn_id)

        s3_hook = S3Hook(self.s3_conn_id)
        # Get all files under the specified prefix
        s3_file_list = s3_hook.list_keys(
            bucket_name=self.s3_bucket,
            prefix=self.s3_prefix
            ) or []

        # Filter files with specified key filter
        s3_file_list = [f for f in s3_file_list if r.match(f)]

        copy_into_template = '''
        alter session set timezone = 'America/Chicago';
        
        CREATE TABLE IF NOT EXISTS 
                {database_name}.{schema_name}.{table_name} (
            RAW_DATA            VARIANT
          , RAW_LOAD_TIME_CST   DATETIME
          , FILE_PATH           VARCHAR
        );
        
        COPY INTO {database_name}.{schema_name}.{table_name}
        FROM (
          SELECT
              $1
            , convert_timezone(
                'America/Chicago', current_timestamp()
                )::datetime
            , METADATA$FILENAME
          FROM '@{database_name}.{schema_name}.{stage_name}'
        )
        FILE_FORMAT = {database_name}.{schema_name}.JSON_FORMAT
          FILES = ('{file_path}')
        '''

        for file in s3_file_list:
            # remove the prefix from the file path as it is already implied
            # in the stage and snowflake will automatically prepend it
            formatted_queries = copy_into_template.format(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=self.table_name,
                stage_name=self.stage_name,
                file_path=file.replace(self.s3_prefix, '')
            )

            sf_hook.run(formatted_queries)

            logging.info(file)

            s3_hook.copy_object(
                source_bucket_key=file,
                # dest_bucket_key=os.path.join(self.s3_loaded_dir, file),
                dest_bucket_key=file.replace(
                    self.s3_prefix,
                    self.s3_loaded_dir
                    ),
                source_bucket_name=self.s3_bucket,
                dest_bucket_name=self.s3_bucket
            )

            s3_hook.delete_objects(bucket=self.s3_bucket, keys=[file])
