import logging
import os
from datetime import datetime as dt
from hashlib import md5

import numpy as np
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class QueryToS3Operator(BaseOperator):

    def __init__(
            self,
            s3_conn_id,
            s3_bucket,
            s3_key,
            snowflake_conn_id,
            query,
            file_format,
            index_value=None,
            *args,
            **kwargs
    ):
        super(QueryToS3Operator, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.snowflake_conn_id = snowflake_conn_id
        self.query = query
        self.file_format = file_format
        self.index_value = index_value

    def execute(self, context):

        sf_hook = SnowflakeHook(self.snowflake_conn_id)
        sf_sqlalchemy_engine = sf_hook.get_sqlalchemy_engine()

        s3_hook = S3Hook(self.s3_conn_id)

        logging.info('Executing query...')
        df = pd.read_sql(self.query, sf_sqlalchemy_engine)
        logging.info('Query complete')

        # create a unique file name based on hash of current time
        file_name = '{}.{}'.format(
            md5(dt.now().strftime('%s').encode()).hexdigest(),
            self.file_format
        )
        file_path = os.path.join('/tmp', file_name)

        if self.file_format == 'csv':
            df.to_csv(file_path)

        elif self.file_format == 'txt':
            logging.info("loading txt file")
            # dumping sql data to numpy array
            numpy_array = df.to_numpy()
            # saving the data in a text file as string.
            np.savetxt(file_path, numpy_array, fmt="%s", delimiter='')
        else:
            raise AirflowException(
                'Currently only csv and txt format supported'
            )

        logging.info(
            'Started loading - {} to {}'.format(file_path, self.s3_key)
        )
        s3_hook.load_file(
            filename=file_path,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        logging.info(
            'Finished loading - {} to {}'.format(file_path, self.s3_key)
        )
