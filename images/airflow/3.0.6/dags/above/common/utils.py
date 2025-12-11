import gzip
import io
import logging
from tempfile import TemporaryDirectory
import json
import os

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from mypy_boto3_s3.service_resource import Bucket

logger = logging.getLogger(__name__)

def put_df_to_s3_bucket(
    df: pd.DataFrame,
    s3_bucket: Bucket,
    s3_key: str,
    file_format: str = "json",
) -> None:
    """
    Converts a DataFrame to CSV or JSON, gzips it, and puts it in an S3 Bucket
    :param df: DataFrame
    :param s3_bucket: S3 Bucket object
    :param s3_key: S3 key ("path" in bucket)
    :param file_format: Defaults to 'json'; can be 'csv' or 'json'
    """
    logger.info(f"Started uploading {s3_key} to {s3_bucket.name} as {file_format}...")

    string_buffer: io.StringIO = io.StringIO()

    if file_format == "csv":
        df.to_csv(string_buffer, index=False)
    elif file_format == "json":
        df.to_json(string_buffer, orient="records", lines=True)
    else:
        raise AirflowException("Argument file_format must be 'csv' or 'json'")

    string_buffer.seek(0)
    gz_buffer: io.BytesIO = io.BytesIO()

    with gzip.GzipFile(mode="w", fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(string_buffer.getvalue(), "utf-8"))

    s3_bucket.meta.client.put_object(
        Bucket=s3_bucket.name, Key=s3_key, Body=gz_buffer.getvalue()
    )

    logger.info(f"Finished uploading {s3_key} to {s3_bucket.name}.")


def put_df_to_s3_bucket_name(
    df: pd.DataFrame,
    s3_conn_id: str | None,
    s3_bucket_name: str,
    s3_key: str,
    file_format: str = "json",
) -> None:
    """
    Converts a DataFrame to CSV or JSON, gzips it, and puts it in an S3 Bucket
    :param df: DataFrame
    :param s3_conn_id: S3 Connection ID
    :param s3_bucket_name: Name of the bucket
    :param s3_key: S3 key ("path" in bucket)
    :param file_format: Defaults to 'json'; can be 'csv' or 'json'
    """
    s3_hook: S3Hook = S3Hook(s3_conn_id)
    bucket: Bucket = s3_hook.get_bucket(s3_bucket_name)  # noqa for type hints
    put_df_to_s3_bucket(df, bucket, s3_key, file_format)


def get_ssh_key_file(ssh_key_name: str) -> None:
    """
    Gets SSH key from the secrets backend and saves it to a temp file.

    If you need to use this local key file in a conn uri,
    run this via PythonOperator prior to the operator that needs the key and
    reference the keyfile using path name /tmp/<name_of_key_in_secret_manager>
    :param ssh_key_name: Name of key in Secrets Manager
    """

    ssh_key: str = Variable.get(ssh_key_name)
    ssh_key_file_name: str = "/tmp/{}".format(ssh_key_name)

    with open(ssh_key_file_name, "w") as ssh_key_file:
        ssh_key_file.write(ssh_key)

    logger.info("Wrote {} to {}".format(ssh_key_name, ssh_key_file_name))


# TODO: DEPRECATED, use `upload_file_to_s3` in s3_utils.py.
def copy_file_to_s3(
    file_path: str, s3_bucket: str, s3_key: str, s3_conn_id: str | None = None
) -> None:
    """
    Copies file from ``file_path`` to the ``s3_key`` in ``s3_bucket``.

    :param file_path: File path for source file.
    :type file_path: str
    :param s3_bucket: S3 Bucket name.
    :type s3_bucket: str
    :param s3_key: File destination in S3 bucket.
    :type s3_key: str
    :param s3_conn_id: _description_, defaults to None
    :type s3_conn_id: str | None, optional
    """
    logger.info(f"Copying {file_path} to S3://{s3_bucket}/{s3_key}...")
    s3_hook: S3Hook = S3Hook(s3_conn_id)
    s3_hook.load_file(
        filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True
    )