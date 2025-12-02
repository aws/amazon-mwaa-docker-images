import gzip
import logging
from tempfile import TemporaryDirectory
import json
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


def upload_file_to_s3(
    file_path: str,
    s3_bucket: str,
    s3_key: str,
    s3_conn_id: str | None = None,
    replace: bool = True,
) -> None:
    """
    Uploads a file to S3.

    Args:
        file_path (str): Local path to the file to upload.
        s3_bucket (str): Name of the S3 bucket.
        s3_key (str): S3 key (path in the bucket) where the file will be stored.
        s3_conn_id (str | None): Airflow S3 connection ID
        replace (bool): If True, replace the file if it already exists in S3
    """
    logger.info(f"Copying {file_path} to S3://{s3_bucket}/{s3_key}...")
    s3_hook: S3Hook = S3Hook(s3_conn_id)
    s3_hook.load_file(
        filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=replace
    )


def upload_string_to_s3(
    string_value: str,
    s3_bucket: str,
    s3_key: str,
    s3_conn_id: str | None = None,
    replace: bool = True,
) -> None:
    """
    Uploads a file to S3.

    Args:
        string_value (str): String to upload.
        s3_bucket (str): Name of the S3 bucket.
        s3_key (str): S3 key (path in the bucket) where the file will be stored.
        s3_conn_id (str | None): Airflow S3 connection ID
        replace (bool): If True, replace the file if it already exists in S3
    """
    logger.info(f"Copying json object to S3://{s3_bucket}/{s3_key}...")
    s3_hook: S3Hook = S3Hook(s3_conn_id)
    s3_hook.load_string(
        string_data=string_value, key=s3_key, bucket_name=s3_bucket, replace=replace
    )


def gzip_json_and_upload_to_s3(
    json_data: dict | str,
    s3_bucket: str,
    s3_key: str,
    aws_conn_id: str = "aws_default",
    replace: bool = True,
    verbose: bool = False,
    **context,
):
    """
    Gzip JSON data and upload it to S3.

    Args:
        json_data (dict or str): JSON data to gzip (dict will be serialized)
        s3_bucket (str): S3 bucket name
        s3_key (str): S3 key/path for the file (e.g., 'data/output.json.gz')
        aws_conn_id (str): Airflow AWS connection ID
        replace (bool): If True, replace the file if it already exists in S3
        verbose (bool): If True, print more zipping information
    """
    with TemporaryDirectory() as temp_dir:
        temp_json_path = os.path.join(temp_dir, "temp_data.json")
        temp_gzip_path = os.path.join(temp_dir, "temp_data.json.gz")

        # Convert dict to JSON string if necessary
        if isinstance(json_data, dict):
            json_string = json.dumps(json_data, indent=2)
        else:
            json_string = json_data

        # Write JSON to temporary file
        with open(temp_json_path, "w") as f:
            f.write(json_string)

        # Gzip the JSON file
        with open(temp_json_path, "rb") as f_in:
            with gzip.open(temp_gzip_path, "wb") as f_out:
                f_out.writelines(f_in)

        original_size = os.path.getsize(temp_json_path)
        compressed_size = os.path.getsize(temp_gzip_path)
        compression_ratio = (1 - compressed_size / original_size) * 100

        if verbose:
            logger.info(f"Original json size: {original_size:,} bytes")
            logger.info(f"Compressed json.gz size: {compressed_size:,} bytes")
            logger.info(f"Compression ratio: {compression_ratio:.1f}%")

        upload_file_to_s3(
            file_path=temp_gzip_path,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            s3_conn_id=aws_conn_id,
            replace=replace,
        )
        if verbose:
            logger.info(f"Uploaded gzipped JSON to s3://{s3_bucket}/{s3_key}")

    return f"s3://{s3_bucket}/{s3_key}"
