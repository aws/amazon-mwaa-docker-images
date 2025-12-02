import glob
import logging
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, List
from zipfile import ZipFile

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from pendulum import datetime, duration, now, instance, DateTime

from above.common.constants import (
    S3_DATALAKE_BUCKET,
    TABLEAU_BACKUP_BUCKET_DIRECTORY,
    TABLEAU_CUSTOM_QUERY_BUCKET_DIRECTORY,
)
from tableau.utils.tableau_utils import get_tableau_dag_default_args
from above.common.utils import copy_file_to_s3

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import Bucket, ObjectSummary

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))
dag_start_date: DateTime = datetime(2024, 6, 11, tz="UTC")

# Constants
QUERY_SEPARATION_STR: str = "\n\n;\n\n"  # Separator to use for multiple query joining


def extract_custom_queries_from_twb_file(twb_file_path: str) -> str:
    """
    Extract custom queries from a Tableau workbook (.twb) file.

    :param twb_file_path: Path to the .twb file
    :return: Concatenated custom queries found in the file
    """
    try:
        with open(twb_file_path, "r") as xml:
            soup = BeautifulSoup(xml.read(), "html.parser")
    except Exception as e:
        logger.error(f"Failed to read TWB file {twb_file_path}: {e}")
        raise AirflowException(f"Failed to read TWB file: {e}")

    query_tag = re.compile("relation")
    relation_tags = soup.find_all(name=query_tag)

    def _clean_relation_tags(tag: str) -> str:
        """Replace characters escaped by Tableau with unescaped equivalents."""
        return tag.replace(">>", ">").replace("<<", "<")

    queries: List[str] = [
        _clean_relation_tags(relation_tag.text)
        for relation_tag in relation_tags
        if relation_tag.text
    ]

    return QUERY_SEPARATION_STR.join(queries)


def extract_custom_queries_from_twb_files(twb_file_paths: List[str]) -> str:
    """
    Extract custom queries from multiple TWB files and concatenate them.

    :param twb_file_paths: List of paths to .twb files
    :return: Concatenated custom queries from all files
    """
    return QUERY_SEPARATION_STR.join(
        extract_custom_queries_from_twb_file(twb_file_path)
        for twb_file_path in twb_file_paths
    )


def process_workbook_file(
    s3_hook: S3Hook, workbook_key: str, bucket_name: str, tmp_dir: str
) -> None:
    """
    Process a single Tableau workbook file to extract and save custom queries.

    :param s3_hook: S3Hook instance for S3 operations
    :param workbook_key: S3 key for the workbook file
    :param bucket_name: S3 bucket name
    :param tmp_dir: Temporary directory path for file processing
    """
    try:
        # Download file from S3
        file_path = s3_hook.download_file(
            key=workbook_key,
            bucket_name=bucket_name,
            local_path=tmp_dir,
            preserve_file_name=True,
        )
        output_file: str = os.path.join(tmp_dir, Path(file_path).stem + ".sql")

        logger.info(f"Processing workbook: {file_path}")

        # If the file is .twbx, unzip it first
        if file_path.endswith(".twbx"):
            with ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)

        # Find all .twb files (could be multiple in a .twbx)
        twb_file_paths: List[str] = glob.glob(f"{tmp_dir}/*.twb")

        if not twb_file_paths:
            logger.warning(f"No .twb files found in {workbook_key}")
            return

        # Extract queries from all .twb files
        queries = extract_custom_queries_from_twb_files(twb_file_paths=twb_file_paths)

        if queries:
            # Write queries to file
            with open(output_file, "w+") as output:
                output.write(queries)

            # Upload to S3
            s3_key = os.path.join(
                TABLEAU_BACKUP_BUCKET_DIRECTORY,
                TABLEAU_CUSTOM_QUERY_BUCKET_DIRECTORY,
                os.path.basename(output_file),
            )
            copy_file_to_s3(
                file_path=output_file,
                s3_bucket=S3_DATALAKE_BUCKET,
                s3_key=s3_key,
            )
            logger.info(f"Saved queries to S3: {s3_key}")
        else:
            logger.info(f"No custom queries found in {workbook_key}")

    except Exception as e:
        logger.error(f"Error processing workbook {workbook_key}: {e}")
        raise AirflowException(f"Failed to process workbook {workbook_key}: {e}")


@task
def backup_custom_tableau_queries() -> None:
    """
    Extract and backup custom SQL queries from Tableau workbooks in S3.

    Scans S3 for Tableau workbook files (.twb/.twbx) modified in the last 24 hours,
    extracts custom SQL queries, and saves them as separate .sql files in S3.
    """
    modified_after: DateTime = now(tz="UTC") - duration(days=1)

    try:
        s3_hook: S3Hook = S3Hook(aws_conn_id=None)
        s3_bucket: Bucket = s3_hook.get_bucket(bucket_name=S3_DATALAKE_BUCKET)
    except Exception as e:
        logger.error(f"Failed to connect to S3: {e}")
        raise AirflowException(f"Failed to connect to S3: {e}")

    # Find workbooks modified after the specified date
    workbook_object_summaries: List[ObjectSummary] = [
        obj
        for obj in s3_bucket.objects.filter(Prefix=TABLEAU_BACKUP_BUCKET_DIRECTORY)
        if obj.key.endswith((".twb", ".twbx"))
        and (instance(obj.last_modified, "UTC") > modified_after)
    ]

    logger.info(f"Found {len(workbook_object_summaries)} workbooks to process")

    if not workbook_object_summaries:
        logger.info("No workbooks found modified in the last 24 hours")
        return

    # Process each workbook
    for workbook_obj in workbook_object_summaries:
        with TemporaryDirectory() as tmp:
            process_workbook_file(
                s3_hook=s3_hook,
                workbook_key=workbook_obj.key,
                bucket_name=workbook_obj.bucket_name,
                tmp_dir=tmp,
            )

    logger.info(f"Successfully processed {len(workbook_object_summaries)} workbooks")


@dag(
    dag_id=this_filename,
    description="Extract and backup custom SQL queries from Tableau workbooks in S3",
    tags=["data", "tableau", "backup", "sql"],
    schedule="20 03 * * *",  # Daily 10:20pm CST (03:20 UTC)
    start_date=dag_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=get_tableau_dag_default_args(dag_start_date),
)
def tableau_custom_query_backup() -> None:
    """
    DAG to extract and backup custom SQL queries from Tableau workbooks.

    Processes Tableau workbook files (.twb/.twbx) that have been modified in the
    last 24 hours, extracts any custom SQL queries, and saves them as .sql files
    in S3 for version control and analysis purposes.
    """
    backup_custom_tableau_queries()


tableau_custom_query_backup()
