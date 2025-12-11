import json
import logging
import os
import requests
import time

from textwrap import dedent
from typing import Dict
from pandas import DataFrame

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from pendulum import datetime, duration, now

from above.common.constants import (
    S3_CONN_ID,
    S3_DATAENGINEERING_BUCKET,
)
from above.common.check_memory_usage import check_memory_usage
from above.common.s3_utils import gzip_json_and_upload_to_s3
from above.common.slack_alert import task_failure_slack_alert
from above.common.snowflake_utils import snowflake_query_to_pandas_dataframe

logger: logging.Logger = logging.getLogger(__name__)
THIS_FILENAME: str = str(os.path.basename(__file__).replace(".py", ""))
DAG_START_DATE: datetime = datetime(2025, 9, 16, tz="UTC")

GDS_CREDENTIALS: Dict = json.loads(Variable.get("gds"))
AUDIT_LOG_AUTH_TOKEN = GDS_CREDENTIALS.get("audit_log_auth_token")

S3_SOURCE_DIR = "sources/GDSCaseCenter/audit_logs/"
CASECENTER_HISTORY_API_URL = "https://casecenter-above-lending-prod.dataview360.com/Above_Lending/above_lending/records/history.json"
DAYS_SINCE_APPLICATION_CREATION = 1600
API_BATCH_SIZE = 500  # Adjust based on memory and API limits
SLEEP_TIMEOUT = 0.01  # To avoid hitting API rate limits or API exhaustion.


@task
def load_and_batch_application_data(
    days_since_application_creation: int, **context
) -> list[list[str]]:
    """Load applications to request from Casecenter and split this into batches."""
    check_memory_usage(tag="Initial memory usage.")
    try:
        query: str = dedent(
            """
                SELECT request_id, created_at FROM MANUAL.DE.GDS_AUDIT_LOG_LIST_ACTIVE;
            """
        )
        df: DataFrame = snowflake_query_to_pandas_dataframe(query)

        # Check row count for this df in the context.
        context["ti"].xcom_push(key=f"row_count_{now().isoformat()}", value=len(df))

        if not df.empty:
            logger.info(f"{len(df)} applications audit_logs to load.")
        else:
            logger.info("No applications audit_logs to load.")
    except ValueError as e:
        logger.error(f"Invalid value: {e}")
        raise AirflowFailException()
    except Exception as e:
        logger.error(f"Other error: {e}")
        raise AirflowFailException()

    # Batching
    df_request_ids: list[str] = df["REQUEST_ID"].tolist()
    batches: list[list[str]] = [
        df_request_ids[i : i + API_BATCH_SIZE]
        for i in range(0, len(df_request_ids), API_BATCH_SIZE)
    ]
    logger.info(f"Split into {len(batches)} batches of up to {API_BATCH_SIZE} each.")
    logger.info(batches)
    check_memory_usage(tag="Memory usage after batching.")

    # This automatically puts `batches` into XCom for the next task to use.
    return batches


@task
def process_batch_of_application_data(batch: list[str]) -> None:
    """Hit API endpoint for each record in the batch."""
    for idx, request_id in enumerate(batch):
        logger.info("On record %s of %s in this batch.", idx + 1, len(batch))
        get_application_audit_log(request_id)


def get_application_audit_log(request_id: str) -> None:
    """
    POST to CaseCenter's history API to get audit log of `request_id`.

    Additionally, this gzips and uploads the returned JSON to S3.

    Args:
        request_id (str): The request_id of the desired audit_log

    Returns:
        json: The returned JSON Audit Log from the API call.
    """

    check_memory_usage(tag=f"GET AUDIT LOG FOR {request_id}")
    time.sleep(SLEEP_TIMEOUT)  # To avoid hitting API rate limits or API exhaustioin.
    try:
        url = (
            f'{CASECENTER_HISTORY_API_URL}?query={{"system.record_id":"{request_id}"}}'
        )
        headers = {"auth_token": f"{AUDIT_LOG_AUTH_TOKEN}"}

        resp = requests.post(url, headers=headers, timeout=600)
        resp.raise_for_status()

        audit_log_json = resp.json()
        gzip_json_and_upload_to_s3(
            json_data=audit_log_json,
            s3_bucket=S3_DATAENGINEERING_BUCKET,
            s3_key=os.path.join(S3_SOURCE_DIR, f"{request_id}.json.gz"),
            aws_conn_id=S3_CONN_ID,
        )

        del audit_log_json

    except Exception as e:
        logger.error(f"Other error: {e}")
        raise AirflowFailException()


@dag(
    dag_id=THIS_FILENAME,
    description="Extracts the audit log from GDS",
    tags=["data", "gds", "applications"],
    schedule="0 3 * * *",  # Daily at 10pm CT
    start_date=DAG_START_DATE,
    max_active_runs=1,
    max_active_tasks=4,  # Adjust based on API capacity
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=DAG_START_DATE,
        depends_on_past=False,
        retries=3,
        retry_delay=duration(minutes=5),
        execution_timeout=duration(minutes=420),
        on_failure_callback=task_failure_slack_alert,
    ),
)
def gds_audit_log_extract(**context):
    batches = load_and_batch_application_data(
        days_since_application_creation=DAYS_SINCE_APPLICATION_CREATION
    )
    process_batch_of_application_data.expand(batch=batches)


gds_audit_log_extract()
