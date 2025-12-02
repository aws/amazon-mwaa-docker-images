from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from time import sleep
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from above.common.constants import (
    DATALAKE_ERROR_DIR, DATALAKE_PREPROCESSED_DIR, DATALAKE_SOURCE_ARCHIVE_DIR,
    DATALAKE_SOURCE_DIR, DATALAKE_SUCCESS_DIR, S3_CONN_ID, S3_DATALAKE_BUCKET
)
from above.common.slack_alert import task_failure_slack_alert
from talkdesk.common.talkdesk import get_talkdesk_api_auth_token
from above.common.utils import put_df_to_s3_bucket_name
from above.operators.preprocess_files_operator import (
    PreprocessFilesOperator,
    UnzipSplitFilesOperator
)

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))

DATA_SOURCE: str = "talkdesk_api"
TALKDESK_REPORTS: List[str] = ["calls", "user_status", "advanced_dialer_calls_report"]
DATESTAMP: str = date.today().isoformat()
S3_SOURCE_DIR: str = os.path.join(DATALAKE_SOURCE_DIR, DATA_SOURCE)
S3_PREPROCESSED_DIR: str = os.path.join(DATALAKE_PREPROCESSED_DIR, DATA_SOURCE)
S3_ERROR_DIR: str = os.path.join(DATALAKE_ERROR_DIR, DATA_SOURCE, DATESTAMP)
S3_SUCCESS_DIR: str = os.path.join(DATALAKE_SUCCESS_DIR, DATA_SOURCE, DATESTAMP)
S3_SOURCE_ARCHIVE_DIR: str = os.path.join(
    DATALAKE_SOURCE_ARCHIVE_DIR,
    DATA_SOURCE,
    DATESTAMP
)

start_date: datetime = datetime(2024, 1, 13, tzinfo=timezone.utc)


@task.short_circuit
def check_data_interval_times(**context):
    data_interval_start: datetime = context.get("data_interval_start")
    previous_end: datetime = context.get("prev_data_interval_end_success")
    logger.info(f"Resuming at: {data_interval_start} from {previous_end}")
    is_new: bool = previous_end is None or data_interval_start >= previous_end
    return True if is_new else False


@task
def get_talkdesk_report_to_s3(talkdesk_auth_token, report, **context):
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    time_format: str = "%Y-%m-%dT%H:%M:%S.000Z"
    talkdesk_report_start: str = data_interval_start.strftime(time_format)
    talkdesk_report_end: str = data_interval_end.strftime(time_format)
    talkdesk_report_request_body: Dict = dict(
        format="json",
        timespan={"from": talkdesk_report_start, "to": talkdesk_report_end}
    )
    headers = {
        "Authorization": f"Bearer {talkdesk_auth_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    base_url: str = "https://api.talkdeskapp.com/data/reports"
    jobs_url: str = f"{base_url}/{report}/jobs"
    response: requests.Response = requests.post(
        url=jobs_url,
        headers=headers,
        json=talkdesk_report_request_body
    )
    response.raise_for_status()
    talkdesk_job: Dict[str, Any] = json.loads(response.content.decode())["job"]
    talkdesk_job_id: str = talkdesk_job.get("id")

    job_url: str = f"{jobs_url}/{talkdesk_job_id}"
    headers = {
        "Authorization": f"Bearer {talkdesk_auth_token}",
        "Accept": "application/json"
    }
    talkdesk_data_entries: List | None = None
    # It usually takes 1-2 seconds to generate the report
    sleep_interval_in_seconds: float = 3
    while not talkdesk_data_entries:
        sleep(sleep_interval_in_seconds)
        logger.info(f"Requesting job {talkdesk_job_id}...")
        response = requests.get(url=job_url, headers=headers)
        content: Dict[str, Any] = json.loads(response.content.decode())
        status: str | None = content.get("job", {}).get("status")
        if status == "failed":
            logger.error("Talkdesk job failed with API status '%s'!", status)
            return False
        elif status:
            logger.info("API status: %s", status)
        # An "entries" element will appear with the returned data
        if "entries" in content.keys():
            talkdesk_data_entries = content.get("entries", [])
            break

    if not len(talkdesk_data_entries):
        return False

    logger.info(
        f"Received {len(talkdesk_data_entries)} rows,"
        f" {sys.getsizeof(talkdesk_data_entries)} bytes"
    )
    entries_df: pd.DataFrame = pd.DataFrame(talkdesk_data_entries)
    logging.info("Data Frame size: {}".format(sys.getsizeof(entries_df)))
    logging.info("Data Frame: {}".format(entries_df))
    filename = (
        f"{report}_{talkdesk_report_start}_{talkdesk_report_end}.json.gz"
    )
    logging.info(f"Writing data frame to file {filename}...")
    put_df_to_s3_bucket_name(
        df=entries_df,
        s3_conn_id=S3_CONN_ID,
        s3_bucket_name=S3_DATALAKE_BUCKET,
        s3_key=os.path.join(S3_SOURCE_DIR, filename),
        file_format='json'
    )
    return True


with DAG(
        dag_id=this_filename,
        description=f"TalkDesk {', '.join(TALKDESK_REPORTS)}"
                    f" from API to Data Warehouse",
        tags=["data", "talkdesk"],
        schedule="15 * * * *",  # every 15 minutes after the hour
        start_date=start_date,
        max_active_runs=1,
        catchup=True,
        default_args=dict(
            owner="Data Engineering",
            start_date=start_date,
            depends_on_past=False,
            retries=3,
            retry_delay=timedelta(minutes=45),
            execution_timeout=timedelta(minutes=9),
            on_failure_callback=task_failure_slack_alert
        )
) as dag:

    unzip_split = UnzipSplitFilesOperator(
        task_id="unzip_split",
        s3_conn_id=S3_CONN_ID,
        s3_in_bucket=S3_DATALAKE_BUCKET,
        s3_source_dir=S3_SOURCE_DIR,
        s3_source_archive_dir=S3_SOURCE_ARCHIVE_DIR,
        dag=dag
    )

    preprocess = PreprocessFilesOperator(
        task_id="preprocess",
        s3_conn_id=S3_CONN_ID,
        s3_in_bucket=S3_DATALAKE_BUCKET,
        s3_source_dir=S3_SOURCE_DIR,
        s3_out_bucket=S3_DATALAKE_BUCKET,
        s3_preprocessed_dir=S3_PREPROCESSED_DIR,
        s3_error_dir=S3_ERROR_DIR,
        s3_success_dir=S3_SUCCESS_DIR,
        file_parsing_args={"sep": ","},
        dag=dag
    )

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_dag_{}_load".format(DATA_SOURCE),
        trigger_dag_id="{}_load".format(DATA_SOURCE),
        dag=dag
    )

    chain(
        check_data_interval_times(),
        [get_talkdesk_report_to_s3.override(task_id=f"get_talkdesk_{report}")
         (get_talkdesk_api_auth_token(), report) for report in TALKDESK_REPORTS],
        unzip_split,
        preprocess,
        trigger_load_dag
    )
