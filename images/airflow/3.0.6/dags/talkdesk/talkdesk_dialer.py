import json
import logging
import os
from datetime import date
from textwrap import dedent
from typing import Dict, List

import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas import DataFrame
from pendulum import datetime, now, duration
from sqlalchemy.engine import Engine
from typing_extensions import LiteralString

from above.common.constants import SNOWFLAKE_CONN_ID
from above.common.slack_alert import task_failure_slack_alert
from talkdesk.common.talkdesk import get_talkdesk_api_auth_token

logger: logging.Logger = logging.getLogger(__name__)
this_modules_name: str = str(os.path.basename(__file__).replace(".py", ""))
start_date: datetime = datetime(2024, 6, 1, tz="UTC")


def query_to_dataframe(query: str) -> DataFrame:
    """
    Executes Snowflake query and returns result
    :param query:
    :return: result
    """
    logger.info(f"Executing query:\n{query}")
    snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
    sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
    dataframe: DataFrame = pd.read_sql(query, sql_engine)
    return dataframe


@task.short_circuit
def check_is_business_day(**context):
    """
    Checks current date against the business calendar to run only during
    business days and not weekends or holidays.
    """
    date_today: str = date.today().strftime("%Y-%m-%d")
    query: str = dedent(
        f"""
        SELECT 
          CONTAINS(
            UTIL_DB.PUBLIC.BUSINESS_DAYS_IN_WEEK_ABOVE_HOLIDAYS('{date_today}')::varchar
            , '{date_today}') AS "IS_TODAY_A_BUSINESS_DAY"
        """
    )
    dataframe: DataFrame = query_to_dataframe(query)

    is_today_a_business_day: bool = (
        False if dataframe.empty else dataframe.is_today_a_business_day.values[0]
    )
    logger.info(f"IS_TODAY_A_BUSINESS_DAY: {is_today_a_business_day}")
    return is_today_a_business_day


@task
def check_dialer_list_freshness():
    """
    Checks most recent dbt load time of collections queues table and raises
    an exception if it isn't the current day's collections queue.
    """
    query: str = dedent(
        """
        SELECT TOP 1
            CONVERT_TIMEZONE('UTC', _DBT_LOAD_TIME_CST)::DATE AS "table_date"
        FROM
            CURATED_PROD.REPORTING.VW_COLLECTIONS_ELIGIBLE
        ORDER BY
            _DBT_LOAD_TIME_CST DESC
        """
    )
    dataframe: DataFrame = query_to_dataframe(query)
    if dataframe.empty:
        raise AirflowException("No collection queue table date found")

    table_date: date = dataframe.table_date.values[0]
    logger.info(f"Collections queue date is {table_date}")
    if table_date != date.today():
        raise AirflowException(f"Collections table is out of date")


def create_record_list(auth_token: str, record_list_name: str) -> str:
    """
    Creates a new Talkdesk Record List (a dialer list) and returns the ID of
    that list.
    :param auth_token:
    :param record_list_name:
    :return record_list_id:
    """
    url: str = "https://api.talkdeskapp.com/record-lists"
    body: Dict = dict(name=record_list_name)
    headers: Dict = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "Accept": "application/json, application/hal+json",
    }
    response: requests.Response = requests.post(url=url, headers=headers, json=body)
    response.raise_for_status()
    response_json: Dict = response.json()
    record_list_id: str = response_json.get("id")
    logger.info(
        f"Create Record List response:"
        f" {response.status_code}<{response.reason}>"
        f" • Name: \"{response_json.get('name')}\""
        f" • Status: {response_json.get('status')}"
        f" • ID: {record_list_id}"
    )
    return record_list_id


@task
def create_talkdesk_record_lists(auth_token: str, view_name: str, record_list_name_suffix: str):
    """
    Creates Talkdesk Record lists (dialer lists)--one for first-time
    delinquencies and another for everyone else--and populates them
    from a data warehouse query result.
    :param auth_token:
    """
    timestamp = now("America/Chicago").strftime("%Y-%m-%d %H:%M%Z %A")
    record_list_name = f"Airflow {timestamp} • {record_list_name_suffix}"
    record_list_id = create_record_list(auth_token=auth_token, record_list_name=record_list_name)

    dataframe: DataFrame = query_to_dataframe(f"Select * from {view_name}" )
    result: List[Dict] = [json.loads(value) for value in dataframe.record_json.values]

    # Send to Talkdesk in chunks
    offset, limit = 0, 25
    total = len(result)
    while offset < total:
        chunk = result[offset:offset + limit]
        response = requests.post(
            url=f"https://api.talkdeskapp.com/record-lists/{record_list_id}/records/bulks",
            headers={
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
                "Accept": "application/json, application/hal+json",
            },
            json={"records": chunk},
        )
        response.raise_for_status()
        body = response.json()
        created = body.get("total_created", 0)
        errors = body.get("total_errors", 0)

        if created == len(chunk) and not errors:
            logger.info(
                f"Success: {created} records sent • Offset {offset} • ID {record_list_id}"
            )
        else:
            logger.warning(
                f"Issues: {created}/{len(chunk)} created • {errors} errors • Offset {offset} • ID {record_list_id}"
            )

        offset += limit

   

@dag(
    dag_id=this_modules_name,
    description="Uploads dialer list from the Data Warehouse to TalkDesk",
    tags=["data", "talkdesk"],
    schedule="12 10 * * 0-5",  # Sun-Fri 0412 CST/0512 CDT
    start_date=start_date,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=start_date,
        depends_on_past=False,
        retries=1,
        retry_delay=duration(minutes=3),
        execution_timeout=duration(minutes=10),
        on_failure_callback=task_failure_slack_alert,
    ),
)
def talkdesk_dialer():
    auth_token: str = get_talkdesk_api_auth_token()
    chain(
        check_is_business_day(),
        check_dialer_list_freshness(),
        create_talkdesk_record_lists(auth_token, 'alerts.airflow.dpd_below_thirty', '1_29_dpd'),
        create_talkdesk_record_lists(auth_token,'alerts.airflow.dpd_plus_thirty', '30_119_dpd')
    )


talkdesk_dialer()
