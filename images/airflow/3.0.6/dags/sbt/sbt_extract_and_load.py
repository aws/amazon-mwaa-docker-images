import json
import logging
import os
from datetime import timedelta
from textwrap import dedent
from typing import Any, Dict, List, Tuple

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from numpy import NaN
from pandas import DataFrame, concat
from pendulum import (
    DateTime, datetime, duration, interval, now, parse
)
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

from above.common.constants import RAW_DATABASE_NAME, SNOWFLAKE_CONN_ID
from above.common.slack_alert import task_failure_slack_alert
from above.common.snowflake_utils import query_to_dataframe

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))

snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
snowflake_connection = snowflake_hook.get_conn()

database_schema: str = "SBT"
temp_table_suffix: str = "_TEMP"
base_url: str = "https://t2c-api.solutionsbytext.com"
default_page_size: int = 1000  # API default is 10
TOKEN_EXPIRATION_IN_SECONDS: int = 30 * 60  # 30 minutes
api_date_format: str = "MM/DD/YYYY HH:mm:ss"
api_start_date: datetime = datetime(2023, 5, 1, tz='UTC')
brands: List = json.loads(Variable.get("sbt_brands"))
groups: List = json.loads(Variable.get("sbt_groups"))


def _set_access_token() -> Tuple[str, DateTime]:
    """
    Fetches a new API access token and stores it, with its expiration timestamp,
    in Airflow Variables.
    :return: access token and expiration timestamp
    """
    client_id: str = Variable.get("sbt_client_id")
    client_secret: str = Variable.get("sbt_client_secret")
    
    url: str = "https://login.solutionsbytext.com/connect/token"
    response: requests.Response = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    )

    if response.status_code == 200:
        response_json: Dict = response.json()
        access_token: str = response_json.get("access_token")
        token_duration: int = response_json.get("expires_in", TOKEN_EXPIRATION_IN_SECONDS)
        expires_at: DateTime = now() + duration(seconds=token_duration)
        logger.info(
            "Received SBT access token ...%s, expires %s.",
            access_token[:10], expires_at
        )
        Variable.set("sbt_access_token", access_token)
        Variable.set("sbt_access_expires_at", expires_at)
    else:
        logger.error("HTTP %d %s", response.status_code, response.reason)
        raise ValueError("Unable to get SBT access token")
    return access_token, expires_at


def get_access_token() -> str:
    """
    Checks Airflow Variables for a valid access token and returns it. If the
    token is nonexistent or will expire within five minutes, it fetches/stores
    a new one and returns it.
    :return: a valid access token
    """
    access_token: str = Variable.get("sbt_access_token", default_var=None)
    expires_at: str = Variable.get("sbt_access_expires_at", default_var=None)
    five_minutes_remaining: DateTime = now() + duration(seconds=300)
    if (
            access_token is None or
            expires_at is None or
            parse(expires_at) < five_minutes_remaining
    ):
        try:
            access_token, expires_at = _set_access_token()
        except Exception as e:
            logger.error("Error fetching access token: %s", e)
            raise
    return access_token


def drop_table(table_name: str) -> None:
    """
    Drops the specified table in the configured raw database and schema.
    :param table_name: table name only
    """
    logger.info("Dropping table %s", table_name)
    query: str = dedent(
        f"""
        DROP TABLE IF EXISTS {RAW_DATABASE_NAME}.{database_schema}.{table_name};
        """
    )
    df: DataFrame = query_to_dataframe(query)
    logger.info(df)


def rename_table(from_name: str, to_name: str) -> None:
    """
    Renames a table in the configured raw database and schema.
    :param from_name: which table to name
    :param to_name: new name for the table
    """
    logger.info("Renaming table %s to %s", from_name, to_name)
    query: str = dedent(
        f"""
        ALTER TABLE IF EXISTS {RAW_DATABASE_NAME}.{database_schema}.{from_name}
        RENAME TO {RAW_DATABASE_NAME}.{database_schema}.{to_name};
        """
    )
    df: DataFrame = query_to_dataframe(query)
    logger.info(df)


def switch_table(from_name: str, to_name: str) -> None:
    """
    Switches one table with another by dropping it and renaming the other table.
    This is used to replace a "live" table with a newly-built temporary table.
    :param from_name: table to be renamed
    :param to_name: table to be dropped
    """
    drop_table(table_name=to_name)
    rename_table(from_name=from_name, to_name=to_name)


def get_latest_timestamp_from_table(
        table_name: str, column_name: str
) -> DateTime:
    """
    Determines how fresh a table's data are by its most recent timestamp.
    This is used to increment a table's data to pick up when it leaves off.
    :param table_name: table name within configured raw database and schema
    :param column_name: name of the column containing dates or timestamps
    :return: the most recent (max) timestamp in specified column
    """
    query: str = dedent(
        f"""
        SELECT NVL(
                    MAX(TRY_TO_TIMESTAMP_TZ("{column_name}")), 
                    '2023-05-01T00:00:00Z'::TIMESTAMP_TZ
               ) AS latest_timestamp
        FROM {RAW_DATABASE_NAME}.{database_schema}.{table_name}
        """
    )
    global api_start_date
    latest_timestamp: DateTime = api_start_date
    try:
        df: DataFrame = query_to_dataframe(query)
        latest_timestamp: DateTime = parse(str(df.latest_timestamp.values[0]))
        logger.info("Most recent record timestamp is %s", latest_timestamp)
        logger.info("Timestamp type is %s", type(latest_timestamp))
    except ProgrammingError as error:
        logger.error("Exception: %s %s", type(error), error)
        logger.warning(
            "Table %s doesn't exist or not authorized; using %s",
            table_name, api_start_date
        )
    return latest_timestamp


def get_data_from_api(url: str) -> Tuple[Dict, int, int]:
    """
    Fetches a single page and its metadata from the SBT API.
    :param url: Full API endpoint URL (with parameters)
    :return: Response JSON data, total number of records, total number of pages
    """
    access_token: str = get_access_token()
    headers: Dict = dict(Authorization=f"Bearer {access_token}")
    logger.info("Fetching data from %s...", url)
    response: requests.Response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(f"{response.status_code}: {response.reason}")
    response_json: Dict = response.json()
    if response_json.get("IsError"):
        raise ValueError(response_json.get("message", "API Response Error"))
    response_data: Dict = response_json.get("data")
    total_count: int = response_data.get("totalCount", 0)
    total_pages: int = response_data.get("totalPages", 0)
    return response_data, total_count, total_pages


# Remove empty dictionaries; they cause PyArrow to fail and should be null
def replace_empty_with_none(value: Any) -> Any:
    return value if value else None


def write_pandas_to_database(
        dataframe: DataFrame, table_name: str
) -> Tuple[bool, int]:
    """
    Inserts Pandas DataFrame data into the specified table in the configured
    raw database and schema
    :param dataframe: a Pandas DataFrame
    :param table_name: the name of the table in raw database's schema
    :return: success flag, number of rows written
    """
    global database_schema
    success, number_of_chunks, number_of_rows, _ = write_pandas(
        snowflake_connection, dataframe, table_name,
        database=RAW_DATABASE_NAME, schema=database_schema,
        auto_create_table=True, quote_identifiers=True,
        use_logical_type=True
    )
    if success:
        logger.info("%d row(s) written", number_of_rows)
    else:
        logger.error("write_pandas unsuccessful for %s", table_name)
    return success, number_of_rows


def transpose_dictionaries(record: List[Dict]) -> Dict[str, str] | None:
    """
    Converts a list of dictionaries found in the PROPERTIES and
    RELATIONSHIPS columns into a single dictionary
    :param record: [ { "name": "xxx", "value"|"relationship": "yyy" } ],...
    :return: { "xxx": "yyy", ... }
    """
    if not isinstance(record, list) or not record:
        return None
    transposed_dicts: Dict = dict()
    for element in record:
        if element:  # skip empty dictionary
            key: str = element.get("name")
            value: str = element.get("value", element.get("relationship"))
            transposed_dicts = {**transposed_dicts, key.upper(): value}
    return transposed_dicts if transposed_dicts else None


@task
def get_subscribers() -> None:
    """
    Extracts subscribers from API endpoint, converts blank values to nulls,
    transforms lists of dictionaries into a single dictionary, and loads
    data into Snowflake. Subscribers table is replaced if it exists, created
    if it doesn't.
    """
    global default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "SUBSCRIBERS"
    temp_table_name: str = f"{table_name}{temp_table_suffix}"
    drop_table(temp_table_name)

    for group in groups:
        page_number: int = 1
        df: DataFrame = DataFrame()
        while True:  # page-level loop
            url: str = (
                f"{base_url}/groups/{group}/subscribers"
                f"?pageNumber={page_number}&pageSize={page_size}"
            )
            response_data, total_count, total_pages = get_data_from_api(url)
            if total_pages == 0:
                logger.info("...No subscribers in group %s", group)
                break

            logger.info("Reading page %d of %d...", page_number, total_pages)
            subscribers: List = response_data.get("data")
            df = concat([df, DataFrame(subscribers)])
            df = df.drop_duplicates(['id'
                , 'firstName'
                , 'lastName'
                #, 'middleName'
                , 'msisdn'
                #, 'landLineNo'
                #,  'email'
                , 'status'
                , 'carrierName'
                , 'carrierId'
                , 'firstOptInDate'
                , 'lastOptinDate'
                , 'lastOptoutDate'
                , 'optinType'])            
            page_number += 1
            if page_number > total_pages:
                logger.info("...%d record(s) parsed", total_count)
                break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["GROUP_NAME"] = group
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            if "PROPERTIES" in df.columns:
                df["PROPERTIES"] = DataFrame(
                    df["PROPERTIES"].apply(transpose_dictionaries)
                )
            if "RELATIONS" in df.columns:
                df["RELATIONS"] = DataFrame(
                    df["RELATIONS"].apply(transpose_dictionaries)
                )
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, temp_table_name)

    switch_table(from_name=temp_table_name, to_name=table_name)


@task
def get_templates():
    """
    Extracts templates from API endpoint, converts blank values to nulls and
    loads data into Snowflake. Templates table is replaced if it exists, created
    if it doesn't.
    """
    global default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "TEMPLATES"
    temp_table_name: str = f"{table_name}{temp_table_suffix}"
    drop_table(table_name=temp_table_name)

    for group in groups:
        page_number: int = 1
        df: DataFrame = DataFrame()
        while True:  # page-level loop
            url: str = (
                f"{base_url}/groups/{group}/templates"
                f"?pageNumber={page_number}&pageSize={page_size}"
            )
            response_data, total_count, total_pages = get_data_from_api(url)
            if total_pages == 0:
                logger.info("...no templates in group %s", group)
                break

            logger.info("Reading page %d of %d...", page_number, total_pages)
            templates: List = response_data.get("data")
            df = concat([df, DataFrame(templates)])
            page_number += 1
            if page_number > total_pages:
                logger.info("...%d record(s) read", total_count)
                break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["GROUP_NAME"] = group
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, temp_table_name)

    switch_table(from_name=temp_table_name, to_name=table_name)


@task
def get_shorturl_clicks():
    """
    Extracts click totals from API endpoint, converts blank values to nulls and
    loads data into Snowflake. Clicks table is replaced if it exists, created
    if it doesn't.

    """
    global default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "SHORTURL_CLICKS"
    temp_table_name: str = f"{table_name}{temp_table_suffix}"
    drop_table(temp_table_name)

    for brand in brands:
        page_number: int = 1
        df: DataFrame = DataFrame()
        while True:  # page-level loop
            url: str = (
                f"{base_url}/brands/{brand}/shorturl-clicks"
                f"?pageNumber={page_number}&pageSize={page_size}"
            )
            response_data, total_count, total_pages = get_data_from_api(url)
            if total_pages == 0:
                logger.info("No clicks!")
                break

            logger.info("Parsing page %d of %d...", page_number, total_pages)
            clicks: List = response_data.get("data")
            df: DataFrame = concat([df, DataFrame(clicks)])
            page_number += 1
            if page_number > total_pages or page_number:
                logger.info("...%d record(s) parsed", total_count)
                break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["BRAND"] = brand
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, temp_table_name)

    switch_table(from_name=temp_table_name, to_name=table_name)


@task
def get_shorturl_click_details():
    """
    Extracts click details from API endpoint, cleans blank values, and loads
    into Snowflake. If table doesn't exist, it's created. If table doesn't
    exist or is empty, all click history is loaded; otherwise, only data not
    loaded is fetched and inserted.
    """
    global api_date_format, default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "SHORTURL_CLICK_DETAILS"
    data_start_at: DateTime = get_latest_timestamp_from_table(
        table_name, column_name="CLICKEDDATETIME"
    ).add(seconds=1)
    request_time: DateTime = now("UTC")
    download_dates: interval = interval(data_start_at, request_time)

    for brand in brands:
        df: DataFrame = DataFrame()
        for download_date in download_dates:
            from_date: str = download_date.format(api_date_format)
            data_end_at: DateTime = min(
                download_date.add(days=1).subtract(microseconds=1),
                request_time
            )  # future date causes HTTP 400, Bad Request
            to_date: str = data_end_at.format(api_date_format)
            page_number: int = 1
            while True:  # page-level loop
                url: str = (
                    f"{base_url}/brands/{brand}/shorturl-click-details"
                    f"?pageNumber={page_number}&pageSize={page_size}"
                    f"&fromDate={from_date}&toDate={to_date}"
                )
                response_data, total_count, total_pages = get_data_from_api(
                    url
                )
                if total_pages == 0:
                    logger.info("No click details!")
                    break

                logger.info(
                    "Parsing page %d of %d...", page_number, total_pages
                )
                click_details: List = response_data.get("data")
                df = concat([df, DataFrame(click_details)])
                page_number += 1
                if page_number > total_pages or page_number:
                    logger.info("...%d record(s) parsed", total_count)
                    break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["BRAND"] = brand
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, table_name)


@task
def get_inbound_messages():
    """
    Extracts inbound messages from API endpoint, cleans blank values, and loads
    into Snowflake. If table doesn't exist, it's created. If table doesn't
    exist or is empty, all message history is loaded; otherwise, only data not
    loaded is fetched and inserted.
    """
    global api_date_format, default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "INBOUND_MESSAGES"
    data_start_at = get_latest_timestamp_from_table(
        table_name, column_name="RECEIVEDAT"
    ).add(seconds=1)
    request_time: DateTime = now("UTC")
    download_dates: interval = interval(data_start_at, request_time)

    for group in groups:
        df: DataFrame = DataFrame()
        for download_date in download_dates:
            from_date: str = download_date.format(api_date_format)
            data_end_at: DateTime = min(
                download_date.add(days=1).subtract(microseconds=1),
                request_time
            )  # future date causes HTTP 400, Bad Request
            to_date: str = data_end_at.format(api_date_format)
            page_number: int = 1
            while True:  # page-level loop
                url: str = (
                    f"{base_url}/groups/{group}/inbound-messages"
                    f"?pageNumber={page_number}&pageSize={page_size}"
                    f"&fromDate={from_date}&toDate={to_date}"
                )
                response_data, total_count, total_pages = get_data_from_api(
                    url
                )
                if total_pages == 0:
                    logger.info("No inbound messages!")
                    break

                logger.info(
                    "Reading page %d of %d...", page_number, total_pages
                )
                messages: List = response_data.get("data")
                df = concat([df, DataFrame(messages)])
                page_number += 1
                if page_number > total_pages:
                    logger.info("...%d record(s) read", total_count)
                    break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["GROUP_NAME"] = group
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            if "SUBSCRIBERCUSTOMPARAMS" in df.columns:
                df["SUBSCRIBERCUSTOMPARAMS"] = DataFrame(
                    df["SUBSCRIBERCUSTOMPARAMS"].apply(transpose_dictionaries)
                )
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, table_name)


@task
def get_outbound_messages():
    """
    Extracts outbound messages from API endpoint, cleans blank values, and loads
    into Snowflake. If table doesn't exist, it's created. If table doesn't
    exist or is empty, all message history is loaded; otherwise, only data not
    loaded is fetched and inserted.
    """
    global api_date_format, default_page_size, temp_table_suffix
    page_size: int = default_page_size
    table_name: str = "OUTBOUND_MESSAGES"
    data_start_at = get_latest_timestamp_from_table(
        table_name, column_name="DELIVEREDAT"
    ).add(seconds=1)
    request_time: DateTime = now("UTC")
    download_dates: interval = interval(data_start_at, request_time)

    for group in groups:
        df: DataFrame = DataFrame()
        for download_date in download_dates:
            from_date: str = download_date.format(api_date_format)
            data_end_at: DateTime = min(
                download_date.add(days=1).subtract(microseconds=1),
                request_time
            )  # future date causes HTTP 400, Bad Request
            to_date: str = data_end_at.format(api_date_format)
            page_number: int = 1
            while True:
                url: str = (
                    f"{base_url}/groups/{group}/outbound-messages"
                    f"?pageNumber={page_number}&pageSize={page_size}"
                    f"&fromDate={from_date}&toDate={to_date}"
                )
                response_data, total_count, total_pages = get_data_from_api(
                    url
                )
                if total_pages == 0:
                    logger.info(
                        "No outbound messages for group %s from %s to %s.",
                        group, from_date, to_date
                    )
                    break

                logger.info(
                    "Reading page %d of %d for group %s from %s to %s...",
                    page_number, total_pages, group, from_date, to_date
                )
                messages: List = response_data.get("data")
                df = concat([df, DataFrame(messages)])
                page_number += 1
                if page_number > total_pages or page_number:
                    logger.info("...%d records read", total_count)
                    break

        if not df.empty:
            df.columns = df.columns.str.upper()
            df.reset_index(drop=True, inplace=True)
            df["GROUP_NAME"] = group
            df["_AIRFLOADED_AT_UTC"] = now("UTC")
            if "SUBSCRIBERCUSTOMPARAMS" in df.columns:
                df["SUBSCRIBERCUSTOMPARAMS"] = DataFrame(
                    df["SUBSCRIBERCUSTOMPARAMS"].apply(transpose_dictionaries)
                )
            if "USER" in df.columns:
                df["USER"] = df["USER"].apply(replace_empty_with_none)
            df = df.replace(NaN, None).replace("", None).replace("null", None)
            write_pandas_to_database(df, table_name)


@dag(
    dag_id=this_filename,
    description="Extracts SBT API data and loads into the data warehouse",
    tags=['data', 'sbt', 'solutions by text'],
    schedule="33 09 * * *",  # Daily 0333 CST/0433 CDT
    start_date=api_start_date,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=api_start_date,
        depends_on_past=False,
        retries=1,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=60),
        on_failure_callback=task_failure_slack_alert
    )
)
def sbt_extract_and_load():
    #get_access_token()  # Necessary to prevent race condition by parallel tasks
    get_shorturl_clicks()
    get_shorturl_click_details()
    get_inbound_messages()
    get_outbound_messages()
    get_templates()
    get_subscribers()


sbt_extract_and_load()

