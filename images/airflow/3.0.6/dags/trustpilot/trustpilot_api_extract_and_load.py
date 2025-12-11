import json
import logging
import os
from datetime import timedelta
from textwrap import dedent
from typing import Any, Dict, List, Tuple

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowFailException
from pandas import DataFrame, concat
from pendulum import (
    DateTime, datetime, now, parse
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from trustpilot import client

from above.common.constants import RAW_DATABASE_NAME, SNOWFLAKE_CONN_ID
from above.common.slack_alert import task_failure_slack_alert
from above.common.snowflake_utils import (
    dataframe_to_snowflake,
    query_to_dataframe
)

logger: logging.Logger = logging.getLogger(__name__)
this_filename: str = str(os.path.basename(__file__).replace(".py", ""))

snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
snowflake_connection = snowflake_hook.get_conn()
database_schema: str = "TRUSTPILOT"

trustpilot_env: Dict = json.loads(Variable.get("trustpilot"))
BUSINESS_UNIT_ID: str = trustpilot_env["BUSINESS_UNIT_ID"]
api_start_timestamp: DateTime = datetime(1970, 1, 1, tz='UTC')

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
    global api_start_timestamp, database_schema
    query: str = dedent(
        f"""
        SELECT NVL(
                    MAX(CONVERT_TIMEZONE('UTC', "{column_name}")), 
                    '{api_start_timestamp.isoformat()}'::TIMESTAMP_TZ
               ) AS latest_timestamp
        FROM {RAW_DATABASE_NAME}.{database_schema}.{table_name}
        """
    )
    latest_timestamp: DateTime = api_start_timestamp
    try:
        df: DataFrame = query_to_dataframe(query)
        latest_timestamp: DateTime = parse(str(df.latest_timestamp.values[0]))
        logger.info(f"Most recent record timestamp is {latest_timestamp}")
        logger.info(f"Timestamp type is {latest_timestamp}")
    except ProgrammingError as error:
        logger.error("Exception: %s %s", type(error), error)
        logger.warning(
            "Table %s doesn't exist or not authorized; using %s",
            table_name, api_start_timestamp
        )
    return latest_timestamp   

@task
def get_private_reviews():
    global api_start_timestamp, database_schema

    def get_review_timestamp(review: Dict[str, Any]) -> DateTime:
        review_timestamp: DateTime = parse(review.get("createdAt"))
        if review.get("updatedAt") is not None:
            updated_at: DateTime = parse(review.get("updatedAt"))
            review_timestamp: max((review_timestamp, updated_at))
        return review_timestamp

    logger.info("Getting warehouse last review timestamp...")
    api_start_timestamp = get_latest_timestamp_from_table(
        "REVIEWS", "_AIRFLOADED_AT"
    )
    number_of_reviews_per_page: int = 100  # default 20, 100 max
    rows: List = []
    page_number: int = 0
    has_next_page: bool = True
    while has_next_page:
        page_number += 1
        logger.info("Getting private reviews, page %d...", page_number)
        params: Dict[str, Any] = dict(
            orderBy=["createdat.desc"],
            startDateTime=api_start_timestamp.isoformat(),
            perPage=number_of_reviews_per_page,
            page=page_number
        )

        client.default_session.setup(
            api_host=trustpilot_env["API_HOST"],
            api_key=trustpilot_env["API_KEY"]
        )
        response = client.get(
            f"https://api.trustpilot.com/v1/business-units/{BUSINESS_UNIT_ID}/reviews",
            params=params
        )

        if response.status_code != 200:
            logger.error(
                f"Requests error {response.status_code}: {response.reason}"
            )
            #If we do not get 200, force task to fail
            raise AirflowFailException()
        
        response_json: Any = response.json()
        reviews: List = response_json.get("reviews", [])
        new_reviews: List = list(filter(
            lambda review: get_review_timestamp(review) > api_start_timestamp,
            reviews
        ))
        rows.extend(new_reviews)
        has_next_page = bool(list(filter(
            lambda link: link.get("rel") == "next-page",
            response_json.get("links")
        )))

    if rows:
        logger.info("Storing %d review(s)...", len(rows))
        df: DataFrame = DataFrame(rows)
        df["_AIRFLOADED_AT"] = now("UTC")
        df.reset_index(drop=True, inplace=True)
        df.replace(to_replace=["None", "null", ""], value=None, inplace=True)
        df.columns = df.columns.str.upper()
        dataframe_to_snowflake(
            df,
            database_name=RAW_DATABASE_NAME,
            schema_name=database_schema,
            table_name="REVIEWS",
            overwrite=False
        )
    else:
        logger.info("No new reviews to store.")

def _get_new_consumer_ids_from_reviews() -> Tuple:
    global api_start_timestamp, database_schema

    logger.info("Getting new consumer IDs...")
    query: str = dedent(
        f"""
            select consumer:id as consumer_id
            FROM {RAW_DATABASE_NAME}.{database_schema}.REVIEWS r 
            left outer join {RAW_DATABASE_NAME}.{database_schema}.CONSUMERS c on r.consumer:id = c.id
            where c.id is null
        """
    )
    try:
        df: DataFrame = query_to_dataframe(query)
        consumer_ids: Tuple = tuple(df.consumer_id)
        logger.info("Found %s new consumer ID(s)", len(consumer_ids))
    except ProgrammingError as error:
        logger.error("Exception: %s %s", type(error), error)
        consumer_ids: Tuple = tuple()
    return consumer_ids

@task
def get_consumer_profiles():
    global api_start_timestamp, database_schema
    consumer_ids: Tuple = _get_new_consumer_ids_from_reviews()
    consumer_ids = list(consumer_ids) 
    #remove double qoute from list for proper request
    consumer_ids = [i.replace('"', '') for i in consumer_ids]
    json_payload = dict(consumerIds=consumer_ids)

    df: DataFrame = DataFrame()

    client.default_session.setup(
                api_host=trustpilot_env["API_HOST"],
                api_key=trustpilot_env["API_KEY"]
            )

    response = client.post(
        f"https://api.trustpilot.com/v1/consumers/profile/bulk",
        json=json_payload
    )

    status_code = response.status_code

    if response.status_code != 200:
        logger.error(
            f"Requests error {status_code}: {response.reason}"
        )
        #If we do not get 200, force task to fail
        raise AirflowFailException()
    response_json: Any = response.json()
    consumers: Dict = response_json.get("consumers", {})
    df = concat([df, DataFrame.from_dict(consumers, orient='index')])

    if not df.empty:
        logger.info("Storing %d consumer profile(s)...", len(df))
        df["_AIRFLOADED_AT"] = now("UTC")
        df.reset_index(drop=True, inplace=True)
        df.replace(to_replace=["None", "null", ""], value=None, inplace=True)
        df.columns = df.columns.str.upper()
        dataframe_to_snowflake(
            df,
            database_name=RAW_DATABASE_NAME,
            schema_name=database_schema,
            table_name="CONSUMERS",
            overwrite=False
        )
    else:
        logger.info("No new consumer profiles to store.")

@dag(
    dag_id=this_filename,
    description="Extracts Trustpilot API data, loads into the data warehouse",
    tags=['data', 'trustpilot'],
    schedule="17 09 * * *",  # Daily 0317 CST/0417 CDT
    start_date=api_start_timestamp,
    max_active_runs=1,
    catchup=False,
    default_args=dict(
        owner="Data Engineering",
        start_date=api_start_timestamp,
        depends_on_past=False,
        retries=0,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=60),
        on_failure_callback=task_failure_slack_alert
    )
)

def trustpilot_extract_and_load():
    #Use consumer in Reviews to get consumer profiles
    get_private_reviews() >> get_consumer_profiles()

trustpilot_extract_and_load()