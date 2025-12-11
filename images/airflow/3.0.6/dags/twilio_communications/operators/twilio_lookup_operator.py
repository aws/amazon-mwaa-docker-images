"""Twilio Lookup Operator for reverse phone number lookups."""

import json
import logging
import os
import time
from typing import Any, Optional, Dict, List
from textwrap import dedent
from botocore.exceptions import ClientError

import pandas as pd
from pandas import DataFrame
from pendulum import datetime, now, today, instance, duration
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from jinja2 import Template
from twilio.base.exceptions import TwilioRestException
from twilio.rest import Client
from twilio.rest.lookups.v2.phone_number import PhoneNumberInstance
from snowflake.connector import SnowflakeConnection
from sqlalchemy.engine import Engine

from above.common.constants import (
    RAW_DATABASE_NAME,
    SNOWFLAKE_CONN_ID,
    TRUSTED_DATABASE_NAME,
    ENVIRONMENT_FLAG,
    S3_DATALAKE_BUCKET,
)
from above.common.snowflake_utils import dataframe_to_snowflake, query_to_dataframe
from above.common.s3_utils import upload_string_to_s3, upload_file_to_s3

logger = logging.getLogger(__name__)

S3_LOG_DIRECTORY_SUFFIX: str = (
    "results/prod/twilio_lookup_logs/"
    if ENVIRONMENT_FLAG == "prod"
    else "results/dev/twilio_lookup_logs/"
)
S3_LOG_DIRECTORY: str = os.path.join(S3_DATALAKE_BUCKET, S3_LOG_DIRECTORY_SUFFIX)


# Default constants
DEFAULT_TWILIO_FIELDS = ["caller_name", "line_type_intelligence"]
DEFAULT_LOOKUP_REFRESH_MONTHS = 12
DEFAULT_API_DELAY_SECONDS = 0.05
DEFAULT_MAX_FAILED_TO_LOG = 10

LOOKUP_LIMIT: int = 5000 if ENVIRONMENT_FLAG == "prod" else 2  # Prevent costly runaways
twilio_fields: List = ["caller_name", "line_type_intelligence"]
twilio_credentials: Dict = json.loads(Variable.get("twilio"))
twilio_client: Client = Client(
    twilio_credentials.get("TWILIO_ACCOUNT_SID"),
    twilio_credentials.get("TWILIO_AUTH_TOKEN"),
)
RAW_SCHEMA_NAME: str = "TWILIO"
RAW_TABLE_NAME: str = "REVERSE_NUMBER_LOOKUPS"
snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
snowflake_hook.database = RAW_DATABASE_NAME
snowflake_hook.schema = RAW_SCHEMA_NAME
sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
snowflake_connection: SnowflakeConnection = snowflake_hook.get_conn()

SQL_DIR: str = os.path.join(os.path.dirname(__file__), "..", "sql")
LOOKUP_REFRESH_MONTHS: int = 12  # Refresh lookups older than this
TWILIO_FIELDS: List[str] = ["caller_name", "line_type_intelligence"]
# Define all columns for the merge operation to avoid duplication
MERGE_COLUMNS: List[str] = [
    "PHONE_NUMBER_E164",
    "PHONE_NUMBER_NATIONAL_FORMAT",
    "PHONE_TYPE",
    "CARRIER_NAME",
    "COUNTRY_CODE",
    "CALLING_COUNTRY_CODE",
    "MOBILE_COUNTRY_CODE",
    "MOBILE_NETWORK_CODE",
    "CALLER_NAME",
    "CALLER_TYPE",
    "IS_VALID",
    "VALIDATION_ERRORS",
    "_ERROR_CODE_CALLER",
    "_ERROR_CODE_LINE_TYPE",
    "_LAST_LOOKUP",
    "_AIRFLOADED_AT",
]


class TwilioLookupOperator(BaseOperator):
    """Operator for performing Twilio reverse phone number lookups and loading to Snowflake.
    This operator:
    - Queries Snowflake for phone numbers needing lookup/refresh
    - Performs Twilio API lookups with rate limiting
    - Processes responses and extracts relevant fields
    - Writes results to Snowflake using MERGE pattern
    Args:
        raw_table_name: Target Snowflake table name for lookup results.
        raw_schema_name: Snowflake schema name.
        twilio_fields: List of Twilio API fields to request.
        lookup_limit: Maximum number of lookups to perform (safety limit).
        lookup_refresh_months: Refresh lookups older than this many months.
        api_delay_seconds: Delay between API calls to avoid rate limits.
        max_failed_numbers_to_log: Maximum number of failed phone numbers to log.
        sql_template_dir: Directory containing SQL template files.
        skip_on_empty: Skip execution if no numbers need lookup.
        write_to_snowflake: Whether to write results (False for testing).
        **kwargs: Additional BaseOperator arguments.
    """

    template_fields = ("lookup_limit", "lookup_refresh_months")

    def __init__(
        self,
        raw_table_name: str = "REVERSE_NUMBER_LOOKUPS",
        raw_schema_name: str = "TWILIO",
        twilio_fields: list[str] | None = None,
        lookup_limit: int = LOOKUP_LIMIT,
        lookup_refresh_months: int = DEFAULT_LOOKUP_REFRESH_MONTHS,
        api_delay_seconds: float = DEFAULT_API_DELAY_SECONDS,
        max_failed_numbers_to_log: int = DEFAULT_MAX_FAILED_TO_LOG,
        sql_template_dir: str | None = None,
        skip_on_empty: bool = True,
        write_to_snowflake: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.raw_table_name = raw_table_name
        self.raw_schema_name = raw_schema_name
        self.twilio_fields = twilio_fields or DEFAULT_TWILIO_FIELDS
        self.lookup_limit = lookup_limit
        self.lookup_refresh_months = lookup_refresh_months
        self.api_delay_seconds = api_delay_seconds
        self.max_failed_numbers_to_log = max_failed_numbers_to_log
        self.sql_template_dir = sql_template_dir
        self.skip_on_empty = skip_on_empty
        self.write_to_snowflake = write_to_snowflake

    def _upload_lookup_to_s3(
        self, record: dict, run_folder: str, phone_number: str
    ) -> None:
        """
        Uploads individual lookup record to S3 as JSON.
        """
        ts = now("UTC").to_iso8601_string().replace(":", "").replace("-", "")
        safe_number = phone_number.replace("+", "").replace("-", "").replace(" ", "")
        s3_key: str = f"{S3_LOG_DIRECTORY_SUFFIX}{run_folder}/{ts}_{safe_number}.json"

        upload_string_to_s3(
            string_value=json.dumps(record, indent=2),
            s3_bucket=S3_DATALAKE_BUCKET,
            s3_key=s3_key,
        )

    def _load_sql_file(self, filename: str) -> str:
        """
        Load SQL file from the sql directory.

        :param filename: Name of the SQL file to load
        :return: SQL content as string
        """
        sql_path: str = os.path.join(SQL_DIR, filename)
        try:
            with open(sql_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            raise AirflowException(f"SQL file not found: {sql_path}")
        except Exception as e:
            raise AirflowException(f"Error loading SQL file {sql_path}: {e}")

    def _render_sql_template(
        self, template_content: str, params: Dict[str, Any]
    ) -> str:
        """
        Render SQL template with Jinja2.

        :param template_content: SQL template content
        :param params: Parameters to render in the template
        :return: Rendered SQL string
        """
        template = Template(template_content)
        template_rendered = template.render(params=params)

        logger.info("Rendering SQL template with params: %s", params)
        logger.info("Template rendered: %s", template_rendered)

        return template_rendered

    def _build_active_numbers_query(self) -> str:
        """
        Build SQL query to find active phone numbers needing lookup.

        :return: SQL query string
        """
        template_content: str = self._load_sql_file("active_numbers_needing_lookup.sql")
        return self._render_sql_template(
            template_content,
            {
                "trusted_database": TRUSTED_DATABASE_NAME,
                "raw_database": RAW_DATABASE_NAME,
                "raw_schema": RAW_SCHEMA_NAME,
                "raw_table": RAW_TABLE_NAME,
                "lookup_refresh_months": LOOKUP_REFRESH_MONTHS,
            },
        )

    def _build_merge_query(self, suffix: str) -> str:
        """
        Build the SQL MERGE statement for updating the reverse lookups table.

        :param suffix: Suffix for the updates table name
        :return: SQL MERGE statement
        """
        # Columns to update (all except the primary key)
        update_columns = [col for col in MERGE_COLUMNS if col != "PHONE_NUMBER_E164"]

        template_content: str = self._load_sql_file("merge_reverse_lookup.sql")
        rendered = self._render_sql_template(
            template_content,
            {
                "raw_database": RAW_DATABASE_NAME,
                "raw_schema": RAW_SCHEMA_NAME,
                "raw_table": RAW_TABLE_NAME,
                "suffix": suffix,
                "update_columns": update_columns,
                "insert_columns": MERGE_COLUMNS,
            },
        )
        logger.info("Rendered template: %s", rendered)
        return rendered

    def fetch_numbers_to_look_up(self) -> DataFrame:
        """
        Queries new leads/applicants' phone numbers and existing phone numbers
        that need a lookup refresh (every 3 months)
        :return: phone numbers reverse lookup dataframe
        """
        global LOOKUP_LIMIT
        active_numbers_needing_lookup_query = self._build_active_numbers_query()
        phone_numbers_df: DataFrame = query_to_dataframe(
            active_numbers_needing_lookup_query
        )
        if (number_of_lookups := len(phone_numbers_df.index)) > LOOKUP_LIMIT:
            logger.warning(
                "Number of phone numbers (%d) exceeds failsafe lookup limit %d!  Using the head of the dataframe with limit %d.",
                number_of_lookups,
                LOOKUP_LIMIT,
                LOOKUP_LIMIT,
            )
            phone_numbers_df = phone_numbers_df.head(LOOKUP_LIMIT)

        logger.info("%d phone numbers need reverse lookups", number_of_lookups)
        return phone_numbers_df

    def fetch_and_lookup_numbers(self) -> None:
        """
        Fetches a dataframe of phone numbers with reverse lookup fields, populates
        the lookup fields via the Twilio API, writes them to a new update table,
        then merges the update table with the reverse lookup table
        :return: None
        """
        df: DataFrame = self.fetch_numbers_to_look_up()
        if df.empty:
            logger.info("All phone numbers already have current lookup information")
            return
        run_folder = now("UTC").format("YYYYMMDD_HHmmss")

        global twilio_client, twilio_fields
        for index in df.index:
            phone_number_to_look_up: str = df.at[index, "phone_number_e164"]
            # noinspection PyRedundantParentheses
            if last_lookup := df.at[index, "_last_lookup"]:
                last_verified_date: str = instance(last_lookup).format("YYYYMMDD")
            else:
                last_verified_date: str = "19700101"
            response: PhoneNumberInstance = twilio_client.lookups.v2.phone_numbers(
                phone_number_to_look_up
            ).fetch(
                fields=",".join(twilio_fields), last_verified_date=last_verified_date
            )
            df.at[index, "_last_lookup"] = today("UTC")
            df.at[index, "phone_number_e164"] = phone_number_to_look_up
            df.at[index, "phone_number_national_format"] = getattr(
                response, "national_format"
            )
            df.at[index, "calling_country_code"] = getattr(
                response, "calling_country_code"
            )
            df.at[index, "country_code"] = getattr(response, "country_code")
            df.at[index, "is_valid"] = getattr(response, "valid")
            validation_errors: List = getattr(response, "validation_errors")

            record = {
                "phone_number": phone_number_to_look_up,
                "caller_name": getattr(response, "caller_name", None),
                "phone_number_national_format": getattr(response, "national_format"),
                "_last_lookup": today("UTC").format("YYYY-MM-DD"),
            }
            self._upload_lookup_to_s3(record, run_folder, phone_number_to_look_up)
            if validation_errors:
                logger.warning("Validation errors: %s", validation_errors)
                df.at[index, "validation_errors"] = validation_errors
            else:
                df.at[index, "validation_errors"] = None

            if caller_details := getattr(response, "caller_name"):
                df.at[index, "caller_name"] = caller_details.get("caller_name")
                df.at[index, "caller_type"] = caller_details.get("caller_type")
                # noinspection PyRedundantParentheses
                if error_code := caller_details.get("error_code"):
                    logger.warning("Caller error code: %d", error_code)
                df.at[index, "_error_code_caller"] = error_code

            if line_details := getattr(response, "line_type_intelligence"):
                df.at[index, "phone_type"] = line_details.get("type")
                df.at[index, "carrier_name"] = line_details.get("carrier_name")
                df.at[index, "mobile_country_code"] = line_details.get(
                    "mobile_country_code"
                )
                df.at[index, "mobile_network_code"] = line_details.get(
                    "mobile_network_code"
                )
                # noinspection PyRedundantParentheses
                if error_code := line_details.get("error_code"):
                    logger.warning("Line type error code: %d", error_code)
                df.at[index, "_error_code_line_type"] = error_code

        df["_airfloaded_at"] = now("UTC")
        df.columns = df.columns.str.upper()
        suffix: str = "_UPDATES"

        merge_query: str = self._build_merge_query(suffix)
        dataframe_to_snowflake(
            df,
            database_name=RAW_DATABASE_NAME,
            schema_name=RAW_SCHEMA_NAME,
            table_name=f"{RAW_TABLE_NAME}{suffix}",
            overwrite=True,
            snowflake_connection=snowflake_connection,
        )
        snowflake_connection.cursor().execute(merge_query)

    def execute(self, context: dict[str, Any]) -> None:
        """Execute the Twilio lookup operator."""
        if self.skip_on_empty:
            phone_numbers_df: DataFrame = self.fetch_numbers_to_look_up()
            if phone_numbers_df.empty:
                logger.info(
                    "No phone numbers need reverse lookups. Skipping execution."
                )
                return

        if self.write_to_snowflake:
            self.fetch_and_lookup_numbers()
        else:
            logger.info("write_to_snowflake is False. Skipping data write.")
