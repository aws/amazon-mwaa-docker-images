import logging
import os
from time import sleep
from typing import Dict
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import pendulum
import yaml
import json
import pandas as pd
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import DagModel
from airflow.utils.session import create_session
from airflow.models import Variable
from pandas import DataFrame
from pendulum import datetime, now
import uuid
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from above.common.constants import (SNOWFLAKE_CONN_ID, SNOWFLAKE_SWAT_VIEWS,
                                    SWAT_YAML_FOLDER, ENVIRONMENT_FLAG)

logger: logging.Logger = logging.getLogger(__name__)

# Slack settings
SLACK_TOKEN = json.loads(Variable.get("SLACK_TOKEN"))["token"] 

DEFAULT_TIMEOUT: int = 15
OUTPUT_DIRECTORY = "/tmp"
MAX_ROWS=100

DEBUG_CHANNEL = "C08GUFS2472" # alerts-swat
DEV_CHANNEL   = "C08E2H7TYAD" # debug-swat


CHANNELS = {
    "alerts-allocation":             "C08K302FSUE",
    "alerts-apps-prod":              "C05DEQSBF71",
    "alerts-capital-markets":        "C09KURQ6353",
    "alerts-collections":            "C05GCG23XUY",
    "alerts-compliance":             "C074GDG4W30",
    "alerts-credit-model-variables": "C08UBKH9WAK",
    "alerts-data-bi":                "C04E17EPS9Y",
    "alerts-leads-eligibility":      "C099QRXJWJK",
    "alerts-servicing":              "C08SR04K1E1",
    "alerts-special-handling":       "C02TR6AGUAD",
    "logs-apps-prod":                "C053MEDFND9",
    "logs-collections":              "C09GU35A01K",
    "logs-credit-model-variables":   "C08PJCYKWN9",
    "logs-data-bi":                  "C09C9EQ0KN0",
    "logs-leads-eligibility":        "C0502AW8UJK",
    "logs-payments":                 "C04SA0HMDR7"
}

slack_client = WebClient(token=SLACK_TOKEN, timeout=DEFAULT_TIMEOUT)

def log_error(msg: str, job: str = "") -> None:
    if ENVIRONMENT_FLAG == "prod":
        send_slack_message(DEBUG_CHANNEL, job, msg)
    else:
        send_slack_message(DEV_CHANNEL, job, msg)
    logger.error(f"{job}: {msg}")

from datetime import datetime
from zoneinfo import ZoneInfo

def expand_hour_field(hour_field: str) -> list[int]:
    hours = set()
    parts = hour_field.split(',')
    for part in parts:
        if '-' in part:
            start, end = map(int, part.split('-'))
            hours.update(range(start, end + 1))
        else:
            hours.add(int(part))
    return sorted(hours)

def convert_local_standard_cron_to_utc(cron_expr: str, local_timezone: str = "America/Chicago") -> str:
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        log_error(f"Only 5-field cron expressions are supported: {cron_expr} ")
        raise ValueError(f"Only 5-field cron expressions are supported: {cron_expr}")

    minute_str, hours_field, day, month, weekday = fields
    
    if hours_field.strip() == "*":
        return f"{minute_str} * {day} {month} {weekday}"

    local_tz = ZoneInfo(local_timezone)
    utc = ZoneInfo("UTC")

    utc_hours = set()
    expanded_hours = expand_hour_field(hours_field)

    if expanded_hours == "*":
        return f"{minute_str} * {day} {month} {weekday}"


    for local_hour in expanded_hours:
        standard_date = datetime(datetime.now().year, 1, 1, local_hour, int(0), tzinfo=local_tz)

        current_date = datetime.now().date()
        local_with_dst = datetime.combine(current_date, standard_date.timetz()).replace(tzinfo=local_tz)

        utc_dt = local_with_dst.astimezone(utc)
        utc_hours.add(utc_dt.hour)

    new_hours_field = ",".join(map(str, sorted(utc_hours)))
    new_cron_expr = f"{minute_str} {new_hours_field} {day} {month} {weekday}"
    return new_cron_expr


# Function to send messages to Slack
def send_slack_message(chan: str, title: str, message: str = "") -> None:
    text = f"{title}\n{message}\nTimestamp: {now().to_datetime_string()}"
    if ENVIRONMENT_FLAG == "prod":
        channel = chan
    else:
        channel = DEV_CHANNEL
    try:
        logger.info(f"Posting message in {channel}")
        response = slack_client.chat_postMessage(channel=channel, text=f"{text}")
        logger.debug(f"postMessage response: {response}")
    except SlackApiError as error:
        log_error(f"Error sending message to channel {channel}: {error}")


# Function to send file to Slack
def send_slack_file(chan: str, filename: str, file_path: str, message: str) -> None:
    if ENVIRONMENT_FLAG == "prod":
        channel = chan
    else: 
        channel = DEV_CHANNEL

    try:
        sleep(1)
        response = slack_client.files_upload_v2(
            file=file_path,
            title=filename,
            channels=channel,
            initial_comment=f"{message}",
        )
        logger.debug(f"File upload response: {response}")
    except SlackApiError as e:
        log_error(f"Slack API error in channel: {channel}, comment: {message},  {e.response['error']}")
    except Exception as e:
        log_error(f"Error sending file to Slack: {str(e)}")



def log_to_snowflake(schema: str, job_name: str, title: str, result_count: int, results: str):
    if ENVIRONMENT_FLAG != "prod":
        return
    try:
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        chicago_time = pendulum.now("America/Chicago").to_datetime_string()
        sql = """
            INSERT INTO alerts.logs.alert_logs (log_timestamp, schema_name, job_name, title, result_count, results)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (chicago_time, schema.upper(), job_name.upper(), title, result_count, results)

        snowflake_hook.run(sql, parameters=params)
    except Exception as e:
        log_error(f"[{job_name}] Failed to log to Snowflake: {e}")


def log_to_snowflake_hist(schema: str, job_name: str, df):
    if ENVIRONMENT_FLAG != "prod":
        return
    try:
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        completed_at = pendulum.now("America/Chicago").to_datetime_string()
        job_run_uuid = str(uuid.uuid4())
        alert_key = f"{schema.upper()}::{job_name.upper()}"
        
        base_cols = [
            "completed_at",
            "schema_name",
            "job_name",
            "job_run_uuid",
            "alert_key",
            "record"
        ]       
       
        df_cols_lower = {c.lower(): c for c in df.columns} if df is not None and not df.empty else {}
        extra_cols = []
        if df is not None and not df.empty:
            if "context_key" in df.columns:
                extra_cols.append("context_key")
            if "unified_id" in df.columns:
                extra_cols.append("unified_id")

        all_cols = base_cols + extra_cols

        rows = []
        if df is not None and not df.empty:
            for _, record in df.iterrows():
                record_dict = record.to_dict()
                for k, v in record_dict.items():
                    if isinstance(v, (pd.Timestamp, datetime)) and v.tz is not None:
                        record_dict[k] = v.isoformat(timespec="milliseconds")
                values = [
                    completed_at,
                    schema.upper(),
                    job_name.upper(),
                    job_run_uuid,
                    alert_key,
                    json.dumps(record_dict),
                ]
                if "context_key" in df.columns:
                    values.append(record["context_key"])
                if "unified_id" in df.columns:
                    values.append(record["unified_id"])
                rows.append(values)
        else:
            # no rows in df, just log the metadata
            values = [completed_at, schema.upper(), job_name.upper(), job_run_uuid, alert_key, "{}"]
            rows.append(values)

        sql_values = []
        for r in rows:
            escaped = ["'" + str(x).replace("'", "''") + "'" if x is not None else "NULL" for x in r]
            sql_values.append(f"({', '.join(escaped)})")

        full_sql = (
            f"INSERT INTO alerts.logs.ALERT_CRITERIA_LOGS "
            f"({', '.join(all_cols)}) VALUES "
            + ",\n".join(sql_values)
        )

        snowflake_hook.run(full_sql)
    except Exception as e:    
        log_error(f"[{job_name}] Failed to log criteria to Snowflake: {e}")




def run_snowflake_query(query: str) -> DataFrame:
    try:
        snowflake_hook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        sql_engine = snowflake_hook.get_sqlalchemy_engine()
        dataframe = pd.read_sql(query, sql_engine)
        return dataframe
    except Exception as e:
        send_slack_message(
            DEBUG_CHANNEL, "SQL Error in run_snowflake_query", f"Error: {e}"
        )
        logger.error(f"Failed to execute query: {query}\nError: {e}")
        return pd.DataFrame()

def get_extract_df(job_name: str, schema: str) -> DataFrame:
    query = f"SELECT * FROM {SNOWFLAKE_SWAT_VIEWS}.{schema}.{job_name}"
    try:
        df = run_snowflake_query(query)
        if not isinstance(df, pd.DataFrame):
            log_error(f"[{job_name}] Query result is not a DataFrame.")
            raise TypeError(f"[{job_name}] Query result is not a DataFrame.")
        return df
    except Exception as e:
        log_error(f"[{job_name}] Unexpected error: {e}")
        return pd.DataFrame()


def process_alert(job_name: str, schema: str, channel: str | list[str], title: str, send_on_zero_results: bool, mute_flg):
    try:
        df = get_extract_df(job_name, schema)
        if mute_flg:
            if not df.empty or send_on_zero_results:
                log_to_snowflake_hist(schema, job_name, df)
        else:
            log_to_snowflake(schema, job_name, title, df.shape[0], df.head(MAX_ROWS).to_json(orient="records"))
            channels = [channel] if isinstance(channel, str) else channel
            if df.empty:
                if send_on_zero_results:
                    for ch in channels:
                        send_slack_message(ch, title, "No Results")
            else:
                file_name = f"{OUTPUT_DIRECTORY}/{schema}_{job_name}_{pendulum.now().format('YYYYMMDD_HHmmss')}.csv"
                df.to_csv(file_name, index=False)
                sleep(5)
                for ch in channels:
                    send_slack_file(ch, job_name, file_name, f"{title}\n")

    except Exception as e:
        log_error(f"[{job_name}] Unexpected error: {e}")


# Create dynamic DAG
def create_dynamic_dag(observation: Dict) -> DAG:
    required_keys = [
        "name",
        "cron_schedule",
        "job",
        "enabled",
        "schema"
    ]

    def get_nested_key(dct, keys):
        for key in keys.split("."):
            if isinstance(dct, dict) and key in dct:
                dct = dct[key]
            else:
                return None
        return dct

    
    missing_keys = [
        key for key in required_keys if get_nested_key(observation, key) is None
    ]
    if missing_keys:
        log_error(f"Missing required keys in observation: {', '.join(missing_keys)}")
        raise ValueError(f"Missing required keys in observation: {', '.join(missing_keys)}")
    try:
        bundle_name = observation["name"]
        cron_schedule = convert_local_standard_cron_to_utc(observation["cron_schedule"])
        schema = observation["schema"] 
        enabled_flg = observation.get("enabled", False)
      
        if enabled_flg == False:
            return None

        jobs = observation.get("job", [])
        if not isinstance(jobs, list):
            log_error(f"[{bundle_name}] 'job' must be a list of job definitions")
            raise ValueError(f"[{bundle_name}] 'job' must be a list")


        @dag(
            dag_id=f"swat__{bundle_name}",
            schedule_interval=cron_schedule,
            start_date=datetime(2025, 3, 20),
            catchup=False,
        )


        def dynamic_dag():
            previous_task = None
            for idx, job_info in enumerate(jobs):
                title = job_info.get("title")
                send_on_zero_results = job_info.get("send_on_zero_results", False)
                view_name = job_info.get("view_name")
                channel_key = job_info.get("channel", "MUTE")  # default MUTE
                mute_flg = True if channel_key.upper() == "MUTE" else False
                channel = CHANNELS.get(channel_key, None) if not mute_flg else None

                if not mute_flg and channel is None:
                    log_error(f"[{job_name}] Invalid channel specified: {channel_key}")
                    raise ValueError(f"[{job_name}] Invalid channel: {channel_key}")

                process_task = PythonOperator(
                    task_id=f"process_alert_{bundle_name}_{idx}",
                    python_callable=process_alert,
                    op_args=[ view_name, schema, channel, title, send_on_zero_results, mute_flg],
                )

                if previous_task:
                    previous_task >> process_task
                previous_task = process_task

                next_dags = job_info.get("next_dags", [])
                if isinstance(next_dags, str):
                    next_dags = [next_dags] 

                for next_dag in next_dags:
                    trigger_task = TriggerDagRunOperator(
                        task_id=f"trigger_{next_dag}_{idx}",
                        trigger_dag_id=f"swat__{next_dag}",
                        wait_for_completion=False,
                    )
                    process_task >> trigger_task

        return dynamic_dag

    except KeyError as e:
        log_error(f"[{job_name}] Key error: {e}")
        raise e

    except ValueError as e:
        log_error(f"[{job_name}] Value error: {e}")
        raise e




    except KeyError as e:
        log_error(f"[{job_name}] Key error: {e}")
        raise e

    except ValueError as e:
        log_error(f"[{job_name}] Value error: {e}")
        raise e


def load_jobs_from_yaml_directory():
    jobs = []
    for filename in os.listdir(SWAT_YAML_FOLDER):
        if filename.endswith(".yaml"):
            file_path = os.path.join(SWAT_YAML_FOLDER, filename)
            try:
                with open(file_path, "r") as file:
                    yaml_data = yaml.safe_load(file)
                    schema = list(yaml_data.keys())[0]
                    observations = yaml_data.get(schema, [])
                    for observation in observations:
                        observation["schema"] = schema
                        jobs.append(observation)
                        logger.info(
                            f"Loaded observation: {observation.get('key')} from {filename}"
                        )
            except yaml.YAMLError as e:
                log_error(f"Error loading YAML from {file_path}: {str(e)}")
            except Exception as e:
                log_error(f"Error loading YAML from {file_path}: {str(e)}")
    return jobs


@dag(
    "static_main_swat",
    default_args={"owner": "data", "retries": 0},
    schedule_interval="20 * * * *",
    start_date=datetime(2025, 3, 20),
    catchup=False,
)


def alert_dag():
    dynamic_dag_ids = []
    observations = load_jobs_from_yaml_directory()
    for observation in observations:
        dag = create_dynamic_dag(observation)
        if dag:
            globals()[dag.__name__] = dag()
            dynamic_dag_ids.append(dag().dag_id)

    paused = []
    for dag_id in dynamic_dag_ids:
        dag_model = DagModel.get_current(dag_id)
        if dag_model is None:
            paused.append(f"{dag_id} (not found)")
        elif dag_model.is_paused:
            paused.append(dag_id)

    if paused and ENVIRONMENT_FLAG == "prod":
        log_error(f"Paused dags {paused}")

alert_dag()
