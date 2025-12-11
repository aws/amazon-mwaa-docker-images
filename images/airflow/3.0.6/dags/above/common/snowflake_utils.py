import logging
import tempfile
from typing import Callable, Any, Optional

import numpy as np
import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas import DataFrame
from snowflake.connector import SnowflakeConnection
from sqlalchemy.engine import Engine
from snowflake.connector.pandas_tools import write_pandas
from above.common.constants import SNOWFLAKE_CONN_ID
from airflow.exceptions import AirflowFailException

logger: logging.Logger = logging.getLogger(__name__)


def get_file_format_function(file_format: str) -> Callable:

    def save_csv(dataframe: pd.DataFrame, temp_file: str) -> None:
        logger.info(f"Saving CSV file as {temp_file}...")
        dataframe.to_csv(temp_file)

    def save_txt(dataframe: pd.DataFrame, temp_file: str) -> None:
        numpy_array: np.ndarray = dataframe.to_numpy()
        logger.info(f"Saving text file as {temp_file}...")
        np.savetxt(temp_file, numpy_array, fmt="%s", delimiter="")

    file_format: str = file_format.lower()
    file_format_functions: dict[str, Callable[..., None]] = dict(
        csv=save_csv, txt=save_txt
    )
    if file_format not in file_format_functions.keys():
        raise AirflowException(
            f"Allowable file formats: {', '.join(file_format_functions.keys())}"
        )
    return file_format_functions[file_format]


def dataframe_to_s3(
    df: DataFrame,
    file_format: str,
    s3_bucket: str,
    s3_conn_id: str | None,
    s3_key: str,
) -> None:
    file_format_function: Callable[..., Any] = get_file_format_function(file_format)
    file_path: str = f"{tempfile.NamedTemporaryFile().name}.{file_format}"
    file_format_function(df, file_path)
    logger.info(f"Copying {file_path} to S3://{s3_bucket}/{s3_key}...")
    s3_hook: S3Hook = S3Hook(s3_conn_id)
    s3_hook.load_file(
        filename=file_path, key=s3_key, bucket_name=s3_bucket, replace=True
    )

def snowflake_query_to_pandas_dataframe(query: str, **context) -> DataFrame:
    """Queries the associated conn_id and returns the results in a pandas dataframe.

    Args:
        query (str): The SQL query to execute.

    Returns:
        DataFrame: Result of the query.
    """
    try:
        logger.info(f"Executing query:\n{query}")
        snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        df = snowflake_hook.get_pandas_df(sql=query)
        
        print(f"Retrieved {len(df)} rows from Snowflake")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nFirst few rows:\n{df.head()}")

    except ValueError as e:
        logger.error(f"Invalid value: {e}")
        raise AirflowFailException()
    except Exception as e:
        logger.error(f"Other error: {e}")
        raise AirflowFailException()
    
    return df


# NOTE: `query_to_dataframe` fairly old and it is a bit brittle at this point (2025Q3). 
# We should replace this with the above `snowflake_query_to_pandas_dataframe fn.`
def query_to_dataframe(query: str, fail_on_empty_result: bool = False) -> DataFrame:
    """
    Returns a DataFrame containing the result of the provided Snowflake query.
    :param query: A Snowflake query
    :param fail_on_empty_result: Optional. If set to True, will raise an
    exception if there are no rows in the result set. Defaults to False
    :return: DataFrame containing the result of the provided Snowflake query
    """
    logger.info("Executing query:")
    logger.info(query)
    snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
    sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
    dataframe: DataFrame = pd.read_sql(query, sql_engine)

    if dataframe.empty:
        if fail_on_empty_result:
            raise AirflowException("Query set to fail on empty result set")
        else:
            logger.warning("Query returned empty result set.")
    else:
        logger.info(f"Query complete: {len(dataframe.index)} rows returned.")

    return dataframe


def dataframe_to_snowflake(
    dataframe: DataFrame,
    database_name: str,
    schema_name: str,
    table_name: str,
    overwrite: bool = False,
    snowflake_connection: Optional[SnowflakeConnection] = None,
) -> tuple[bool, int]:
    """
    Inserts Pandas DataFrame data into the specified table in the configured
    raw database and schema
    :param dataframe: a Pandas DataFrame
    :param database_name: a Snowflake database name
    :param schema_name: a Snowflake schema name in database
    :param table_name: a Snowflake table name in schema
    :param overwrite: overwrite table rather than append to it
    :param snowflake_connection: a Snowflake connection with cursor
    :return: success flag, number of rows written
    """
    if snowflake_connection is None:
        snowflake_hook: SnowflakeHook = SnowflakeHook(SNOWFLAKE_CONN_ID)
        snowflake_connection = snowflake_hook.get_conn()

    success, number_of_chunks, number_of_rows, _ = write_pandas(
        snowflake_connection,
        dataframe,
        table_name,
        database=database_name,
        schema=schema_name,
        auto_create_table=True,
        quote_identifiers=True,
        use_logical_type=True,
        overwrite=overwrite,
    )
    if success:
        logger.info("%d row(s) written", number_of_rows)
    else:
        logger.error("write_pandas unsuccessful for %s", table_name)
    return success, number_of_rows


@task.short_circuit
def query_to_s3(
    s3_conn_id: str | None,
    s3_bucket: str,
    s3_key: str,
    snowflake_conn_id: str,
    query: str,
    file_format: str,
    fail_on_empty_result: bool = False,
) -> bool:
    get_file_format_function(file_format)

    logger.info("Executing query:")
    logger.info(query)
    snowflake_hook: SnowflakeHook = SnowflakeHook(snowflake_conn_id)
    sql_engine: Engine = snowflake_hook.get_sqlalchemy_engine()
    df: DataFrame = pd.read_sql(query, sql_engine)

    if df.empty:
        if fail_on_empty_result:
            raise AirflowException("Query set to fail on empty result set")
        else:
            logger.warning("Query returned empty result set: short-circuiting.")
            return False
    else:
        logger.info(f"Query complete: {len(df.index)} rows returned.")
        dataframe_to_s3(df, file_format, s3_bucket, s3_conn_id, s3_key)
        return True
