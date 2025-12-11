CREATE TABLE IF NOT EXISTS {{ params.database_name }}.{{ params.schema_name }}.{{ params.table_name }} (
    RAW_DATA        VARIANT
  , LOAD_TIME_CST   DATETIME
  , FILE_PATH       VARCHAR
)