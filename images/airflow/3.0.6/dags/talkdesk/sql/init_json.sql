CREATE SCHEMA IF NOT EXISTS {{ params.database_name }}.{{ params.schema_name }}
;

CREATE FILE FORMAT IF NOT EXISTS {{ params.database_name }}.{{ params.schema_name }}.JSON_FORMAT
  TYPE = json
  NULL_IF = ('', 'null')
;

CREATE OR REPLACE STAGE {{ params.database_name }}.{{ params.schema_name }}.{{ params.stage_name }} URL='{{ params.external_stage_url }}'
  STORAGE_INTEGRATION = {{ params.storage_integration_name }}
  file_format = {{ params.database_name }}.{{ params.schema_name }}.JSON_FORMAT
