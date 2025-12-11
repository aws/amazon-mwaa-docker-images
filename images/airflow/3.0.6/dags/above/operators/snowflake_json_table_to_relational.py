import json
import logging
import re

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeJsonTableToRelational(BaseOperator):

    def __init__(
            self,
            snowflake_conn_id,
            fully_qualified_table_name,
            relational_destination_name,
            relational_object_type='view',
            json_column_name='RAW_DATA',
            keep_extra_columns=True,
            *args,
            **kwargs
    ):
        super(SnowflakeJsonTableToRelational, self).__init__(*args, **kwargs)

        self.snowflake_conn_id = snowflake_conn_id
        self.fully_qualified_table_name = fully_qualified_table_name
        self.relational_destination_name = relational_destination_name
        self.relational_object_type = relational_object_type
        self.json_column_name = json_column_name
        self.keep_extra_columns = keep_extra_columns

    def execute(self, context):
        flatten_query_template = (
            "SELECT DISTINCT KEY FROM {}, LATERAL FLATTEN(INPUT => {})"
        )
        definition_template = "CREATE OR REPLACE VIEW {} AS SELECT {} FROM {}"

        field_diffs = None

        sf_hook = SnowflakeHook(self.snowflake_conn_id)
        sf_sqlalchemy_engine = sf_hook.get_sqlalchemy_engine()

        # try to get existing relational ddl
        try:
            existing_relational_ddl = pd.read_sql(
                '''SELECT GET_DDL('{}', '{}')'''.format(
                    self.relational_object_type,
                    self.relational_destination_name
                ),
                sf_sqlalchemy_engine
            )
            existing_relational_ddl = existing_relational_ddl.values[0][0]

        except:
            existing_relational_ddl = None
            logging.info(
                'Unable to fetch ddl - {}'.format(
                    self.relational_destination_name
                )
            )

        if existing_relational_ddl is not None:
            logging.info(
                'Relational object already exists,'
                ' performing incremental updates to DDL...'
            )

            # get existing columns in relational object from ddl
            # will match anything between RAW_DATA: and ::VARCHAR
            existing_columns = re.findall(
                r'(?<={}:)(.*)(?=::VARCHAR)'.format(self.json_column_name),
                existing_relational_ddl
            )

            # remove any quote wrapping if we have it
            existing_columns = [s.replace('"', '') for s in existing_columns]

            # get most recently loaded data
            most_recent_query = '''
            SELECT {json_column_name}
            FROM {table_name}
            WHERE RAW_LOAD_TIME_CST = (
              SELECT MAX(RAW_LOAD_TIME_CST)
              FROM {table_name}
            ) LIMIT 1
            '''.format(
                json_column_name=self.json_column_name,
                table_name=self.fully_qualified_table_name
            )

            most_recent_data = pd.read_sql(
                most_recent_query,
                sf_sqlalchemy_engine
            )

            if len(most_recent_data.index) > 0:
                # get keys from most recent load json
                most_recent_data_json = json.loads(
                    most_recent_data[self.json_column_name.lower()].iloc[0]
                )
                most_recent_data_keys = set(most_recent_data_json.keys())
            else:
                most_recent_data_keys = set()

            logging.info('-- Most recent load keys --')
            logging.info(most_recent_data_keys)
            # union existing and new cols
            cols = pd.DataFrame(
                set(existing_columns) | set(most_recent_data_keys),
                columns=['key']
            )

            # check for differences in previos load compared to most recent load
            previous_load_query = '''
            WITH previous_load_time AS (
              SELECT MAX(RAW_LOAD_TIME_CST) as previous_load
              FROM {table_name}
              WHERE RAW_LOAD_TIME_CST < (SELECT MAX(RAW_LOAD_TIME_CST)
                                         FROM {table_name})
            )
            SELECT {json_column_name}
            FROM {table_name}
            JOIN previous_load_time
            WHERE RAW_LOAD_TIME_CST = previous_load
            LIMIT 1
            '''.format(
                json_column_name=self.json_column_name,
                table_name=self.fully_qualified_table_name
            )

            previous_load_data = pd.read_sql(
                previous_load_query,
                sf_sqlalchemy_engine
            )

            if len(previous_load_data.index) > 0:
                # get keys from most recent load json
                previous_load_data_json = json.loads(
                    previous_load_data[self.json_column_name.lower()].iloc[0]
                )
                previous_load_data_keys = set(previous_load_data_json.keys())
            else:
                previous_load_data_keys = set()

            logging.info('-- Previous load keys --')
            logging.info(previous_load_data_keys)

            missing_fields = list(
                previous_load_data_keys - most_recent_data_keys
            )
            new_fields = list(most_recent_data_keys - previous_load_data_keys)

            # if there are any field changes, store them in field_diffs to be
            # passed via xcom and used in alerting
            if len(missing_fields + new_fields):
                field_diffs = {
                    'missing_fields': missing_fields,
                    'new_fields': new_fields
                }

        else:
            logging.info(
                'Relational object does not exist, performing full key scan...'
            )
            flatten_query = flatten_query_template.format(
                self.fully_qualified_table_name,
                self.json_column_name
            )

            cols = pd.read_sql(flatten_query, sf_sqlalchemy_engine)

        # create unique aliases for cols with same name but
        # different capitalization
        dup_cols = cols.key[cols.key.str.upper().duplicated(keep=False)]
        dup_mapper = {}
        for col in dup_cols:
            if col.upper() in dup_mapper.keys():
                dup_mapper[col.upper()].append(col)
            else:
                dup_mapper[col.upper()] = [col]

        alias_mapper = {}
        for k, v in dup_mapper.items():
            for i, c in enumerate(v):
                alias_mapper[c] = '{}_{}'.format(c.upper(), i + 1)

        cols['alias'] = cols.key.replace(alias_mapper)

        # wrap cols that start with a number in quotes
        # TODO expand this to more use cases as they pop up
        start_with_number = cols.key.str.match('[0-9]')
        cols.loc[start_with_number, 'key'] = '"' + cols.key.loc[
            start_with_number] + '"'
        cols.loc[start_with_number, 'alias'] = cols.key.loc[start_with_number]
        # if the name of the column is "group", there will be an error;
        # Prefix this with the name of the table
        col_is_called_group = cols.key.str.match('group')
        cols.loc[col_is_called_group, 'alias'] = \
            self.fully_qualified_table_name.split('.')[-1].lower() + '_' + \
            cols.key.loc[col_is_called_group]

        select_text_list = (
                self.json_column_name + ':' + cols.key
                + '::VARCHAR as ' + cols.alias
        ).to_list()

        if self.keep_extra_columns:

            df_sample = pd.read_sql(
                'SELECT * FROM {} LIMIT 1'.format(
                    self.fully_qualified_table_name
                ),
                sf_sqlalchemy_engine
            )

            extra_cols = [c.upper() for c in df_sample.columns]
            extra_cols.remove(self.json_column_name)

            select_text_list = select_text_list + extra_cols

        select_text = '\n, '.join(select_text_list)

        object_definition = definition_template.format(
            self.relational_destination_name,
            select_text,
            self.fully_qualified_table_name
        )

        with sf_sqlalchemy_engine.connect() as con:
            con.execute(object_definition)

        return field_diffs
