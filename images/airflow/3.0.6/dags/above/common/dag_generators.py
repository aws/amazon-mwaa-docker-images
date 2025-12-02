import json
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from above.operators.snowflake_json_table_to_relational import \
    SnowflakeJsonTableToRelational
from above.operators.snowflake_load_from_s3_and_archive import \
    SnowflakeLoadFromS3AndArchive


def consolidate_field_diffs(task_ids, **context):
    consolidated_field_diffs = {}
    for task_id in task_ids:
        field_diffs = context['ti'].xcom_pull(
            task_ids=task_id, key='return_value'
        )
        logging.info(task_id)
        logging.info(field_diffs)
        logging.info('----------')
        if field_diffs:
            consolidated_field_diffs[task_id] = field_diffs

    # push consolidated diffs into xcom to be used by slack warning function
    context['ti'].xcom_push(
        key='consolidated_field_diffs', value=consolidated_field_diffs
    )


def load_raw_from_s3(
        dag_id,
        snowflake_conn_id,
        query_params,
        default_args,
        config_file_dir,
        sql_template_searchpath,
        schedule_interval=None,
        catchup=False,
        tags=None
):
    logging.info(
        f"Executing load_raw_from_s3 with dag_id={dag_id}, query_params="
        f"{query_params}, default_args={default_args}, config_file_dir="
        f"{config_file_dir}, sql_template_searchpath="
        f"{sql_template_searchpath}"
    )
    logging.info(
        f"Template files in sql_template_searchpath: "
        f"{os.listdir(sql_template_searchpath)}"
    )

    dag = DAG(
        dag_id,
        schedule_interval=schedule_interval,
        default_args=default_args,
        template_searchpath=sql_template_searchpath,
        catchup=catchup,
        tags=tags
    )

    with dag:
        init = SnowflakeOperator(
            task_id='init',
            snowflake_conn_id=snowflake_conn_id,
            sql='init_json.sql',
            params=query_params,
            dag=dag,
        )

        # NOTE: Removed a Bedrock block that does not apply to Above
        # https://github.com/Beyond-Finance/airflow-dags/blob
        # /15c94defe0bc1a1d20fd73b694864c2b77a3abd5/plugins/helpers
        # /dag_generators.py#L65
        generate_view_task_ids = []
        for file in os.listdir(config_file_dir):
            with open(os.path.join(config_file_dir, file), 'r') as f:
                config_args = json.load(f)

            task_id = file.replace('.json', '')

            # Airflow 2.0 connector will error if extra, unneeded kwargs are
            # passed (behavior in old version was to ignore them).
            query_params_load_from_s3 = query_params.copy()
            del (query_params_load_from_s3['storage_integration_name'])
            del (query_params_load_from_s3['external_stage_url'])
            load_json = SnowflakeLoadFromS3AndArchive(
                dag=dag,
                task_id='{}_load_json'.format(task_id),
                snowflake_conn_id=snowflake_conn_id,
                **config_args,
                **query_params_load_from_s3,
            )

            generate_view_task_id = '{}_generate_view'.format(task_id)
            globals()[generate_view_task_id] = SnowflakeJsonTableToRelational(
                dag=dag,
                task_id=generate_view_task_id,
                snowflake_conn_id=snowflake_conn_id,
                fully_qualified_table_name='{}.{}.{}'.format(
                    query_params['database_name'],
                    query_params['schema_name'],
                    config_args['table_name']
                ),
                relational_destination_name='{}.{}.{}_VW'.format(
                    query_params['database_name'],
                    query_params['schema_name'],
                    config_args['table_name']
                ),
            )

            generate_view_task_ids.append(generate_view_task_id)

            load_json.set_upstream(init)
            globals()[generate_view_task_id].set_upstream(load_json)

        consolidate = PythonOperator(
            task_id='consolidate_field_diffs',
            provide_context=True,
            python_callable=consolidate_field_diffs,
            op_kwargs={'task_ids': generate_view_task_ids},
            dag=dag,
        )

        for task_id in generate_view_task_ids:
            globals()[task_id].set_downstream(consolidate)

    return dag
