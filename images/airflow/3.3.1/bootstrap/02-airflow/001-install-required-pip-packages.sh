#!/bin/bash
set -e

# shellcheck source=images/airflow/3.3.1/bootstrap/common.sh
source /bootstrap/common.sh

# Define an array of required packages
REQUIRED_PACKAGES=(
    "apache-airflow-providers-amazon[aiobotocore]"
    "apache-airflow[celery,statsd]==${AIRFLOW_VERSION}"
    "apache-airflow-core[gunicorn]==${AIRFLOW_VERSION}"
    "apache-airflow-providers-postgres"
    "apache-airflow-providers-fab"
    "celery[sqs]"
    "boto3-stubs[logs]"
    "boto3-stubs[sqs]"
    psycopg2
    pycurl
    watchtower
    fluent-logger
    virtualenv
    apache-airflow-providers-standard
    # DuckDB runs its analytical SQL in-process, so the provider is only useful with the duckdb
    # library alongside it. Neither is in the essential constraints, so requirements.txt can
    # override the default versions installed here. 002-install-duckdb-extensions.sh adds the S3
    # extensions for this default duckdb.
    "apache-airflow-providers-duckdb==${DUCKDB_PROVIDER_VERSION}"
    "duckdb==${DUCKDB_VERSION}"
)

# safe-pip-install always install all required packages, along with whatever
# the user provides, hence we don't need to provide anything here.
safe-pip-install -c "${AIRFLOW_CONSTRAINTS_FILE}" "${REQUIRED_PACKAGES[@]}" 
