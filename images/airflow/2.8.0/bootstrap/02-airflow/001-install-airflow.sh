#!/bin/bash
set -e

# shellcheck source=images/airflow/2.8.0/bootstrap/common.sh
source /bootstrap/common.sh

verify_env_vars_exist \
    AIRFLOW_AMAZON_PROVIDERS_VERSION \
    AIRFLOW_VERSION \
    PYTHON_VERSION

PYTHON_MAJOR_MINOR_VERSION=${PYTHON_VERSION%.*}

CONSTRAINT_FILE="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"
pip3 install --constraint "${CONSTRAINT_FILE}" \
    pycurl \
    psycopg2 \
    "celery[sqs]" \
    "apache-airflow[celery,statsd]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-amazon[aiobotocore]==${AIRFLOW_AMAZON_PROVIDERS_VERSION}" \
    watchtower
