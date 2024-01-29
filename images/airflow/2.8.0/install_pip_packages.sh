#!/bin/bash

AIRFLOW_AMAZON_PROVIDERS_VERSION=8.13.0
AIRFLOW_VERSION=2.8.0
PYTHON_MAJOR_MINOR_VERSION=3.11

CONSTRAINT_FILE="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"
pip3 install --constraint "${CONSTRAINT_FILE}" \
    autopep8 \
    jinja2 \
    pycurl \
    psycopg2 \
    "celery[sqs]" \
    "apache-airflow[celery,statsd]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-amazon[aiobotocore]==${AIRFLOW_AMAZON_PROVIDERS_VERSION}" \
    watchtower