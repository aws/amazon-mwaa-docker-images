#!/bin/bash
set -e

# List of required environment variables
required_vars=("AIRFLOW_VERSION" "AIRFLOW_AMAZON_PROVIDERS_VERSION" "PYTHON_VERSION")

# Function to check if environment variables are set
check_env_vars() {
    for var in "${required_vars[@]}"; do
        if [[ -z ${!var} ]]; then
            echo "Error: Environment variable ${var} is not set."
            exit 1
        fi
    done
}

# Check required environment variables
check_env_vars

CONSTRAINT_FILE="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip3 install --constraint "${CONSTRAINT_FILE}" \
    pycurl \
    psycopg2 \
    "celery[sqs]" \
    "apache-airflow[celery,statsd]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-amazon[aiobotocore]==${AIRFLOW_AMAZON_PROVIDERS_VERSION}" \
    watchtower