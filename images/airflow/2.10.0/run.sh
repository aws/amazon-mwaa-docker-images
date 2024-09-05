#!/bin/bash
set -e

# Build the Docker image
./build.sh

ACCOUNT_ID="" # Put your account ID here.
ENV_NAME="" # Choose an environment name here.

# AWS Credentials
AWS_ACCESS_KEY_ID="" # Put your credentials here.
AWS_SECRET_ACCESS_KEY="" # Put your credentials here.
AWS_SESSION_TOKEN="" # Put your credentials here.
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

# MWAA Configuration
MWAA__CORE__REQUIREMENTS_PATH="/usr/local/airflow/requirements/requirements.txt"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-DAGProcessing"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-Scheduler"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-Task"
MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-Scheduler"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-WebServer"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN="arn:aws:logs:us-west-2:${ACCOUNT_ID}:log-group:${ENV_NAME}-Worker"
MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL="INFO"
export MWAA__CORE__REQUIREMENTS_PATH
export MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL
export MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL
export MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL
export MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL
export MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL
export MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED
export MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN
export MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL

docker compose up 
