#!/bin/bash
set -e

# Check if 'podman' or 'finch' is available, otherwise use 'docker'
if command -v finch &> /dev/null; then
    CONTAINER_RUNTIME="finch"
elif command -v podman &> /dev/null; then
    CONTAINER_RUNTIME="podman"
else
    CONTAINER_RUNTIME="docker"
fi

# Generate valid Fernet key as json
generate_fernet_key() {

    # Install cryptography package quietly
    chmod +x temporary-pip-install generate_fernet_key.py
    ./temporary-pip-install cryptography >/dev/null 2>&1
    
    # Generate the key and format as JSON
    KEY=$(python3 generate_fernet_key.py)
    
    # Uninstall cryptography package quietly
    python3 -m pip uninstall -y cryptography cryptography-vectors &>/dev/null 2>&1
    
    echo "$KEY"
}

# Set up cache directory ; generate if it dosen't exist
CACHE_DIR="${HOME}/.cache/mwaa-local"
FERNET_KEY_FILE="${CACHE_DIR}/fernet.key"
mkdir -p "${CACHE_DIR}"

# Check if we have a cached Fernet key, if not generate and cache it
if [ ! -f "${FERNET_KEY_FILE}" ]; then
    generate_fernet_key > "${FERNET_KEY_FILE}"
    chmod 600 "${FERNET_KEY_FILE}"
fi

# Read the Fernet key from cache
FERNET_KEY=$(cat "${FERNET_KEY_FILE}")
export FERNET_KEY

# Build the Docker image
./build.sh $CONTAINER_RUNTIME

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
MWAA__CORE__TASK_MONITORING_ENABLED="false"
MWAA__CORE__TERMINATE_IF_IDLE="false"
MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED="false"
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
export MWAA__CORE__TASK_MONITORING_ENABLED
export MWAA__CORE__TERMINATE_IF_IDLE
export MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED

$CONTAINER_RUNTIME compose up 
