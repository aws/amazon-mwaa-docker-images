#!/bin/bash
set -e

COMMAND=$1

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
REGION="us-west-2" # Keeping the region us-west-2 as default.

# AWS Credentials
AWS_ACCESS_KEY_ID="" # Put your credentials here.
AWS_SECRET_ACCESS_KEY="" # Put your credentials here.
AWS_SESSION_TOKEN="" # Put your credentials here.
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

# Set to http://host_name:8080
MWAA__CORE__API_SERVER_URL=""

# BOM Generation
GENERATE_BILL_OF_MATERIALS="False"
export GENERATE_BILL_OF_MATERIALS

# MWAA Configuration
MWAA__CORE__REQUIREMENTS_PATH="/usr/local/airflow/requirements/requirements.txt"
MWAA__CORE__STARTUP_SCRIPT_PATH="/usr/local/airflow/startup/startup.sh"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-DAGProcessing"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-Scheduler"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-Task"
MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-Scheduler"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-WebServer"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED="true"
MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}-Worker"
MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL="INFO"
MWAA__CORE__TASK_MONITORING_ENABLED="false"
MWAA__CORE__TERMINATE_IF_IDLE="false"
MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED="false"
export MWAA__CORE__REQUIREMENTS_PATH
export MWAA__CORE__STARTUP_SCRIPT_PATH
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
export MWAA__CORE__API_SERVER_URL

# Function to create CloudWatch log group if it doesn't exist
create_log_group_if_not_exists() {
    local component=$1
    local log_enabled=$2

    if [ -z "$ENV_NAME" ]; then
        echo "Not creating log group for $component as ENV_NAME is not set."
        return
    fi

    if [ "$log_enabled" != "true" ]; then
        echo "Skipping log group creation as logging is not enabled for $component."
        return
    fi

    local log_group_name="${ENV_NAME}-${component}"
    echo "Verifying existence of log group: '$log_group_name' in region '$REGION'..."

    if aws logs describe-log-groups --log-group-name-prefix "$log_group_name" --region "$REGION" | grep -q "$log_group_name"; then
        echo "Log group '$log_group_name' already exists in region '$REGION'."
    else
        echo "Creating log group '$log_group_name' in region '$REGION'..."
        if aws logs create-log-group --log-group-name "$log_group_name" --region "$REGION"; then
            echo "Log group '$log_group_name' created successfully in region '$REGION'."
        else
            echo "Error creating log group '$log_group_name' in region '$REGION'."
        fi
    fi
}

# Create log groups for each component
create_log_group_if_not_exists "DAGProcessing" "$MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED"
create_log_group_if_not_exists "Scheduler" "$MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED"
create_log_group_if_not_exists "Task" "$MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED"
create_log_group_if_not_exists "WebServer" "$MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED"
create_log_group_if_not_exists "Worker" "$MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED"

if [ "$COMMAND" == "test-requirements" ] || [ "$COMMAND" == "test-startup-script" ]; then
    $CONTAINER_RUNTIME compose -f docker-compose-test-commands.yaml up "$COMMAND" --abort-on-container-exit
else
    $CONTAINER_RUNTIME compose up
fi