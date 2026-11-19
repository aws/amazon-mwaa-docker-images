#!/bin/bash
set -e

COMMAND=$1

# The test-command flags below set these. Ignore values inherited from the caller's shell.
unset MWAA_TEST_DAGS_PATH MWAA_TEST_PLUGINS_PATH MWAA_TEST_REQUIREMENTS_DIR \
    MWAA_TEST_REQUIREMENTS_FILE MWAA_TEST_STARTUP_SCRIPT_PATH MWAA_TEST_REPORT_PATH

# Exit code for invalid command-line usage (EX_USAGE in sysexits.h).
EX_USAGE=64

usage_error() {
    echo "ERROR: $1" >&2
    echo "Usage:" >&2
    echo "  ./run.sh test-dags [--dags <dir>] [--plugins <dir or plugins.zip>] [--requirements <file>] [--startup-script <file>] [--report <file>]" >&2
    echo "  ./run.sh test-requirements [--requirements <file>]" >&2
    echo "  ./run.sh test-startup-script [--startup-script <file>]" >&2
    exit "$EX_USAGE"
}

# Print the absolute path of the existing folder $1.
absolute_dir() {
    (cd "$1" && pwd)
}

# Print the absolute path of $1, whose parent folder must exist.
absolute_path() {
    echo "$(absolute_dir "$(dirname "$1")")/$(basename "$1")"
}

# Exit with a usage error if compose can't mount the path $1.
require_mountable() {
    [[ "$1" != *:* ]] || usage_error "Paths that contain ':' can't be mounted: $1"
}

# Exit with a usage error if anything, even a broken symlink, exists at the report path
# $1. run.sh creates the report itself, so it never empties or changes a file you have.
require_new_report() {
    if [ -e "$1" ] || [ -L "$1" ]; then
        usage_error "The report file already exists: $1. Pass a new path with --report, or delete the file."
    fi
}

# Print the path $1 with every symlink resolved. The last part of $1 may not exist yet.
physical_path() {
    local path=$1 link hops=0
    while [ -L "$path" ]; do
        hops=$((hops + 1))
        [ "$hops" -le 40 ] || return 1
        link=$(readlink "$path")
        case "$link" in
            /*) path=$link ;;
            *) path="$(dirname "$path")/$link" ;;
        esac
    done
    if [ -d "$path" ]; then
        (cd "$path" && pwd -P)
    else
        echo "$(cd "$(dirname "$path")" && pwd -P)/$(basename "$path")"
    fi
}

# Print the folders Finch shares with its virtual machine, as Finch documents them: your
# home folder and each additional_directories entry in ~/.finch/finch.yaml, with symlinks
# resolved. Entries that don't exist are left out.
finch_shared_dirs() {
    local config="$HOME/.finch/finch.yaml" line dir in_list=false
    (cd "$HOME" && pwd -P)
    if [ ! -f "$config" ]; then
        return 0
    fi
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%%#*}
        if [[ "$line" =~ ^additional_directories: ]]; then
            in_list=true
            continue
        fi
        if [[ "$line" =~ ^[^[:space:]] ]]; then
            in_list=false
        fi
        if $in_list && [[ "$line" =~ ^[[:space:]]*(-[[:space:]]+)?path:[[:space:]]*(.*[^[:space:]]) ]]; then
            dir=${BASH_REMATCH[2]}
            dir=${dir#[\"\']}
            dir=${dir%[\"\']}
            if [[ "$dir" == "~"* ]]; then
                dir="$HOME${dir#\~}"
            fi
            if [ -d "$dir" ]; then
                (cd "$dir" && pwd -P)
            fi
        fi
    done < "$config"
    return 0
}

is_test_command() {
    [ "$1" == "test-dags" ] || [ "$1" == "test-requirements" ] || [ "$1" == "test-startup-script" ]
}

# Input flags of the test commands. Each one sets a variable that
# docker-compose-test-commands.yaml maps to a read-only volume. Unset variables fall
# back to this folder's dags/, plugins/, requirements/ and startup/, as before.
if is_test_command "$COMMAND"; then
    shift
    while [ $# -gt 0 ]; do
        case "$1" in
            --*=*) usage_error "Put a space between the option and its value: ${1%%=*} <value>" ;;
        esac
        [ $# -ge 2 ] || usage_error "$1 needs a value."
        case "$COMMAND $1" in
            "test-dags --dags")
                [ -d "$2" ] || usage_error "--dags must be a folder: $2"
                MWAA_TEST_DAGS_PATH=$(absolute_dir "$2")
                require_mountable "$MWAA_TEST_DAGS_PATH"
                export MWAA_TEST_DAGS_PATH
                ;;
            "test-dags --plugins")
                if [ -d "$2" ] || { [ -f "$2" ] && [[ "$2" == *.zip ]]; }; then
                    MWAA_TEST_PLUGINS_PATH=$(absolute_path "$2")
                    require_mountable "$MWAA_TEST_PLUGINS_PATH"
                    export MWAA_TEST_PLUGINS_PATH
                else
                    usage_error "--plugins must be a folder or a .zip file: $2"
                fi
                ;;
            "test-dags --requirements" | "test-requirements --requirements")
                [ -f "$2" ] || usage_error "--requirements must be a file: $2"
                # Mount the file's folder, so -r and -c lines that name files next to it work.
                MWAA_TEST_REQUIREMENTS_DIR=$(absolute_dir "$(dirname "$2")")
                require_mountable "$MWAA_TEST_REQUIREMENTS_DIR"
                MWAA_TEST_REQUIREMENTS_FILE=$(basename "$2")
                export MWAA_TEST_REQUIREMENTS_DIR
                ;;
            "test-dags --startup-script" | "test-startup-script --startup-script")
                [ -f "$2" ] || usage_error "--startup-script must be a file: $2"
                MWAA_TEST_STARTUP_SCRIPT_PATH=$(absolute_path "$2")
                require_mountable "$MWAA_TEST_STARTUP_SCRIPT_PATH"
                export MWAA_TEST_STARTUP_SCRIPT_PATH
                ;;
            "test-dags --report")
                [ -d "$(dirname "$2")" ] || usage_error "--report folder does not exist: $(dirname "$2")"
                MWAA_TEST_REPORT_PATH=$(absolute_path "$2")
                ;;
            *)
                usage_error "Unknown option for $COMMAND: $1"
                ;;
        esac
        shift 2
    done
    if [ "$COMMAND" == "test-dags" ]; then
        # A new report name for each run, so earlier reports are kept.
        MWAA_TEST_REPORT_PATH="${MWAA_TEST_REPORT_PATH:-$PWD/validation-report-$(date +%Y%m%d-%H%M%S).json}"
        require_mountable "$MWAA_TEST_REPORT_PATH"
        require_new_report "$MWAA_TEST_REPORT_PATH"
    fi
fi

# Check if 'podman' or 'finch' is available, otherwise use 'docker'
if command -v finch &> /dev/null; then
    CONTAINER_RUNTIME="finch"
elif command -v podman &> /dev/null; then
    CONTAINER_RUNTIME="podman"
else
    CONTAINER_RUNTIME="docker"
fi

# Finch shares only some host folders with its virtual machine, and a path outside them
# is mounted as an empty folder, so test-dags would check nothing and still exit 0.
# Finch documents the shared folders as your home folder plus additional_directories in
# ~/.finch/finch.yaml. Paths are resolved first, and compose gets the resolved path: the
# virtual machine has its own /tmp, for example, while macOS's /tmp is /private/tmp.
if [ "$CONTAINER_RUNTIME" == "finch" ] && is_test_command "$COMMAND"; then
    finch_dirs=$(finch_shared_dirs)
    # Print the path $2, given with the flag $1, with symlinks resolved. Exit with a usage
    # error unless it's in a folder Finch shares.
    finch_shared_path() {
        local resolved dir
        resolved=$(physical_path "$2") || usage_error "Could not resolve the $1 path: $2"
        while IFS= read -r dir; do
            case "$resolved/" in
                "${dir%/}"/*) echo "$resolved"; return 0 ;;
            esac
        done <<< "$finch_dirs"
        usage_error "With Finch, the $1 path must be in your home folder or in a folder listed under additional_directories in ~/.finch/finch.yaml, because Finch shares only those folders with its virtual machine: $2"
    }
    # Replace the path in the variable named $2, given with the flag $1, with its resolved form.
    use_finch_shared_path() {
        local resolved
        [ -n "${!2:-}" ] || return 0
        resolved=$(finch_shared_path "$1" "${!2}") || exit "$EX_USAGE"
        printf -v "$2" '%s' "$resolved"
    }
    use_finch_shared_path --dags MWAA_TEST_DAGS_PATH
    use_finch_shared_path --plugins MWAA_TEST_PLUGINS_PATH
    if [ -n "${MWAA_TEST_REQUIREMENTS_DIR:-}" ]; then
        # Check the file as well as its folder, because the file can be a symlink.
        finch_shared_path --requirements "$MWAA_TEST_REQUIREMENTS_DIR/$MWAA_TEST_REQUIREMENTS_FILE" >/dev/null || exit "$EX_USAGE"
        use_finch_shared_path --requirements MWAA_TEST_REQUIREMENTS_DIR
    fi
    use_finch_shared_path --startup-script MWAA_TEST_STARTUP_SCRIPT_PATH
    use_finch_shared_path --report MWAA_TEST_REPORT_PATH
fi

# PLATFORM (linux/amd64 or linux/arm64) is read by build.sh and by
# docker-compose-test-commands.yaml. The full local stack runs on the host's platform.
if [ -n "${PLATFORM:-}" ] && [ "$COMMAND" != "test-dags" ] && [ "$COMMAND" != "test-requirements" ] && [ "$COMMAND" != "test-startup-script" ] && [ "$COMMAND" != "package-requirements" ]; then
    echo "ERROR: PLATFORM is only supported with test-dags, test-requirements, test-startup-script and package-requirements." >&2
    exit 1
fi

# Create .env now, readable only by you. write_compose_env_file fills it in right before compose
# runs. finch compose reads .env through the Finch VM's file share, which can miss a file created
# moments earlier, so the file has to exist well before compose starts.
(umask 077 && : >> .env)

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
export REGION

# CloudWatch logging mode: "auto" (on when ACCOUNT_ID+ENV_NAME are set), "true" (force on), "false" (force off).
USE_CLOUDWATCH_LOGS="auto"

# AWS Credentials
# For local running without a real AWS account, dummy values are sufficient —
# ElasticMQ (local SQS) does not validate credentials, but boto3 requires
# non-empty values to sign requests. Replace with real credentials if you
# want CloudWatch logging or other real AWS service access.
AWS_ACCESS_KEY_ID="local" # Put your credentials here, or leave as "local" for offline use.
AWS_SECRET_ACCESS_KEY="local" # Put your credentials here, or leave as "local" for offline use.
AWS_SESSION_TOKEN="" # Put your credentials here (leave empty for offline use).
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

# Set to http://host_name:8080
MWAA__CORE__API_SERVER_URL="http://mwaa-331-webserver:8080"

# BOM Generation
GENERATE_BILL_OF_MATERIALS="False"
export GENERATE_BILL_OF_MATERIALS

# Local Runner Indicator
MWAA_LOCAL_RUNNER="true"
export MWAA_LOCAL_RUNNER

# MWAA Configuration
MWAA__CORE__REQUIREMENTS_PATH="/usr/local/airflow/requirements/${MWAA_TEST_REQUIREMENTS_FILE:-requirements.txt}"
MWAA__CORE__STARTUP_SCRIPT_PATH="/usr/local/airflow/startup/startup.sh"
# Logging values derived from USE_CLOUDWATCH_LOGS.
case "$USE_CLOUDWATCH_LOGS" in
    auto)
        if [ -n "$ACCOUNT_ID" ] && [ -n "$ENV_NAME" ]; then
            LOGS_ENABLED="true"
        else
            LOGS_ENABLED="false"
        fi
        ;;
    true)
        if [ -z "$ACCOUNT_ID" ] || [ -z "$ENV_NAME" ]; then
            echo "ERROR: USE_CLOUDWATCH_LOGS=\"true\" requires ACCOUNT_ID and ENV_NAME (or use \"auto\"/\"false\")." >&2
            exit 1
        fi
        LOGS_ENABLED="true"
        ;;
    false)
        LOGS_ENABLED="false"
        ;;
    *)
        echo "ERROR: USE_CLOUDWATCH_LOGS must be auto, true, or false (got \"$USE_CLOUDWATCH_LOGS\")." >&2
        exit 1
        ;;
esac

if [ "$LOGS_ENABLED" == "true" ]; then
    arn_prefix="arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${ENV_NAME}"
    DAGPROCESSOR_LOG_GROUP_ARN="${arn_prefix}-DAGProcessing"
    SCHEDULER_LOG_GROUP_ARN="${arn_prefix}-Scheduler"
    TASK_LOG_GROUP_ARN="${arn_prefix}-Task"
    TRIGGERER_LOG_GROUP_ARN="${arn_prefix}-Scheduler"  # Triggerer reuses the Scheduler log group
    WEBSERVER_LOG_GROUP_ARN="${arn_prefix}-WebServer"
    WORKER_LOG_GROUP_ARN="${arn_prefix}-Worker"
else
    DAGPROCESSOR_LOG_GROUP_ARN=""
    SCHEDULER_LOG_GROUP_ARN=""
    TASK_LOG_GROUP_ARN=""
    TRIGGERER_LOG_GROUP_ARN=""
    WEBSERVER_LOG_GROUP_ARN=""
    WORKER_LOG_GROUP_ARN=""
fi

MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN="$DAGPROCESSOR_LOG_GROUP_ARN"
MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN="$SCHEDULER_LOG_GROUP_ARN"
MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN="$TASK_LOG_GROUP_ARN"
MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN="$TRIGGERER_LOG_GROUP_ARN"
MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN="$WEBSERVER_LOG_GROUP_ARN"
MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL="INFO"
MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED="$LOGS_ENABLED"
MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN="$WORKER_LOG_GROUP_ARN"
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

# Keep a written report readable only by you, or remove the file if the run wrote nothing.
finish_report() {
    if [ -s "$MWAA_TEST_REPORT_PATH" ]; then
        chmod 600 "$MWAA_TEST_REPORT_PATH" 2>/dev/null || true
    else
        rm -f "$MWAA_TEST_REPORT_PATH"
    fi
}

# test-dags writes its report through a bind mount. Set this before .env is written,
# so every runtime mounts the same file.
if [ "$COMMAND" == "test-dags" ]; then
    export MWAA_TEST_REPORT_PATH
    # The container's airflow user writes the report in place. Create a new file first,
    # so the bind mount is a file and not a folder. With set -C the shell refuses to
    # create it if anything exists at that path by now.
    if ! (set -C && : > "$MWAA_TEST_REPORT_PATH") 2>/dev/null; then
        usage_error "Could not create the report file: $MWAA_TEST_REPORT_PATH"
    fi
    # Let the container write it for this run only.
    chmod 666 "$MWAA_TEST_REPORT_PATH"
    trap finish_report EXIT
fi

# Write the values the compose files read to .env. finch compose runs inside the Finch VM and
# only receives the AWS credentials from this shell, but it reads .env from this directory.
# Docker and Podman read .env too, and the values exported in this shell take precedence, so
# they see the same values as before. .env is gitignored, holds credentials and the Fernet key,
# and is rewritten on every run.
write_compose_env_file() {
    local env_file=".env"
    local name
    for name in "$@"; do
        if [[ "${!name:-}" == *"'"* || "${!name:-}" == *$'\n'* ]]; then
            echo "ERROR: $name contains a single quote or a newline and can't be written to $env_file." >&2
            exit 1
        fi
    done
    # Rewrite the file in place instead of deleting it, so the Finch VM keeps seeing the same file.
    # Set the mode before writing, because umask only applies when a file is created.
    (umask 077 && : >> "$env_file")
    chmod 600 "$env_file"
    {
        echo "# Generated by run.sh on every run. Edit the values in run.sh instead."
        for name in "$@"; do
            printf "%s='%s'\n" "$name" "${!name:-}"
        done
    } > "$env_file"
}

write_compose_env_file \
    AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY \
    AWS_SESSION_TOKEN \
    FERNET_KEY \
    MWAA_TEST_DAGS_PATH \
    MWAA_TEST_PLUGINS_PATH \
    MWAA_TEST_REPORT_PATH \
    MWAA_TEST_REQUIREMENTS_DIR \
    MWAA_TEST_STARTUP_SCRIPT_PATH \
    MWAA__CORE__API_SERVER_URL \
    MWAA__CORE__KMS_KEY_ARN \
    MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED \
    MWAA__CORE__REQUIREMENTS_PATH \
    MWAA__CORE__STARTUP_SCRIPT_PATH \
    MWAA__CORE__TASK_MONITORING_ENABLED \
    MWAA__CORE__TERMINATE_IF_IDLE \
    MWAA__HEALTH_MONITORING_ENABLE_REVAMPED_HEALTHCHECK \
    MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL \
    MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL \
    MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL \
    MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL \
    MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL \
    MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED \
    MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN \
    MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL \
    PLATFORM \
    REGION

if is_test_command "$COMMAND"; then
    COMPOSE=("$CONTAINER_RUNTIME" compose -f docker-compose-test-commands.yaml)
    # --force-recreate starts a fresh container on every run, so packages and plugins
    # installed by an earlier run can't change the result.
    status=0
    if [ "$CONTAINER_RUNTIME" == "finch" ]; then
        # finch compose has no --exit-code-from, so read the exit code from the container.
        # Remove the previous run's container first, so its exit code can't be read by mistake.
        "${COMPOSE[@]}" rm -f -s "$COMMAND" >/dev/null 2>&1 || true
        "${COMPOSE[@]}" up "$COMMAND" --abort-on-container-exit --force-recreate || true
        container_id=$("${COMPOSE[@]}" ps -a -q "$COMMAND") || container_id=""
        # More than one container means we can't tell which one just ran, so fail instead of guessing.
        if [[ "$container_id" == *$'\n'* ]]; then
            echo "ERROR: found more than one $COMMAND container, so its exit code is unknown." >&2
            container_id=""
        fi
        status=$("$CONTAINER_RUNTIME" inspect --format '{{.State.ExitCode}}' "$container_id" 2>/dev/null) || status=1
        [[ "$status" =~ ^[0-9]+$ ]] || status=1
    else
        "${COMPOSE[@]}" up "$COMMAND" --exit-code-from "$COMMAND" --force-recreate || status=$?
    fi
    exit "$status"
elif [ "$COMMAND" == "package-requirements" ]; then
    $CONTAINER_RUNTIME compose -f docker-compose-test-commands.yaml up "$COMMAND" --abort-on-container-exit
else
    $CONTAINER_RUNTIME compose up
fi