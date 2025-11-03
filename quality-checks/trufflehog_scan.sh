#!/bin/bash
set -e

# Ensure the script is being executed from the repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
if [[ "$PWD" != "$REPO_ROOT" ]]; then
    SCRIPT_NAME=$(basename "$0")
    echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
    exit 1
fi

status=0

# Function to check a directory
check_dir() {
    local dir=$1  # Directory to work in
    local venv_dir="${dir}/.venv"  # Virtual environment path

    echo "Checking directory \"${dir}\"..."

    # Check if virtualenv exists, if not create it and install dependencies
    if [[ ! -d "$venv_dir" ]]; then
        echo "Virtual environment doesn't exist at ${venv_dir}. Please run the script ./create_venvs.py."
        exit 1
    fi

    # Activate the virtual environment
    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"

    # Run TruffleHog to scan for secrets
    echo "Running TruffleHog to scan for secrets..."
    if ! (trufflehog filesystem "${dir}"); then
        echo "TruffleHog detected potential secrets."
        status=1
    else
        echo "TruffleHog scan passed."
    fi

    # Clean up the temporary exclusion file
    rm -f "$EXCLUDE_FILE"

    # Deactivate the virtual environment
    deactivate

    echo
}

# Main repo setup and checks
check_dir "."

exit $status
