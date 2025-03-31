#!/bin/bash
set -e

check_dir() {
    local dir=$1  # Directory to work in
    local venv_dir="${dir}/.venv"  # virtual environment path

    echo "Checking directory \"${dir}\"..."
    # Ensure the script is being executed while being in the repo root.
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(dirname "$SCRIPT_DIR")"
    if [[ "$PWD" != "$REPO_ROOT" ]]; then
        SCRIPT_NAME=$(basename "$0")
        echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
        exit 1
    fi

    status=0

    # Check if virtualenv exists, if not create it and install dependencies
        if [[ ! -d "$venv_dir" ]]; then
            echo "Virtual environment doesn't exist at ${venv_dir}. Please run the script ./create_venvs.py."
            exit 1
        fi

        # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"

    # Run pytest directly on the tests directory
    echo "Running pytest..."
    if ! pytest tests/ ; then
        status=1
    fi

    deactivate

    exit $status
}
check_dir "."
