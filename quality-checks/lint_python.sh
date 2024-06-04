#!/bin/bash
set -e

# Ensure the script is being executed while being in the repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
if [[ "$PWD" != "$REPO_ROOT" ]]; then
    SCRIPT_NAME=$(basename "$0")
    echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
    exit 1
fi

status=0

check_dir() {
    local dir=$1  # Directory to work in
    local venv_dir="${dir}/.venv"  # virtual environment path

    echo "Checking directory \"${dir}\"..."

    # Check if virtualenv exists, if not create it and install dependencies
    if [[ ! -d "$venv_dir" ]]; then
        echo "Virtual environment doesn't exist at ${venv_dir}. Please run the script ./create_venvs.py."
        exit 1
    fi

    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"
    # Run ruff and Pyright
    echo "Running ruff..."
    ruff check "${dir}" || status=1
    echo "Running Pyright..."
    pyright "${dir}" || status=1
    deactivate

    echo
}

# Main repo setup and checks
check_dir "."

# Setup and checks for each Docker image under ./images/airflow
for image_dir in ./images/airflow/*; do
    if [[ -d "$image_dir" ]]; then
        check_dir "$image_dir"
    fi
done

exit $status