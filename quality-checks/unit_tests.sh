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

    # Check if virtualenv exists
    if [[ ! -d "$venv_dir" ]]; then
        echo "Virtual environment doesn't exist at ${venv_dir}. Please run the script ./create_venvs.py."
        exit 1
    fi

    # Suppress pip installation messages
    export PIP_QUIET=1

    # Find all version directories
    versions_path="tests/images/airflow"
    for version_dir in "$versions_path"/*/; do
        if [ -d "$version_dir" ]; then
            version=$(basename "$version_dir")
            version_venv="images/airflow/$version/.venv"
            
            echo -e "\nTesting Airflow version: $version"
            echo "----------------------------------------"

            # Check if version-specific venv exists
            if [[ ! -d "$version_venv" ]]; then
                echo "❌ Virtual environment doesn't exist at ${version_venv}. Please run ./create_venvs.py"
                status=1
                continue
            fi

            # Activate version-specific venv
            # shellcheck source=/dev/null
            source "${version_venv}/bin/activate" > /dev/null 2>&1

            # Run pytest with minimal output
            if ! pytest "tests/images/airflow/$version/" --quiet --no-header --tb=short -v; then
                echo "❌ Tests failed for version $version"
                status=1
            else
                echo "✅ All tests passed for version $version"
            fi

            # Deactivate after each version
            deactivate > /dev/null 2>&1
        fi
    done

    exit $status
}

# Suppress pip warnings
export PYTHONWARNINGS=ignore::DeprecationWarning

check_dir "."
