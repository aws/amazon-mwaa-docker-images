#!/bin/bash

# Ensure the script is being executed while being in the repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
if [[ "$PWD" != "$REPO_ROOT" ]]; then
    SCRIPT_NAME=$(basename "$0")
    echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
    exit 1
fi

# Lint all Python files
echo "Running Flake8 on Python files..."
if ! flake8 .; then
    echo "Flake8 linting failed."
    exit 1
else
    echo "Flake8 linting passed."
fi
