#!/bin/bash

# Ensure the script is being executed while being in the repo root.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
if [[ "$PWD" != "$REPO_ROOT" ]]; then
    SCRIPT_NAME=$(basename "$0")
    echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
    exit 1
fi

# Lint all Bash files, excluding .venv directory
echo "Running ShellCheck on Bash scripts..."
if ! find . -type f -name "*.sh" -not -path "./.venv/*" -exec shellcheck {} +; then
    echo "ShellCheck linting failed."
    exit 1
else
    echo "ShellCheck linting passed."
fi
