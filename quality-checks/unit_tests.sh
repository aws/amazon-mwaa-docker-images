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

# Run pytest directly on the tests directory
echo "Running pytest..."
if ! pytest tests/ ; then
    status=1
fi

exit $status
