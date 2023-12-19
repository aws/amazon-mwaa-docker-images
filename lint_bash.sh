#!/bin/bash

# Lint all Bash files
echo "Running ShellCheck on Bash scripts..."
if ! find . -type f -name "*.sh" -exec shellcheck {} +; then
    echo "ShellCheck linting failed."
    exit 1
else
    echo "ShellCheck linting passed."
fi
