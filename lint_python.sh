#!/bin/bash

# Lint all Python files
echo "Running Flake8 on Python files..."
if ! flake8 .; then
    echo "Flake8 linting failed."
    exit 1
else
    echo "Flake8 linting passed."
fi
