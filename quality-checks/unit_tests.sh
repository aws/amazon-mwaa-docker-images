#!/bin/bash
set -e  # Exit immediately on error

RESULTS_DIR="./quality-checks/results"
mkdir -p "$RESULTS_DIR"
COVERAGE_THRESHOLD=80
FAILED_COVERAGE=0
COVERAGE_SUMMARY="${RESULTS_DIR}/coverage_summary.log"
VENV_DIR="./.venv"


# Ensure summary file is cleared at the start
true > "$COVERAGE_SUMMARY"

check_dir() {
    local dir=$1  # Directory containing Airflow version (e.g., ./images/airflow/2.10.1)
    local version_name
    version_name=$(basename "$dir")  # Extract Airflow version separately
    local test_dir="tests/images/airflow/${version_name}"
    local coverage_file="${RESULTS_DIR}/coverage-${version_name}.xml"

    echo "Checking directory \"${dir}\"..."

    # Ensure the script is being executed from the repo root.
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(dirname "$SCRIPT_DIR")"
    if [[ "$PWD" != "$REPO_ROOT" ]]; then
        SCRIPT_NAME=$(basename "$0")
        echo "The script must be run from the repo root. Please cd into the repo root directory and type: ./quality-checks/${SCRIPT_NAME}"
        exit 1
    fi

    # Check if virtualenv exists, if not, exit with an error message
    if [[ ! -d "$VENV_DIR" ]]; then
        echo "Virtual environment doesn't exist at ${VENV_DIR}. Please run the script ./create_venvs.py."
        exit 1
    fi

    # Check if test directory exists
    if [[ ! -d "$test_dir" ]]; then
        echo "No test directory found for ${version_name}, skipping..."
        return
    fi

    # Activate the virtual environment
    # shellcheck source=/dev/null
    source "${VENV_DIR}/bin/activate"

    # Run pytest with coverage for this Airflow version and store result in a coverage file
    echo "Running pytest with coverage for Airflow version: ${version_name}..."
    pytest --cov="$dir" --cov-report=xml:"$coverage_file" "$test_dir"

    # Deactivate the virtual environment
    deactivate
}

# Run checks for each Airflow version under ./images/airflow/
for image_dir in ./images/airflow/*; do
    if [[ -d "$image_dir" ]]; then
        check_dir "$image_dir"
    fi
done

# Activate the virtual environment
# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

# Now run diff-cover and generate a summary at the end
echo "================= COVERAGE SUMMARY =================" > "$COVERAGE_SUMMARY"
for coverage_file in "$RESULTS_DIR"/coverage-*.xml; do
    version_name=$(basename "$coverage_file" | sed 's/coverage-//; s/.xml//')
    echo "Checking coverage for new/changed code in ${version_name}..."

    if ! diff-cover "$coverage_file" --fail-under=$COVERAGE_THRESHOLD >> "$COVERAGE_SUMMARY" 2>&1; then
        echo "❌ ERROR: Coverage below ${COVERAGE_THRESHOLD}% for ${version_name}!" | tee -a "$COVERAGE_SUMMARY"
        FAILED_COVERAGE=1
    else
        echo "✅ Coverage meets requirement for ${version_name}." | tee -a "$COVERAGE_SUMMARY"
    fi
done
deactivate

# Print the final coverage summary
cat "$COVERAGE_SUMMARY"

# Delete results directory after printing the results
rm -rf "$RESULTS_DIR"

# Exit with failure if any coverage check failed
if [[ $FAILED_COVERAGE -ne 0 ]]; then
    exit 1
fi
