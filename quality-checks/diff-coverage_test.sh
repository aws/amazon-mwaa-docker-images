#!/bin/bash

# This script tests coverage of modified/added python code only, until we add more unit tests
# Test fails if coverage of the new code is below 80%
# TODO : In the future, replace with a full fledged coverage test script

set -e

# Color codes and unicode symbols for formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
CHECK_MARK="\xE2\x9C\x94"
CROSS_MARK="\xE2\x9C\x98"

# Create temporary directory
TEMP_DIR=$(mktemp -d)

# Helper functions for formatting
print_header() {
    echo -e "\n═══════════════════════════════════════════"
    echo -e "$1"
    echo -e "═══════════════════════════════════════════"
}

print_section() {
    echo -e "\n$1"
    echo "───────────────────────────────────────────"
}

print_success() {
    echo -e "${GREEN}${CHECK_MARK} $1${NC}"
}

print_error() {
    echo -e "${RED}${CROSS_MARK} $1${NC}"
}

format_coverage_output() {
    local coverage_text="$1"
    local output=""

    while IFS= read -r line; do
        if [[ $line == *"Diff Coverage"* ]] || [[ $line == "-------------"* ]] || [[ $line == "Diff:"* ]]; then
            # Headers and separators stay uncolored
            output+="${line}\n"
        elif [[ $line == *"Failure"* ]]; then
            # Failure message in red
            output+="${RED}${line}${NC}\n"
        elif [[ $line == *": Missing lines"* ]]; then
            # Files with missing coverage
            file_path=$(echo "$line" | cut -d' ' -f1)
            coverage=$(echo "$line" | grep -o '([0-9.]*%)')
            missing_lines=$(echo "$line" | grep -o ': Missing lines.*')
            coverage_num=$(echo "$coverage" | tr -d '()%')

            if (( $(echo "$coverage_num < 80" | bc -l) )); then
                output+="${file_path} ${RED}${coverage}${NC}${RED}${missing_lines}${NC}\n"
            else
                output+="${file_path} ${GREEN}${coverage}${NC}${missing_lines}\n"
            fi
        elif [[ $line == *"Total:"* ]] || [[ $line == *"Missing:"* ]]; then
            # Statistics lines
            label=$(echo "$line" | cut -d: -f1):
            value=$(echo "$line" | cut -d: -f2)
            output+="${label}${value}\n"
        elif [[ $line == *"Coverage:"* ]]; then
            # Final coverage percentage
            label="Coverage:"
            value=$(echo "$line" | cut -d: -f2 | tr -d ' ')
            coverage_num=$(echo "$value" | tr -d '%')
            
            if (( $(echo "$coverage_num < 80" | bc -l) )); then
                output+="${label} ${RED}${value}${NC}\n"
            else
                output+="${label} ${GREEN}${value}${NC}\n"
            fi
        else
            # Any other lines
            output+="${line}\n"
        fi
    done <<< "$coverage_text"

    echo -e "$output"
}

get_base_branch() {
    echo -e "${BLUE}Fetching main branch for comparison...${NC}" >&2
    if git fetch --deepen=50 https://github.com/aws/amazon-mwaa-docker-images.git main >/dev/null 2>&1; then
        echo "FETCH_HEAD"  # This points to the fetched main branch
    else
        echo -e "${RED}Error: Could not fetch main branch. Please check your internet connection.${NC}" >&2
        exit 1
    fi
}

# shellcheck disable=SC2317
# shellcheck disable=SC2329
cleanup() {
    # Remove temporary directory
    rm -rf "$TEMP_DIR"
}

trap cleanup EXIT

check_coverage() {
    local dir=$1
    local base_branch=$2
    local version
    version=$(basename "$dir")
    local venv_dir="${dir}/.venv"
    local coverage_file="${dir}/coverage.xml"
    local results_file="${TEMP_DIR}/${version}_results.txt"
    local failed=""

    # Check if virtualenv exists
    if [[ ! -d "$venv_dir" ]]; then
        echo "Virtual environment doesn't exist at ${venv_dir}. Please run the script ./create_venvs.py."
        exit 1
    fi

    # shellcheck source=/dev/null
    source "${venv_dir}/bin/activate"

    # Run pytest with coverage
    if ! pytest "tests/images/airflow/${version}" \
        --cov="${dir}" \
        --cov-report="xml:${coverage_file}" \
        --cov-report=term-missing > "${TEMP_DIR}/pytest.log" 2>&1; then
        print_error "pytest failed for ${dir}:"
        cat "${TEMP_DIR}/pytest.log"
        deactivate
        return 1
    fi

    # Run diff-cover and capture output
    if ! diff-cover "${coverage_file}" --compare-branch="$base_branch" --fail-under=80 > "${TEMP_DIR}/coverage.txt" 2>&1; then
        print_error "Coverage check failed for ${dir}"
        failed="true"
    else
        print_success "Coverage check passed for ${dir}"
    fi

    # Create formatted results
    {
        if [ -n "$failed" ]; then
            print_header "Results for ${dir} (${RED}FAILED${NC})"
        else
            print_header "Results for ${dir} (${GREEN}PASSED${NC})"
        fi
        echo
        coverage_text=$(cat "${TEMP_DIR}/coverage.txt")
        echo -e "$(format_coverage_output "$coverage_text")"
    } > "$results_file"

    deactivate

    [ -z "$failed" ]
    return $?
}

main() {
    local status=0

    print_header "Coverage Check On Added Code"

    # Check for untracked files
    local untracked_files
    untracked_files=$(git ls-files --others --exclude-standard | grep "^images/airflow/.*\.py$" || true)
    if [ -n "$untracked_files" ]; then
        echo -e "\n${YELLOW}Warning: Found untracked python files that won't be included in coverage check:${NC}"
        printf '%s\n' "$untracked_files"
        echo -e "${YELLOW}Run 'git add' on these files to include them in coverage checks.${NC}\n"
    fi

    # Get the base branch once
    local base_branch
    base_branch=$(get_base_branch)

    # Get list of modified directories
    local modified_dirs
    modified_dirs=$(git diff --name-only "$base_branch" | grep "^images/airflow/.*\.py$" | cut -d/ -f1-3 | sort -u || true)

    if [ -z "$modified_dirs" ]; then
        print_success "No python code modifications found in airflow image directories"
        exit 0
    fi

    print_section "Modified Image Directories"
    for dir in $modified_dirs; do
        echo "  - $dir"
    done

    # Check each modified image directory
    for dir in $modified_dirs; do
        # Only run for images with populated test directories
        # TODO: Append below line to `if` statement when test folder for 3.0.6 is complete \
        # || [ "$dir" == "images/airflow/3.0.6" ] \
        if [ "$dir" == "images/airflow/2.9.2" ] \
        || [ "$dir" == "images/airflow/2.10.1" ] \
        || [ "$dir" == "images/airflow/2.10.3" ] \
        || [ "$dir" == "images/airflow/2.11.0" ] \
        ; then
            if [ -d "$dir" ]; then
                echo -e "\n${BLUE}Checking ${dir}...${NC}"
                if ! check_coverage "$dir" "$base_branch"; then
                    status=1
                fi
            fi
        else
            echo "Skipping directory \"${dir}\" (tests not yet created)..."
        fi
    done

    # Show final results
    print_header "Detailed Coverage Report"
    for results in "${TEMP_DIR}"/*_results.txt; do
        if [ -f "$results" ]; then
            cat "$results"
        fi
    done

    if [ $status -ne 0 ]; then
        echo
        print_error "Some coverage checks failed. Please ensure all modified code has adequate test coverage."
    else
        echo
        print_success "All coverage checks passed!"
    fi

    exit $status
}

main
