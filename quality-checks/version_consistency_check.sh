#!/bin/bash
# Verifies that EXPECTED_AIRFLOW_VERSION in each image's
# post-startup-script-verification matches AIRFLOW_VERSION in its Dockerfile.base.

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
images_dir="images/airflow"

for version_dir in "$images_dir"/*/; do
    [ -d "$version_dir" ] || continue
    version=$(basename "$version_dir")

    dockerfile="$version_dir/Dockerfiles/Dockerfile.base"
    verification="$version_dir/bin/airflow-user/post-startup-script-verification"

    # Skip if either file doesn't exist
    if [ ! -f "$dockerfile" ] || [ ! -f "$verification" ]; then
        continue
    fi

    dockerfile_version=$(grep -oP 'ENV AIRFLOW_VERSION=\K.*' "$dockerfile")
    expected_version=$(grep -oP 'EXPECTED_AIRFLOW_VERSION="\K[^"]+' "$verification")

    if [[ "$dockerfile_version" != "$expected_version" ]]; then
        echo "FAIL: Version mismatch in $version: Dockerfile.base has $dockerfile_version, post-startup-script-verification has $expected_version"
        status=1
    else
        echo "OK: $version: versions match ($dockerfile_version)"
    fi
done

if [[ $status -eq 0 ]]; then
    echo "All version consistency checks passed."
else
    echo "Version consistency check failed."
fi

exit $status
