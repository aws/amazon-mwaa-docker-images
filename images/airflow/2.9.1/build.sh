#!/bin/bash
set -e

# Generate the Dockerfiles from the templates.
# shellcheck source=/dev/null
source "../../../.venv/bin/activate"
python3 ../generate-dockerfiles.py
deactivate

# Build the base image.
docker build -f ./Dockerfiles/Dockerfile.base -t amazon-mwaa-docker-images/airflow:2.9.1-base ./

# Build the derivatives.
for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        dockerfile_name="Dockerfile"
        tag_name="amazon-mwaa-docker-images/airflow:2.9.1"

        if [[ "$build_type" != "standard" ]]; then
            dockerfile_name="${dockerfile_name}-${build_type}"
            tag_name="${tag_name}-${build_type}"
        fi

        if [[ "$dev" == "True" ]]; then
            dockerfile_name="${dockerfile_name}-dev"
            tag_name="${tag_name}-dev"
        fi

        docker build -f "./Dockerfiles/${dockerfile_name}" -t "${tag_name}" ./
    done
done
