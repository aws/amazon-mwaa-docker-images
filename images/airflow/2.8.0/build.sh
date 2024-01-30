#!/bin/bash
set -e

python3 generate-dockerfile.py

#!/bin/bash

for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        dockerfile_name="Dockerfile"
        tag_name="amazon-mwaa/airflow:2.8.0"

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