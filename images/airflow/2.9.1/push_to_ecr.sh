#!/bin/bash
set -e

# Build the Docker image
./build.sh

# Constants
AWS_REGION=us-west-2
REPO_NAME=amazon-mwaa-docker-images

# Create the ECR repository if it doesn't already exist.
if ! aws ecr describe-repositories --repository-names ${REPO_NAME} --region ${AWS_REGION} > /dev/null 2>&1 ; then
    echo "Repository does not exist. Creating repository..."
    aws ecr create-repository --repository-name ${REPO_NAME} --region ${AWS_REGION}
    echo "Repository created."
else
    echo "Repository ${REPO_NAME} already exists."
fi

# Construct the ECR URI.
account_id=$(aws sts get-caller-identity --query Account --output text)
ecr_uri="${account_id}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}"

# Authenticate Docker to the ECR registry
aws ecr get-login-password --region "${AWS_REGION}" | docker login --username AWS --password-stdin "${ecr_uri}"

for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        tag_name="amazon-mwaa-docker-images/airflow:2.9.1"
        target_tag_name="${ecr_uri}:airflow-2.9.1"

        if [[ "$build_type" != "standard" ]]; then
            tag_name="${tag_name}-${build_type}"
            target_tag_name="${target_tag_name}-${build_type}"
        fi

        if [[ "$dev" == "True" ]]; then
            tag_name="${tag_name}-dev"
            target_tag_name="${target_tag_name}-dev"
        fi

        docker image tag "${tag_name}" "${target_tag_name}"
        docker push "${target_tag_name}"
        echo "Docker image ${target_tag_name} has been pushed to ECR"
    done
done
