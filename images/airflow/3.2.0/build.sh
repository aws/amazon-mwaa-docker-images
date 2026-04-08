#!/bin/bash
set -e

CONTAINER_RUNTIME=$1
echo "Using $CONTAINER_RUNTIME runtime in build.sh"

# Check if GENERATE_BILL_OF_MATERIALS flag is set (default is False)
GENERATE_BILL_OF_MATERIALS=${GENERATE_BILL_OF_MATERIALS:-"False"}
echo "GENERATE_BILL_OF_MATERIALS is set to $GENERATE_BILL_OF_MATERIALS"

# Generate the Dockerfiles from the templates.
# shellcheck source=/dev/null
source "../../../.venv/bin/activate"
python3 ../generate-dockerfiles.py
deactivate

# Only prepare BOM directories if we're generating BOMs
if [[ "$GENERATE_BILL_OF_MATERIALS" == "True" ]]; then
    BOM_DOCKER_PATH="/BillOfMaterials"
    BOM_LOCAL_PATH="./BillOfMaterials"
    # Prepare the local directory for the Bill of Materials.
    rm -rf ${BOM_LOCAL_PATH} && mkdir ${BOM_LOCAL_PATH}
fi

# Build the base image.
${CONTAINER_RUNTIME} build -f ./Dockerfiles/Dockerfile.base -t amazon-mwaa-docker-images/airflow:3.1.6-base ./


# Build the derivatives.
for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        dockerfile_name="Dockerfile"
        tag_name="3.1.6"

        if [[ "$build_type" != "standard" ]]; then
            dockerfile_name="${dockerfile_name}-${build_type}"
            tag_name="${tag_name}-${build_type}"
        fi

        if [[ "$dev" == "True" ]]; then
            dockerfile_name="${dockerfile_name}-dev"
            tag_name="${tag_name}-dev"
        fi

        IMAGE_NAME="amazon-mwaa-docker-images/airflow:${tag_name}"
        ${CONTAINER_RUNTIME} build -f "./Dockerfiles/${dockerfile_name}" -t "${IMAGE_NAME}" ./

        # Now we copy the Bill of Materials from the Docker image into this
        # repository so it can be checked into source control for easy visibility.

        if [[ "$GENERATE_BILL_OF_MATERIALS" == "True" ]]; then
            # Create a temporary container in stop state.
            ${CONTAINER_RUNTIME} container rm bom_temp_container > /dev/null 2>&1 || true
            ${CONTAINER_RUNTIME} container create --name bom_temp_container $IMAGE_NAME

            # Copy the directory from the container to the local machine and rename
            # it to distinguish each image.
            ${CONTAINER_RUNTIME} cp bom_temp_container:"${BOM_DOCKER_PATH}" "${BOM_LOCAL_PATH}"
            mv "${BOM_LOCAL_PATH}/BillOfMaterials" "${BOM_LOCAL_PATH}/${tag_name}"

            # Remove the temporary container
            ${CONTAINER_RUNTIME} rm bom_temp_container
        fi
    done
done
