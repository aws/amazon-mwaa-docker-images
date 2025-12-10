#!/bin/bash
set -e

echo "Generating Bill of Materials for the distribution the Docker Image..."

# Generate Bill of Materials for the system packages we are using.
echo "This Docker image includes the following third-party software/licensing:" > /tmp/System-Packages-BOM.txt
rpm -qa --queryformat "%{NAME}-%{VERSION}: %{LICENSE}\n" >> /tmp/System-Packages-BOM.txt

# Install pip-licenses
pip3 install --root-user-action=ignore pip-licenses

# Python package BOM
echo "This Docker image includes the following third-party software/licensing:" > /tmp/Python-Packages-BOM.txt

# Run pip-licenses as airflow user
runuser -u airflow -- bash -c "
pip-licenses \
  --from=mixed \
  --format=plain-vertical \
  --with-url \
  --with-license-file \
  --with-notice-file
" >> /tmp/Python-Packages-BOM.txt

# Uninstall pip-licenses
pip3 uninstall -y pip-licenses

# Move results
TARGET_DIR=/BillOfMaterials
mkdir -p ${TARGET_DIR}
mv /tmp/System-Packages-BOM.txt ${TARGET_DIR}
mv /tmp/Python-Packages-BOM.txt ${TARGET_DIR}
