#!/bin/bash
set -e

echo "Generating Bill of Materials for the distribution the Docker Image..."

# Generate Bill of Materials for the system packages we are using.
echo "This Docker image includes the following third-party software/licensing:" > /tmp/System-Packages-BOM.txt
rpm -qa --queryformat "%{NAME}-%{VERSION}: %{LICENSE}\n" >> /tmp/System-Packages-BOM.txt

# Generate Bill of Materials for the Python packages we are using.
pip3 install pip-licenses 
echo "This Docker image includes the following third-party software/licensing:" > /tmp/Python-Packages-BOM.txt
sudo -u airflow pip-licenses --from=mixed --format=plain-vertical --with-url --with-license-file --with-notice-file | sudo tee -a /tmp/Python-Packages-BOM.txt
pip3 uninstall -y pip-licenses 

TARGET_DIR=/BillOfMaterials
mkdir ${TARGET_DIR}
mv /tmp/System-Packages-BOM.txt ${TARGET_DIR}
mv /tmp/Python-Packages-BOM.txt ${TARGET_DIR}
