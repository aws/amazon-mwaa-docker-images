#!/bin/bash
set -e

# Install sudo if needed
dnf install -y sudo

# Add airflow user to sudoers with no password requirement
echo 'airflow ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers.d/airflow
chmod 440 /etc/sudoers.d/airflow
