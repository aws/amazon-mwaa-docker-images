#!/bin/bash
set -e

dnf install -y shadow-utils

# AIRFLOW_USER_HOME is defined in the Dockerfile.
adduser -s /bin/bash -d "${AIRFLOW_USER_HOME}" airflow

dnf remove -y shadow-utils
