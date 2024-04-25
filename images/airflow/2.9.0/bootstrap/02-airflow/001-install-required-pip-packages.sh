#!/bin/bash
set -e

# shellcheck source=images/airflow/2.9.0/bootstrap/common.sh
source /bootstrap/common.sh

# safe-pip-install always install all required packages, along with whatever
# the user provides, hence we don't need to provide anything here.
safe-pip-install
