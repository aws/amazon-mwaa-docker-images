#!/bin/bash
set -e

dnf install -y java-17-amazon-corretto                      # For Java lovers.
dnf install -y libcurl-devel                                # For pycurl
dnf install -y postgresql-devel                             # For psycopg2
dnf install -y procps                                       # For 'ps' command, which is used for monitoring.
dnf install -y jq                                           # For 'jq' command, which is used in execute-startup.sh