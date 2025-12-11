#!/bin/bash

export ENVIRONMENT_STAGE="local"
echo "Airflow Environment is:" $ENVIRONMENT_STAGE

if [ "$CONNECT_TO_RDS_PROXY" = "true" ]; then
  echo "Connecting to RDS proxy via SSH..."
  ssh -4 -i ${AIRFLOW_LOCAL_CONNECTIONS_DIR:-${PWD}}/files/keys/ssh_host.pem -fNT -o ServerAliveInterval=60 -o ServerAliveCountMax=10 -o ExitOnForwardFailure=yes -o StrictHostKeyChecking=no -L 5432:${RDS_HOST}:5432 ${SSH_ADDRESS}
  echo "SSH tunnel to RDS proxy established."
else
  echo "Skipping RDS proxy connection (CONNECT_TO_RDS_PROXY=${CONNECT_TO_RDS_PROXY:-false})."
fi

# --- ADD THIS SECTION TO IMPORT CONNECTIONS ---
CONNECTIONS_FILE="/usr/local/airflow/files/connections.json"
echo "Attempting to import connections from: ${CONNECTIONS_FILE}"
if [ -f "${CONNECTIONS_FILE}" ]; then
  # The 'airflow connections import' command reads the JSON and inserts/updates connections in the DB
  airflow connections import "${CONNECTIONS_FILE}"
  echo "Successfully imported connections."
else
  echo "Warning: Connections file not found at ${CONNECTIONS_FILE}. Skipping import."
fi

airflow dags pause --treat-dag