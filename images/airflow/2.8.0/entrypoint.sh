#!/bin/bash
set -e

# Check for the command and execute the corresponding Airflow component
case "$1" in
  webserver)
    exec python3 /entrypoint.py webserver
    ;;
  scheduler)
    exec python3 /entrypoint.py scheduler
    ;;
  worker)
    exec python3 /entrypoint.py worker
    ;;
  shell)
    exec /bin/bash
    ;;
  *)
    echo 'Error: Invalid command or no command is provided. Valid commands are: "webserver", "scheduler", "worker", or "shell".'
    exit 1
    ;;
esac
