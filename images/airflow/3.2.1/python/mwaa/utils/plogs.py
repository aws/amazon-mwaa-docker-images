"""
A module containing utility functions for processable logs.

Processable logs are JSON-formatted, structured logs that contain useful information
about the behaviour of the containers. They can be used to automate certain actions
based on certain events. Their main use is within MWAA to monitor the health of
enviornmetns. They get ingested by Fargate (the infrastructure we deploy our enviroments
on) which allow us to
"""

import json
import os
import time


def generate_plog(
    logs_processor_name: str,
    log_message: str,
):
    """
    Generate a processable log.

    A processable log is structured log that carrys certain useful information about
    the behaviour of the container and is processed by the MWAA service.

    :param logs_processor_name: The name of the logs processor responsible we want to
      process this log. These names are defined by the MWAA service and is not public.
    :param log_message: The message we want to send.

    :returns The generated processable log.
    """
    airflow_version = os.environ.get("AIRFLOW_VERSION", "Unknown")
    customer_account_id = os.environ.get("CUSTOMER_ACCOUNT_ID", "Unknown")
    environment_name = os.environ.get("AIRFLOW_ENV_NAME", "Unknown")

    return json.dumps(
        {
            "signature": "mwaa_plog_v1",
            "logsProcessorName": logs_processor_name,
            "dataPlaneCell": "Unknown",
            "airflowVersion": airflow_version,
            "airflowImage": "Unknown",
            "customerAccountId": customer_account_id,
            "environmentName": environment_name,
            "timestamp": int(time.time()),
            "message": log_message,
        }
    )
