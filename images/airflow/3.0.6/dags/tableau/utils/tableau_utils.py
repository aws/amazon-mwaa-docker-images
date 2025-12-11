"""
Shared utilities for Tableau DAGs.

Contains common functions and configurations used across multiple Tableau-related DAGs.
"""

import json
import logging
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import Variable
from pendulum import DateTime, duration

from above.common.constants import ENVIRONMENT_FLAG
from above.common.slack_alert import task_failure_slack_alert

logger: logging.Logger = logging.getLogger(__name__)


def get_tableau_credentials() -> Dict[str, str]:
    """
    Retrieve Tableau credentials from Airflow Variables.

    :return: Dictionary containing TOKEN_NAME and TOKEN_SECRET
    :raises AirflowException: If credentials are missing or invalid
    """
    try:
        tableau_env: Dict[str, str] = json.loads(Variable.get("tableau"))

        if not tableau_env.get("TOKEN_NAME") or not tableau_env.get("TOKEN_SECRET"):
            raise AirflowException("Tableau credentials incomplete")
        logger.info(tableau_env)
        return tableau_env
    except Exception as e:
        logger.error(f"Failed to retrieve Tableau credentials: {e}")
        raise AirflowException(f"Failed to retrieve Tableau credentials: {e}")


def get_tableau_dag_default_args(start_date: DateTime) -> dict[str, Any]:
    """
    Get standard default arguments for Tableau DAGs.

    :param start_date: The start date for the DAG
    :return: Dictionary of default arguments
    """
    return dict(
        owner="Data Engineering",
        start_date=start_date,
        depends_on_past=False,
        retries=0,  # Manually retry only after manual re-rerun
        retry_delay=duration(minutes=10),
        execution_timeout=duration(minutes=120),
        on_failure_callback=task_failure_slack_alert
        if ENVIRONMENT_FLAG == "prod"
        else None,
    )