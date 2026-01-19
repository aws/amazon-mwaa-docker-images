"""
This script is responsible for reserializing Airflow meta database objects.
"""

from argparse import Namespace
import logging.config
import os
import sys

from mwaa.utils.dblock import with_db_lock


# Hard-code the module path for logging since __name__ will be '__main__' when run as script
logger = logging.getLogger("mwaa.database.reserialize")


def _reserialize():
    """
    Reserializes Airflow database objects by calling the Airflow CLI reserialize command.
    Requires all Airflow environment variables to be properly configured.
    """
    from argparse import Namespace
    from airflow.cli.commands import dag_command as airflow_dag_command

    args = Namespace(bundle_name=None)
    logging.info("Reserializing Airflow dags")
    airflow_dag_command.dag_reserialize(args)

def _main():
    """Main entry point for the reserialize script."""
    _reserialize()


if __name__ == "__main__":
    _main()
else:
    logger.error(
        "This module cannot be imported. It should be run directly using: python -m mwaa.database.reseralize"
    )
    sys.exit(1)
