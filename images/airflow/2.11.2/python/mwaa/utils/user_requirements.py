"""
User requirements handling module for Amazon MWAA.

This module manages user-defined Python package requirements and their
installation process in Amazon MWAA environments. It handles requirement
file parsing, validation, and package installation procedures.
"""
# Python imports
from datetime import timedelta
import logging
import os
import re

# Our imports
from mwaa.logging.config import MWAA_LOGGERS
from mwaa.logging.loggers import CompositeLogger
from mwaa.subprocess.subprocess import Subprocess
from mwaa.subprocess.conditions import TimeoutCondition
from mwaa.utils.encoding import auto_decode

MWAA_DOCS_REQUIREMENTS_GUIDE = "https://docs.aws.amazon.com/mwaa/latest/userguide/working-dags-dependencies.html#working-dags-dependencies-test-create"

USER_REQUIREMENTS_MAX_INSTALL_TIME = timedelta(minutes=9)

logger = logging.getLogger("mwaa.entrypoint")


def _read_requirements_file(requirements_file: str) -> str:
    with open(requirements_file, "rb") as f:
        return auto_decode(f.read())


def _requirements_has_constraints(requirements_file: str):
    content = _read_requirements_file(requirements_file)
    for line in content.splitlines():
        # Notice that this regex check will also match lines with commented out
        # constraints flag. This is intentional as a mechanism for users who want to
        # avoid enforcing the default Airflow constraints, yet don't want to provide a
        # constraints file.
        if re.search(r"-c |--constraint ", line):
            return True
    return False


async def install_user_requirements(cmd: str, environ: dict[str, str]):
    """
    Install user requirements.

    User requirements should be placed in a requirements.txt file and the environment
    variable `MWAA__CORE__REQUIREMENTS_PATH` should be set to the location of that file.
    In a Docker Compose setup, you would usually want to create a volume that maps a
    requirements.txt file in the host machine somewhere in the container, and then set
    the `MWAA__CORE__REQUIREMENTS_PATH` accordingly.

    :param environ: A dictionary containing the environment variables.
    """
    requirements_file = environ.get("MWAA__CORE__REQUIREMENTS_PATH")
    logger.info(f"MWAA__CORE__REQUIREMENTS_PATH = {requirements_file}")
    if requirements_file and os.path.isfile(requirements_file):
        logger.info(f"Installing user requirements from {requirements_file}...")
        # For hybrid worker/scheduler containers we publish the requirement install logs
        # to the worker CloudWatch log group.
        logger_prefix = "worker" if cmd == "hybrid" else cmd;
        subprocess_logger = CompositeLogger(
            "requirements_composite_logging",  # name can be anything unused.
            # We use a set to avoid double logging to console if the user doesn't
            # use CloudWatch for logging.
            *set(
                [
                    logging.getLogger(MWAA_LOGGERS.get(f"{logger_prefix}_requirements")),
                    logger,
                ]
            ),
        )

        extra_args = []
        try:
            if not _requirements_has_constraints(requirements_file):
                subprocess_logger.warning(
                    "WARNING: Constraints should be specified for requirements.txt. "
                    f"Please see {MWAA_DOCS_REQUIREMENTS_GUIDE}"
                )
                subprocess_logger.warning("Forcing local constraints")
                extra_args = ["-c", os.environ["AIRFLOW_CONSTRAINTS_FILE"]]
        except Exception as e:
            subprocess_logger.warning(f"Unable to scan requirements file: {e}")
            subprocess_logger.warning(
                "Cannot determine whether the requirements.txt file has constraints "
                "or not; forcing local constraints."
            )
            extra_args = ["-c", os.environ["AIRFLOW_CONSTRAINTS_FILE"]]

        pip_process = Subprocess(
            cmd=["safe-pip-install", "-r", requirements_file, *extra_args],
            env=environ,
            process_logger=subprocess_logger,
            conditions=[
                TimeoutCondition(USER_REQUIREMENTS_MAX_INSTALL_TIME),
            ],
            friendly_name=f"{logger_prefix}_requirements",
        )
        pip_process.start()
        if pip_process.process and pip_process.process.returncode != 0:
            subprocess_logger.error(
                "ERROR: pip installation exited with a non-zero error code. This could "
                "be the result of package conflict. Notice that MWAA enforces a list "
                "of critical packages, e.g. Airflow, Celery, among others, whose "
                "version cannot be overridden by the customer as that can break our "
                "setup. Please double check your requirements.txt file."
            )
    else:
        logger.info("No user requirements to install.")
