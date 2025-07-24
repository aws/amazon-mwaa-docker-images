"""
Utility functions related to StatsD.
"""

# Python imports
from functools import cache

# 3rd-party imports
from statsd import StatsClient  # type: ignore
from airflow.metrics.statsd_logger import SafeStatsdLogger


@cache
def get_statsd():
    """
    Retrieves a StatsD client for publishing metrics.

    Important Note: Typically, one would want to use the `Stats` object from the
    `airflow.stats` module. However, this requires the necessary Airflow configuration
    to have been initialized, which is not always the case. One example is the metrics
    published by the code in our entrypoint.py. The reason is that we define the
    necessary Airflow configuration via environment variables and then pass them to
    child processes, e.g. scheduler, worker, etc., but the parent process itself (the
    entrypoint.py) doesn't have those enviromnent variables.

    This is more of a temporary workaround, since ideally we should patch the
    environment variables of the parent process as well. This, however, is a bit
    challenging since the process of patching the enviromnent variables should happen
    before any Airflow module is imported, which is a bit tricky considering that
    even pretty much all modules in our code ends up importing Airflow, directly or
    indirectly, hence some substantial refactoring is required. This, however, isn't
    impossible, and hence a GitHub issue is created to track it:

    https://github.com/aws/amazon-mwaa-docker-images/issues/99
    """
    statsd_client = StatsClient(
        host="localhost",
        port=8125,
        prefix="airflow",
    )

    return SafeStatsdLogger(statsd_client)
