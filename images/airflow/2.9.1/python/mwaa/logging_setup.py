"""
This module contains code for configuring the Python logging system.

A default root logger is configured by this module, with a console handler, and a level
set according to an environment variable.
"""

import logging.config
import os


def setup_logging():
    """
    Configure the Python logging system based on environment variables.

    This function configures the logging system based on the value of the environment
    variable `MWAA__CORE__LOG_LEVEL`. If the variable is not set or contains an invalid
    log level, the logging level defaults to 'INFO'. The logging level determines which
    messages are logged.

    Valid log levels are: DEBUG, INFO, WARNING, ERROR, CRITICAL.

    Note:
        If the specified log level is invalid, a warning message will be printed,
        and the logging level will default to 'WARNING'.
    """
    log_level = os.environ.get("MWAA__CORE__LOG_LEVEL", "WARNING").upper()
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        default_log_level = "INFO"
        print(f"Invalid log level: '{log_level}'. Defaulting to {default_log_level}.")
        log_level = default_log_level

    log_format = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"

    logging_config = {
        "version": 1,
        "formatters": {"standard": {"format": log_format}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": log_level,
            }
        },
        "root": {"handlers": ["console"], "level": log_level},
    }

    logging.config.dictConfig(logging_config)
