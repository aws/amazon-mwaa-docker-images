"""Contain functions for retrieving Airflow Celery-related configuration."""

# Python imports
from typing import Any
import copy


# 3rd party imports
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

# Our import
from mwaa.config.aws import get_aws_region
from mwaa.config.database import MWAA_CONNECT_ARGS
from mwaa.config.sqs import get_sqs_queue_name, get_sqs_queue_url, should_use_ssl

from mwaa.celery.sqs_broker import Transport
from mwaa.utils import qualified_name


def get_broker_transport_config() -> dict[str, Any]:
    """
    Return the MWAA broker transport configuration for Celery.

    This is the broker settings shared by:
      - The worker-side Celery app (via celery_config_options / MWAA_CELERY_CONFIG)
      - The scheduler-side Celery app (via AIRFLOW__CELERY__EXTRA_CELERY_CONFIG)

    If you change anything here, both code paths pick it up automatically.

    :returns A dictionary with broker_transport and broker_transport_options keys.
    """
    sqs_queue_name = get_sqs_queue_name()
    sqs_queue_url = get_sqs_queue_url()
    return {
        "broker_transport": qualified_name(Transport),
        "broker_transport_options": {
            "predefined_queues": {
                sqs_queue_name: {"url": sqs_queue_url},
                "default": {"url": sqs_queue_url},
            },
            "is_secure": should_use_ssl(),
            "region": get_aws_region(),
            "visibility_timeout": 43200,
        },
    }


def create_celery_config() -> dict[str, Any]:
    """
    Generate the configuration that will be passed to Celery.

    :returns A dictionary containing the Celery configuration.
    """
    broker_config = get_broker_transport_config()
    # We use Airflow's default configuration and make the changes we want.
    celery_config: dict[str, Any] = copy.deepcopy(DEFAULT_CELERY_CONFIG)
    celery_config = {
        **celery_config,
        "broker_transport": broker_config["broker_transport"],
        "broker_transport_options": {
            **celery_config["broker_transport_options"],
            **broker_config["broker_transport_options"],
        },
        "database_engine_options": {
            "pool_pre_ping": True,
            "pool_recycle": 1200,
            "connect_args": MWAA_CONNECT_ARGS
        }
    }

    return celery_config


MWAA_CELERY_CONFIG = create_celery_config()
