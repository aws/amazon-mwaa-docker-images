# Python imports
import copy

# 3rd party imports
from airflow.providers.celery.executors.default_celery import (
    DEFAULT_CELERY_CONFIG
)

# Our import
from mwaa.config.aws import get_aws_region
from mwaa.config.sqs import get_sqs_queue_name, get_sqs_queue_url


def create_celery_config():
    """
    Generate the configuration that will be passed to Celery. This is used in
    the "celery" section of the Airflow configuration.
    """

    # We use Airflow's default condfiguration and make the changes we want.
    celery_config = copy.deepcopy(DEFAULT_CELERY_CONFIG)
    celery_config = {
        **celery_config,
        "broker_transport_options": {
            **celery_config["broker_transport_options"],
            "predefined_queues": {
                get_sqs_queue_name(): {
                    "url": get_sqs_queue_url()
                }
            },
            "is_secure": True,
            "region": get_aws_region(),
        },
    }

    return celery_config


MWAA_CELERY_CONFIG = create_celery_config()
