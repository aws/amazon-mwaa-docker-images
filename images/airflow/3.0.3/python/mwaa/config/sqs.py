"""Contain functions for retrieving Airflow SQS-related configuration."""

# Python imports
import os
from urllib.parse import urlparse, urlunparse

# 3rd party imports
import boto3

# Our imports
from mwaa.config.aws import get_aws_region


def _change_protocol_to_sqs(url: str) -> str:
    """
    Make the given SQS endpoint Celery-friendly by setting the URL protocol to sqs://.

    Notice that there is no such thing as SQS protocol, but this is the
    URL convention that Celery uses to understand that the given URL is for
    an SQS queue.

    :returns The Celery-friendly SQS endpoint.
    """
    parsed_url = urlparse(url)

    # Check if the scheme was missing and was defaulted to 'http'
    if parsed_url.netloc == "":
        # Scheme is missing, netloc is actually part of the path.
        # See the documentation for urlparse() if you don't understand the
        # reasoning.
        new_netloc = parsed_url.path
        new_path = ""
    else:
        # Scheme is present.
        new_netloc = parsed_url.netloc
        new_path = parsed_url.path

    return urlunparse(
        parsed_url._replace(scheme="sqs", netloc=new_netloc, path=new_path)
    )


def get_sqs_default_endpoint() -> str:
    """
    Retrieve the default SQS endpoint for the current AWS region.

    :returns The endpoint.
    """
    # Create a session with the specified region
    session = boto3.Session(region_name=get_aws_region())

    # Create an SQS client from this session
    sqs = session.client("sqs")

    # Return the endpoint URL
    return sqs.meta.endpoint_url


def get_sqs_endpoint() -> str:
    """
    Retrieve the SQS endpoint to communicate with.

    The user can specify the endpoint via the `MWAA_CONFIG__CUSTOM_SQS_ENDPOINT`
    environment variable. Otherwise, the default endpoint for the current AWS region is
    used.

    :returns The SQS endpoint.
    """
    return _change_protocol_to_sqs(
        os.environ.get("MWAA__SQS__CUSTOM_ENDPOINT") or get_sqs_default_endpoint()
    )


def _get_queue_name_from_url(queue_url: str) -> str:
    """
    Extract the queue name from an Amazon SQS queue URL.

    :param queue_url: The URL of the SQS queue.

    :returns The name of the queue or None if the URL is invalid.
    """
    try:
        # Validate the protocol (to flag accidentally passing of sqs://
        # protocol which is just a Celery convention, rather than an
        # actual protocol.)
        if not queue_url.startswith("http://") and not queue_url.startswith("https://"):
            raise ValueError(
                f"URL {queue_url} is should start with http:// or https://"
            )

        parts = queue_url.split("/")

        if len(parts) < 2:
            raise ValueError(f"URL {queue_url} is invalid.")

        return parts[-1]
    except Exception as e:
        raise RuntimeError(f"Failed to extract queue name. Erorr: {e}")


def get_sqs_queue_url() -> str:
    """
    Retrieve the URL of the SQS queue specified for use with Celery.

    :returns The queue URL.
    """
    env_var_name = "MWAA__SQS__QUEUE_URL"
    if env_var_name not in os.environ:
        raise RuntimeError(
            "The name of the SQS queue to use should be specified in an "
            f"environment variable called '{env_var_name}.'"
        )
    return os.environ.get(env_var_name)  # type: ignore


def get_sqs_queue_name() -> str:
    """
    Retrieve the name of the SQS queue specified for use with Celery.

    :returns The queue name.
    """
    return _get_queue_name_from_url(get_sqs_queue_url())


def should_create_queue() -> bool:
    """
    Determine whether the SQS queue should be created or not. Only used with CeleryExecutor.

    :return: True or False.
    """
    return os.environ.get("MWAA__SQS__CREATE_QUEUE", "false").lower() == "true"

def should_use_ssl() -> bool:
    """
    Determine whether to use SSL when communicating with SQS or not.

    This configuration is expected to be true when connecting to AWS SQS, as it enforces
    the use of SQS. On the otherhand, when using elasticmq, which doesn't support SSL,
    this should be set to false.

    :return: True or False.
    """
    return os.environ.get("MWAA__SQS__USE_SSL", "true").lower() == "true"
