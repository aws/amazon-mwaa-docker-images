"""Contain a function for retrieving AWS-related configuration."""

import os


def get_aws_region() -> str:
    """
    Retrieve the AWS region the container should communicate with.

    This is assumed to be available in either the AWS_REGION or AWS_DEFAULT_REGION
    environment variables, checked respectively.

    :returns The AWS region

    :raises RuntimeError if no environment variable for the region is available.
    """
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region
    else:
        raise RuntimeError("Region must be specified.")
