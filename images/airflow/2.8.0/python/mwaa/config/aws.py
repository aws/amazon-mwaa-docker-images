import os


def get_aws_region() -> str:
    """
    Retrieves the AWS region the container should communicate with. This is
    assumed to be available in either the AWS_REGION or AWS_DEFAULT_REGION
    environment variables, checked respectively.
    """
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region
    else:
        raise RuntimeError("Region must be specified.")
