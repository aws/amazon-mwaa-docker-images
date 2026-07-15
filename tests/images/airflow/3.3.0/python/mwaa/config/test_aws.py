import os
import pytest
from mwaa.config.aws import get_aws_region

@pytest.mark.parametrize(
    "env_vars, expected", 
    [
        ({"AWS_REGION": "us-east-1"}, "us-east-1"),
        ({"AWS_DEFAULT_REGION": "us-west-2"}, "us-west-2"),
        ({"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-west-2"}, "us-east-1"),
    ]
)
def test_get_aws_region_success(env_helper, env_vars, expected):
    env_helper.set(env_vars)

    assert get_aws_region() == expected

def test_get_aws_region_raises_runtime_error():
    with pytest.raises(RuntimeError, match="Region must be specified."):
        get_aws_region()