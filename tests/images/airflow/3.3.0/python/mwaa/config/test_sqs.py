"""Tests for SQS configuration module."""

from urllib.parse import urlparse
import pytest
from unittest.mock import patch, MagicMock

from mwaa.config.sqs import (
    _change_protocol_to_sqs,
    get_sqs_default_endpoint,
    get_sqs_endpoint,
    _get_queue_name_from_url,
    get_sqs_queue_url,
    get_sqs_queue_name,
    should_create_queue,
    should_use_ssl,
)

from .base_config_test import BaseConfigTest


class TestSQSConfig(BaseConfigTest):

    # ---------------------------------------------------------
    # _change_protocol_to_sqs
    # ---------------------------------------------------------
    @pytest.mark.parametrize(
        "input_url, expected_netloc, expected_path",
        [
            pytest.param(
                "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
                "sqs.us-east-1.amazonaws.com",
                "/123456789012/test-queue",
                id="aws_https_url",
            ),
            pytest.param(
                "http://localhost:9324/queue/test-queue",
                "localhost:9324",
                "/queue/test-queue",
                id="localstack_http_url",
            ),
        ],
    )
    def test_change_protocol_to_sqs(self, input_url, expected_netloc, expected_path):
        result = _change_protocol_to_sqs(input_url)
        parsed = urlparse(result)

        assert parsed.scheme == "sqs"
        assert parsed.netloc == expected_netloc
        assert parsed.path == expected_path


    # ---------------------------------------------------------
    # get_sqs_default_endpoint
    # ---------------------------------------------------------
    def test_get_sqs_default_endpoint(self):
        with patch("mwaa.config.sqs.boto3.Session") as mock_session, \
             patch("mwaa.config.sqs.get_aws_region", return_value="us-east-1"):

            mock_client = MagicMock()
            mock_client.meta.endpoint_url = "https://sqs.us-east-1.amazonaws.com"
            mock_session.return_value.client.return_value = mock_client

            result = get_sqs_default_endpoint()

            mock_session.assert_called_once_with(region_name="us-east-1")
            mock_session.return_value.client.assert_called_once_with("sqs")
            assert result == "https://sqs.us-east-1.amazonaws.com"


    # ---------------------------------------------------------
    # get_sqs_endpoint
    # ---------------------------------------------------------
    def test_get_sqs_endpoint_custom(self, env_helper):
        env_helper.set({
            "MWAA__SQS__CUSTOM_ENDPOINT": "http://localhost:9324/queue"
        })

        result = get_sqs_endpoint()
        assert result.startswith("sqs://")

    def test_get_sqs_endpoint_default(self, env_helper):
        env_helper.delete(["MWAA__SQS__CUSTOM_ENDPOINT"])

        with patch("mwaa.config.sqs.get_sqs_default_endpoint") as mock_default:
            mock_default.return_value = "https://sqs.us-east-1.amazonaws.com"

            result = get_sqs_endpoint()

            assert result.startswith("sqs://")
            mock_default.assert_called_once()


    # ---------------------------------------------------------
    # _get_queue_name_from_url
    # ---------------------------------------------------------
    @pytest.mark.parametrize(
        "queue_url, expected_name",
        [
            ("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue", "test-queue"),
            ("http://localhost:9324/queue/test-queue", "test-queue"),
        ],
    )
    def test_get_queue_name_from_url(self, queue_url, expected_name):
        assert _get_queue_name_from_url(queue_url) == expected_name

    @pytest.mark.parametrize(
        "invalid_url",
        [
            "sqs://invalid-url",      # invalid protocol
            "ftp://example.com/q",    # invalid protocol
            # "https://",               # invalid structure
            "just-a-string",
        ],
    )
    def test_get_queue_name_from_url_invalid(self, invalid_url):
        with pytest.raises(RuntimeError, match="Failed to extract queue name"):
            _get_queue_name_from_url(invalid_url)


    # ---------------------------------------------------------
    # get_sqs_queue_url
    # ---------------------------------------------------------
    def test_get_sqs_queue_url(self, env_helper):
        env_helper.set({
            "MWAA__SQS__QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        })

        assert get_sqs_queue_url() == \
            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

    def test_get_sqs_queue_url_missing(self, env_helper):
        env_helper.delete(["MWAA__SQS__QUEUE_URL"])

        with pytest.raises(RuntimeError):
            get_sqs_queue_url()


    # ---------------------------------------------------------
    # get_sqs_queue_name
    # ---------------------------------------------------------
    def test_get_sqs_queue_name(self, env_helper):
        env_helper.set({
            "MWAA__SQS__QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        })

        assert get_sqs_queue_name() == "test-queue"

    def test_get_sqs_queue_name_calls_parser(self, env_helper):
        env_helper.set({
            "MWAA__SQS__QUEUE_URL": "https://example.com/test"
        })

        with patch("mwaa.config.sqs._get_queue_name_from_url") as mock_parser:
            mock_parser.return_value = "test"

            result = get_sqs_queue_name()

            assert result == "test"
            mock_parser.assert_called_once()


    # ---------------------------------------------------------
    # should_create_queue
    # ---------------------------------------------------------
    @pytest.mark.parametrize(
        "env_value, expected",
        [
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("anything", False),
            (None, False),
        ],
    )
    def test_should_create_queue(self, env_helper, env_value, expected):
        if env_value is not None:
            env_helper.set({
                "MWAA__SQS__CREATE_QUEUE": env_value
            })
        else:
            env_helper.delete(["MWAA__SQS__CREATE_QUEUE"])

        assert should_create_queue() == expected


    # ---------------------------------------------------------
    # should_use_ssl
    # ---------------------------------------------------------
    @pytest.mark.parametrize(
        "env_value, expected",
        [
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("anything", False),
            (None, True),
        ],
    )
    def test_should_use_ssl(self, env_helper, env_value, expected):
        if env_value is not None:
            env_helper.set({
                "MWAA__SQS__USE_SSL": env_value
            })
        else:
            env_helper.delete(["MWAA__SQS__USE_SSL"])

        assert should_use_ssl() == expected