from unittest.mock import patch


def test_get_broker_transport_config():
    """Test that get_broker_transport_config returns the shared broker config."""
    with \
        patch("mwaa.config.celery.get_aws_region", return_value="us-east-1"), \
        patch("mwaa.config.celery.should_use_ssl", return_value=True), \
        patch("mwaa.config.celery.get_sqs_queue_url", return_value="https://sqs.aws/test-queue"), \
        patch("mwaa.config.celery.get_sqs_queue_name", return_value="test-queue"), \
        patch("mwaa.config.celery.qualified_name", return_value="mocked.transport.Transport"):

        from mwaa.config.celery import get_broker_transport_config

        result = get_broker_transport_config()

        assert result["broker_transport"] == "mocked.transport.Transport"
        assert result["broker_transport_options"]["region"] == "us-east-1"
        assert result["broker_transport_options"]["is_secure"] is True
        assert result["broker_transport_options"]["visibility_timeout"] == 43200
        assert result["broker_transport_options"]["predefined_queues"]["test-queue"]["url"] == "https://sqs.aws/test-queue"
        assert result["broker_transport_options"]["predefined_queues"]["default"]["url"] == "https://sqs.aws/test-queue"
        # Ensure only broker_transport and broker_transport_options are present
        assert set(result.keys()) == {"broker_transport", "broker_transport_options"}


def test_create_celery_config_happy_path():
    """Test that create_celery_config merges broker config with Airflow defaults."""
    with \
        patch("mwaa.config.celery.get_aws_region", return_value="us-east-1"), \
        patch("mwaa.config.celery.should_use_ssl", return_value=True), \
        patch("mwaa.config.celery.get_sqs_queue_url", return_value="https://sqs.aws/test-queue"), \
        patch("mwaa.config.celery.get_sqs_queue_name", return_value="test-queue"), \
        patch("mwaa.config.celery.qualified_name", return_value="mocked.transport.Transport"), \
        patch("airflow.providers.celery.executors.default_celery.DEFAULT_CELERY_CONFIG", new={"broker_transport_options": {"visibility_timeout": 30}}):

        from mwaa.config.celery import create_celery_config

        result = create_celery_config()

        assert result["broker_transport"] == "mocked.transport.Transport"
        assert result["broker_transport_options"]["region"] == "us-east-1"
        assert result["broker_transport_options"]["is_secure"] is True
        assert result["broker_transport_options"]["predefined_queues"]["test-queue"]["url"] == "https://sqs.aws/test-queue"
        assert result["broker_transport_options"]["predefined_queues"]["default"]["url"] == "https://sqs.aws/test-queue"
        # Verify database_engine_options are still present (worker-only config)
        assert "database_engine_options" in result
        assert result["database_engine_options"]["pool_pre_ping"] is True


def test_create_celery_config_uses_get_broker_transport_config():
    """Verify create_celery_config delegates to get_broker_transport_config for broker settings."""
    mock_broker = {
        "broker_transport": "test.Transport",
        "broker_transport_options": {
            "predefined_queues": {"q": {"url": "http://q"}},
            "is_secure": False,
            "region": "eu-west-1",
            "visibility_timeout": 100,
        },
    }
    with \
        patch("mwaa.config.celery.get_broker_transport_config", return_value=mock_broker), \
        patch("airflow.providers.celery.executors.default_celery.DEFAULT_CELERY_CONFIG", new={"broker_transport_options": {}}):

        from mwaa.config.celery import create_celery_config

        result = create_celery_config()

        assert result["broker_transport"] == "test.Transport"
        assert result["broker_transport_options"]["region"] == "eu-west-1"
        assert result["broker_transport_options"]["visibility_timeout"] == 100
        assert result["broker_transport_options"]["predefined_queues"]["q"]["url"] == "http://q"
