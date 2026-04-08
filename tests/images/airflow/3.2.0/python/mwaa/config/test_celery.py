from unittest.mock import patch

def test_create_celery_config_happy_path():

    with \
        patch("mwaa.config.aws.get_aws_region", return_value="us-east-1"), \
        patch("mwaa.config.sqs.should_use_ssl", return_value=True), \
        patch("mwaa.config.sqs.get_sqs_queue_url", return_value="https://sqs.aws/test-queue"), \
        patch("mwaa.config.sqs.get_sqs_queue_name", return_value="test-queue"), \
        patch("mwaa.utils.qualified_name", return_value="mocked.transport.Transport"), \
        patch("airflow.providers.celery.executors.default_celery.DEFAULT_CELERY_CONFIG", new={"broker_transport_options": {"visibility_timeout": 30}}):

        from mwaa.config.celery import create_celery_config
        
        result = create_celery_config()

        assert result["broker_transport"] == "mocked.transport.Transport"
        assert result["broker_transport_options"]["region"] == "us-east-1"
        assert result["broker_transport_options"]["is_secure"] is True
        assert result["broker_transport_options"]["predefined_queues"]["test-queue"]["url"] == "https://sqs.aws/test-queue"
        assert result["broker_transport_options"]["predefined_queues"]["default"]["url"] == "https://sqs.aws/test-queue"
