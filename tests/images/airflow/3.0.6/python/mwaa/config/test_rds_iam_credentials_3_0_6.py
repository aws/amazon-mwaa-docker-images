import pytest
import json
import time
from unittest.mock import patch, MagicMock


def test_get_ecs_credentials_success():
    """Test successful ECS credentials retrieval"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    mock_credentials = {
        "AccessKeyId": "test_key",
        "SecretAccessKey": "test_secret",
        "Token": "test_token",
    }

    with patch.dict(
        "os.environ",
        {
            "AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI": "/v2/credentials/test",
            "ECS_CONTAINER_METADATA_URI": "http://169.254.170.2/v3/containers/test",
        },
    ), patch("urllib.request.build_opener") as mock_build_opener:
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_credentials).encode("utf-8")

        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value = mock_response
        mock_build_opener.return_value = mock_opener

        result = RDSIAMCredentialProvider.get_ecs_credentials()
        assert result == mock_credentials


def test_get_ecs_credentials_missing_env():
    """Test ECS credentials with missing environment variables"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(
            ValueError, match="AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI not set"
        ):
            RDSIAMCredentialProvider.get_ecs_credentials()


def test_get_ecs_credentials_missing_metadata_uri():
    """Test ECS credentials with missing ECS_CONTAINER_METADATA_URI"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict(
        "os.environ",
        {"AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI": "/v2/credentials/test"},
        clear=True,
    ):
        with pytest.raises(ValueError, match="ECS_CONTAINER_METADATA_URI not set"):
            RDSIAMCredentialProvider.get_ecs_credentials()


def test_get_ecs_credentials_ignores_proxy_env_vars():
    """Test that proxy environment variables don't affect ECS metadata requests"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    mock_credentials = {
        "AccessKeyId": "test_key",
        "SecretAccessKey": "test_secret",
        "Token": "test_token",
    }

    with patch.dict(
        "os.environ",
        {
            "AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI": "/v2/credentials/test",
            "ECS_CONTAINER_METADATA_URI": "http://169.254.170.2/v3/containers/test",
            "HTTP_PROXY": "http://should-not-be-used:8080",
            "HTTPS_PROXY": "http://should-not-be-used:8080",
        },
    ), patch("urllib.request.build_opener") as mock_build_opener, patch(
        "urllib.request.ProxyHandler"
    ) as mock_proxy_handler:
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_credentials).encode("utf-8")

        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value = mock_response
        mock_build_opener.return_value = mock_opener

        result = RDSIAMCredentialProvider.get_ecs_credentials()

        mock_proxy_handler.assert_called_once_with({})
        mock_build_opener.assert_called_once()
        assert result == mock_credentials


def test_get_rds_iam_token_hostname_success():
    """Test successful RDS IAM token hostname retrieval"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict("os.environ", {"RDS_IAM_TOKEN_HOSTNAME": "test.rds.amazonaws.com"}):
        result = RDSIAMCredentialProvider.get_rds_iam_token_hostname()
        assert result == "test.rds.amazonaws.com"


def test_get_rds_iam_token_hostname_missing():
    """Test RDS IAM token hostname with missing environment variable"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(
            ValueError,
            match="RDS_IAM_TOKEN_HOSTNAME environment variable is required",
        ):
            RDSIAMCredentialProvider.get_rds_iam_token_hostname()


def test_generate_rds_auth_token():
    """Test RDS auth token generation"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    mock_credentials = {
        "AccessKeyId": "test_key",
        "SecretAccessKey": "test_secret",
        "Token": "test_token",
    }

    with patch("boto3.client") as mock_boto3:
        mock_rds_client = MagicMock()
        mock_rds_client.generate_db_auth_token.return_value = "test_auth_token"
        mock_boto3.return_value = mock_rds_client

        result = RDSIAMCredentialProvider.generate_rds_auth_token(
            mock_credentials, "test.rds.amazonaws.com", 5432, "airflow_user"
        )

        assert result == "test_auth_token"
        mock_rds_client.generate_db_auth_token.assert_called_once()


def test_generate_rds_auth_token_failure():
    """Test RDS auth token generation failure"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    mock_credentials = {
        "AccessKeyId": "test_key",
        "SecretAccessKey": "test_secret",
        "Token": "test_token",
    }

    with patch("boto3.client") as mock_boto3:
        mock_rds_client = MagicMock()
        mock_rds_client.generate_db_auth_token.side_effect = Exception(
            "Token generation failed"
        )
        mock_boto3.return_value = mock_rds_client

        with pytest.raises(Exception, match="Token generation failed"):
            RDSIAMCredentialProvider.generate_rds_auth_token(
                mock_credentials, "test.rds.amazonaws.com", 5432, "airflow_user"
            )


def test_get_token_cached():
    """Test cached token retrieval"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    RDSIAMCredentialProvider._token = "cached_token"
    RDSIAMCredentialProvider._expires_at = time.time() + 600

    result = RDSIAMCredentialProvider.get_token()
    assert result == "cached_token"


def test_get_token_refresh_needed():
    """Test token refresh when expired"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    RDSIAMCredentialProvider._token = "old_token"
    RDSIAMCredentialProvider._expires_at = time.time() - 100

    with patch.object(
        RDSIAMCredentialProvider, "generate_credentials", return_value="new_token"
    ):
        result = RDSIAMCredentialProvider.get_token()
        assert result == "new_token"
        assert RDSIAMCredentialProvider._token == "new_token"


def test_generate_credentials_success():
    """Test successful credential generation"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    mock_credentials = {
        "AccessKeyId": "key",
        "SecretAccessKey": "secret",
        "Token": "token",
    }

    with patch.object(
        RDSIAMCredentialProvider, "get_ecs_credentials", return_value=mock_credentials
    ), patch.object(
        RDSIAMCredentialProvider,
        "get_rds_iam_token_hostname",
        return_value="test.rds.amazonaws.com",
    ), patch.object(
        RDSIAMCredentialProvider, "generate_rds_auth_token", return_value="auth_token"
    ):
        result = RDSIAMCredentialProvider.generate_credentials()
        assert result == "auth_token"


def test_generate_credentials_failure():
    """Test credential generation failure"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.object(
        RDSIAMCredentialProvider,
        "get_ecs_credentials",
        side_effect=Exception("ECS error"),
    ):
        with pytest.raises(Exception, match="ECS error"):
            RDSIAMCredentialProvider.generate_credentials()


def test_create_db_connection_url():
    """Test database connection URL creation"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict(
        "os.environ",
        {
            "MWAA__DB__POSTGRES_HOST": "test.rds.amazonaws.com",
            "MWAA__DB__POSTGRES_PORT": "5432",
            "MWAA__DB__POSTGRES_DB": "airflow",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
        },
    ):
        result = RDSIAMCredentialProvider.create_db_connection_url("test_token")
        assert (
            "postgresql+psycopg2://airflow_user:test_token"
            "@test.rds.amazonaws.com:5432/airflow?sslmode=require" in result
        )


def test_create_db_connection_url_generate_token():
    """Test database connection URL creation with token generation"""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    with patch.dict(
        "os.environ",
        {
            "MWAA__DB__POSTGRES_HOST": "test.rds.amazonaws.com",
            "MWAA__DB__POSTGRES_PORT": "5432",
            "MWAA__DB__POSTGRES_DB": "airflow",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
        },
    ), patch.object(
        RDSIAMCredentialProvider, "generate_credentials", return_value="generated_token"
    ):
        result = RDSIAMCredentialProvider.create_db_connection_url()
        assert (
            "postgresql+psycopg2://airflow_user:generated_token"
            "@test.rds.amazonaws.com:5432/airflow?sslmode=require" in result
        )


def test_use_iam_credentials():
    """Test IAM credentials flag detection"""
    from mwaa.config.rds_iam_credentials import use_iam_credentials

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "true"}):
        assert use_iam_credentials() is True

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}):
        assert use_iam_credentials() is False

    with patch.dict("os.environ", {}, clear=True):
        assert use_iam_credentials() is False


def test_is_using_rds_proxy():
    """Test RDS proxy detection"""
    from mwaa.config.rds_iam_credentials import is_using_rds_proxy

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "require"}):
        assert is_using_rds_proxy() is True

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "disable"}):
        assert is_using_rds_proxy() is False

    with patch.dict("os.environ", {}, clear=True):
        assert is_using_rds_proxy() is False
