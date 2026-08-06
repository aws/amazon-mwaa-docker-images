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


# --- ECS metadata endpoint hardening (see P481207036) ---

_ECS_ENV = {
    "AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI": "/v2/credentials/abc",
    "ECS_CONTAINER_METADATA_URI": "http://169.254.170.2/v3/containers/test",
}
_CREDS = {
    "AccessKeyId": "k",
    "SecretAccessKey": "s",
    "Token": "t",
}


def _mock_opener(side_effect=None, payload=None):
    """Build a mock urllib opener returning payload, or raising side_effect."""
    opener = MagicMock()
    if side_effect is not None:
        opener.open.side_effect = side_effect
    else:
        response = MagicMock()
        response.read.return_value = json.dumps(payload or _CREDS).encode("utf-8")
        opener.open.return_value.__enter__.return_value = response
    return opener


def _reset_token_cache():
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    RDSIAMCredentialProvider._token = None
    RDSIAMCredentialProvider._expires_at = 0


def test_ecs_metadata_fetch_passes_a_timeout():
    """The fetch must be bounded; without a timeout urllib blocks indefinitely.

    It runs inside the SQLAlchemy do_connect listener, so an unbounded hang
    would stall a metadata database connection.
    """
    from mwaa.config import rds_iam_credentials
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    opener = _mock_opener()
    with patch.dict("os.environ", _ECS_ENV), patch(
        "urllib.request.build_opener", return_value=opener
    ):
        assert RDSIAMCredentialProvider.get_ecs_credentials() == _CREDS

    _, kwargs = opener.open.call_args
    assert kwargs.get("timeout") == rds_iam_credentials._METADATA_TIMEOUT_SECONDS
    assert kwargs["timeout"] > 0


def test_ecs_metadata_fetch_retries_transient_failure_then_succeeds():
    """A single connect timeout must not fail the caller."""
    import urllib.error

    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    response = MagicMock()
    response.read.return_value = json.dumps(_CREDS).encode("utf-8")
    ok = MagicMock()
    ok.__enter__ = MagicMock(return_value=response)
    ok.__exit__ = MagicMock(return_value=False)

    opener = MagicMock()
    opener.open.side_effect = [urllib.error.URLError("timed out"), ok]

    with patch.dict("os.environ", _ECS_ENV), patch(
        "urllib.request.build_opener", return_value=opener
    ), patch("tenacity.nap.time.sleep"):
        assert RDSIAMCredentialProvider.get_ecs_credentials() == _CREDS

    assert opener.open.call_count == 2


def test_ecs_metadata_fetch_does_not_retry_config_errors():
    """A missing environment variable is not transient; fail immediately."""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    opener = _mock_opener()
    with patch.dict("os.environ", {}, clear=True), patch(
        "urllib.request.build_opener", return_value=opener
    ):
        with pytest.raises(ValueError):
            RDSIAMCredentialProvider.get_ecs_credentials()
    opener.open.assert_not_called()


def test_transient_error_classification():
    """429 and 5xx are retryable; 4xx client errors are not."""
    import urllib.error

    from mwaa.config.rds_iam_credentials import _is_transient_metadata_error

    def http(code):
        return urllib.error.HTTPError("u", code, "msg", None, None)

    assert _is_transient_metadata_error(http(429)) is True
    assert _is_transient_metadata_error(http(503)) is True
    assert _is_transient_metadata_error(http(500)) is True
    assert _is_transient_metadata_error(http(404)) is False
    assert _is_transient_metadata_error(http(403)) is False
    assert _is_transient_metadata_error(urllib.error.URLError("boom")) is True
    assert _is_transient_metadata_error(TimeoutError()) is True
    assert _is_transient_metadata_error(ConnectionResetError()) is True
    assert _is_transient_metadata_error(ValueError("config")) is False


def test_failed_refresh_reuses_a_still_valid_cached_token():
    """A refresh failure inside the grace window must not fail the caller.

    Tokens live 15 minutes and are refreshed at 10, so a blip during refresh
    still leaves a usable token.
    """
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    try:
        RDSIAMCredentialProvider._token = "still_valid"
        # Inside the refresh window (expires in 60s) but not yet expired.
        RDSIAMCredentialProvider._expires_at = time.time() + 60

        with patch.object(
            RDSIAMCredentialProvider,
            "generate_credentials",
            side_effect=Exception("metadata endpoint timed out"),
        ):
            assert RDSIAMCredentialProvider.get_token() == "still_valid"
    finally:
        _reset_token_cache()


def test_failed_refresh_raises_once_the_token_has_expired():
    """Past expiry there is nothing safe to reuse, so surface the failure."""
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    try:
        RDSIAMCredentialProvider._token = "expired"
        RDSIAMCredentialProvider._expires_at = time.time() - 1

        with patch.object(
            RDSIAMCredentialProvider,
            "generate_credentials",
            side_effect=Exception("metadata endpoint timed out"),
        ):
            with pytest.raises(Exception, match="metadata endpoint timed out"):
                RDSIAMCredentialProvider.get_token()
    finally:
        _reset_token_cache()


def test_token_expiry_measured_after_the_mint():
    """Expiry must be timed from after the fetch, not before the lock wait.

    Uses a stepped clock so a slow mint is distinguishable: timing from before
    would set expiry to 1000+900, from after to 2000+900. With a real (instant)
    mint the two are indistinguishable, which is why the clock is stubbed.
    """
    from mwaa.config.rds_iam_credentials import RDSIAMCredentialProvider

    calls = {"n": 0}

    def stepped_clock():
        calls["n"] += 1
        return 1000.0 if calls["n"] == 1 else 2000.0

    try:
        _reset_token_cache()
        with patch(
            "mwaa.config.rds_iam_credentials.time.time", side_effect=stepped_clock
        ), patch.object(
            RDSIAMCredentialProvider, "generate_credentials", return_value="fresh"
        ):
            assert RDSIAMCredentialProvider.get_token() == "fresh"

        assert RDSIAMCredentialProvider._expires_at == 2000.0 + 15 * 60
    finally:
        _reset_token_cache()
