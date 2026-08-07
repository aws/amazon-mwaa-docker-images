import pytest
import json
import time
from unittest.mock import patch, MagicMock


def test_get_ecs_credentials_success():
    """Test successful ECS credentials retrieval"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    mock_credentials = {
        'AccessKeyId': 'test_key',
        'SecretAccessKey': 'test_secret',
        'Token': 'test_token'
    }
    
    with patch.dict('os.environ', {
        'AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI': '/v2/credentials/test',
        'ECS_CONTAINER_METADATA_URI': 'http://169.254.170.2/v3/containers/test'
    }), \
    patch('urllib.request.build_opener') as mock_build_opener:
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = json.dumps(mock_credentials).encode('utf-8')
        
        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value = mock_response
        mock_build_opener.return_value = mock_opener
        
        result = RDSIAMCredentialProvider.get_ecs_credentials()
        assert result == mock_credentials


def test_get_ecs_credentials_missing_env():
    """Test ECS credentials with missing environment variables"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(ValueError, match="AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI not set"):
            RDSIAMCredentialProvider.get_ecs_credentials()


def test_get_ecs_credentials_missing_metadata_uri():
    """Test ECS credentials with missing ECS_CONTAINER_METADATA_URI"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {'AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI': '/v2/credentials/test'}, clear=True):
        with pytest.raises(ValueError, match="ECS_CONTAINER_METADATA_URI not set"):
            RDSIAMCredentialProvider.get_ecs_credentials()


def test_get_ecs_credentials_http_error():
    """Test ECS credentials with HTTP error response"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {
        'AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI': '/v2/credentials/test',
        'ECS_CONTAINER_METADATA_URI': 'http://169.254.170.2/v3/containers/test'
    }), \
    patch('urllib.request.build_opener') as mock_build_opener:
        
        mock_response = MagicMock()
        mock_response.status = 404
        
        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value = mock_response
        mock_build_opener.return_value = mock_opener
        
        with pytest.raises(Exception, match="Failed to fetch ECS credentials: 404"):
            RDSIAMCredentialProvider.get_ecs_credentials()


def test_get_ecs_credentials_ignores_proxy_env_vars():
    """Test that proxy environment variables don't affect ECS metadata requests"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    mock_credentials = {
        'AccessKeyId': 'test_key',
        'SecretAccessKey': 'test_secret', 
        'Token': 'test_token'
    }
    
    with patch.dict('os.environ', {
        'AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI': '/v2/credentials/test',
        'ECS_CONTAINER_METADATA_URI': 'http://169.254.170.2/v3/containers/test',
        'HTTP_PROXY': 'http://should-not-be-used:8080',
        'HTTPS_PROXY': 'http://should-not-be-used:8080',
        'http_proxy': 'http://should-not-be-used:8080',
        'https_proxy': 'http://should-not-be-used:8080'
    }), \
    patch('urllib.request.build_opener') as mock_build_opener, \
    patch('urllib.request.ProxyHandler') as mock_proxy_handler:
        
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.read.return_value = json.dumps(mock_credentials).encode('utf-8')
        
        mock_opener = MagicMock()
        mock_opener.open.return_value.__enter__.return_value = mock_response
        mock_build_opener.return_value = mock_opener
        
        result = RDSIAMCredentialProvider.get_ecs_credentials()
        
        # Verify that ProxyHandler was instantiated with empty dict
        mock_proxy_handler.assert_called_once_with({})
        # Verify build_opener was called with the no-proxy handler
        mock_build_opener.assert_called_once()
        
        assert result == mock_credentials


def test_get_rds_iam_token_hostname_success():
    """Test successful RDS IAM token hostname retrieval"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {'RDS_IAM_TOKEN_HOSTNAME': 'test.rds.amazonaws.com'}):
        result = RDSIAMCredentialProvider.get_rds_iam_token_hostname()
        assert result == 'test.rds.amazonaws.com'


def test_get_rds_iam_token_hostname_missing():
    """Test RDS IAM token hostname with missing environment variable"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {}, clear=True), \
         patch('mwaa.utils.get_rds_iam_credentials.logger') as mock_logger:
        
        with pytest.raises(ValueError, match="RDS_IAM_TOKEN_HOSTNAME environment variable is required"):
            RDSIAMCredentialProvider.get_rds_iam_token_hostname()


def test_generate_rds_auth_token():
    """Test RDS auth token generation"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    mock_credentials = {
        'AccessKeyId': 'test_key',
        'SecretAccessKey': 'test_secret',
        'Token': 'test_token'
    }
    
    with patch('boto3.client') as mock_boto3:
        mock_rds_client = MagicMock()
        mock_rds_client.generate_db_auth_token.return_value = 'test_auth_token'
        mock_boto3.return_value = mock_rds_client
        
        result = RDSIAMCredentialProvider.generate_rds_auth_token(
            mock_credentials, 'test.rds.amazonaws.com', 5432, 'airflow_user'
        )
        
        assert result == 'test_auth_token'
        mock_rds_client.generate_db_auth_token.assert_called_once()


def test_generate_rds_auth_token_failure():
    """Test RDS auth token generation failure"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    mock_credentials = {
        'AccessKeyId': 'test_key',
        'SecretAccessKey': 'test_secret',
        'Token': 'test_token'
    }
    
    with patch('boto3.client') as mock_boto3, \
         patch('mwaa.utils.get_rds_iam_credentials.logger') as mock_logger:
        
        mock_rds_client = MagicMock()
        mock_rds_client.generate_db_auth_token.side_effect = Exception("Token generation failed")
        mock_boto3.return_value = mock_rds_client
        
        with pytest.raises(Exception, match="Token generation failed"):
            RDSIAMCredentialProvider.generate_rds_auth_token(
                mock_credentials, 'test.rds.amazonaws.com', 5432, 'airflow_user'
            )
        
        mock_logger.error.assert_called_once_with("Failed to generate RDS auth token: Token generation failed")


def test_get_token_cached():
    """Test cached token retrieval"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    # Set up cached token
    RDSIAMCredentialProvider._token = 'cached_token'
    RDSIAMCredentialProvider._expires_at = time.time() + 600  # 10 minutes from now
    
    result = RDSIAMCredentialProvider.get_token()
    assert result == 'cached_token'


def test_get_token_refresh_needed():
    """Test token refresh when expired"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    # Set up expired token
    RDSIAMCredentialProvider._token = 'old_token'
    RDSIAMCredentialProvider._expires_at = time.time() - 100  # Expired
    
    with patch.object(RDSIAMCredentialProvider, 'generate_credentials', return_value='new_token'):
        result = RDSIAMCredentialProvider.get_token()
        assert result == 'new_token'
        assert RDSIAMCredentialProvider._token == 'new_token'


def test_generate_credentials_success():
    """Test successful credential generation"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    mock_credentials = {'AccessKeyId': 'key', 'SecretAccessKey': 'secret', 'Token': 'token'}
    
    with patch.object(RDSIAMCredentialProvider, 'get_ecs_credentials', return_value=mock_credentials), \
         patch.object(RDSIAMCredentialProvider, 'get_rds_iam_token_hostname', return_value='test.rds.amazonaws.com'), \
         patch.object(RDSIAMCredentialProvider, 'generate_rds_auth_token', return_value='auth_token'):
        
        result = RDSIAMCredentialProvider.generate_credentials()
        assert result == 'auth_token'


def test_generate_credentials_failure():
    """A failed mint must raise rather than return None.

    Regression test: returning None poisoned the cache -- get_token() stored
    the None with a full 15-minute lifetime, so every caller in the process
    was handed an unusable credential until the entry aged out.
    """
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.object(RDSIAMCredentialProvider, 'get_ecs_credentials', side_effect=Exception("ECS error")), \
         patch('mwaa.utils.get_rds_iam_credentials.logger') as mock_logger:
        
        with pytest.raises(Exception, match="ECS error"):
            RDSIAMCredentialProvider.generate_credentials()
        mock_logger.error.assert_called_with("Failed to update credentials: ECS error")


def test_create_db_connection_url():
    """Test database connection URL creation"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {
        'POSTGRES_HOST': 'test.rds.amazonaws.com',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'airflow',
        'SSL_MODE': 'require'
    }):
        result = RDSIAMCredentialProvider.create_db_connection_url('test_token')
        assert 'postgresql+psycopg2://airflow_user:test_token@test.rds.amazonaws.com:5432/airflow?sslmode=require' in result


def test_create_db_connection_url_generate_token():
    """The default token path must go through the thread-safe cache.

    Regression test: calling generate_credentials() directly bypassed
    get_token(), minting a fresh token (ECS metadata fetch + signed boto3 call)
    on every connection instead of reusing the cached one.
    """
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    # Start from a cold cache; other tests in this module mutate this state.
    RDSIAMCredentialProvider._token = None
    RDSIAMCredentialProvider._expires_at = 0

    with patch.dict('os.environ', {
        'POSTGRES_HOST': 'test.rds.amazonaws.com',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'airflow',
        'SSL_MODE': 'require'
    }), \
    patch.object(RDSIAMCredentialProvider, 'generate_credentials', return_value='generated_token') as mock_generate:
        expected = (
            'postgresql+psycopg2://airflow_user:generated_token'
            '@test.rds.amazonaws.com:5432/airflow?sslmode=require'
        )

        assert expected in RDSIAMCredentialProvider.create_db_connection_url()
        # Second call must be served from the cache, not mint a new token.
        assert expected in RDSIAMCredentialProvider.create_db_connection_url()
        assert mock_generate.call_count == 1

    RDSIAMCredentialProvider._token = None
    RDSIAMCredentialProvider._expires_at = 0


def test_create_db_connection_url_generation_failure():
    """URL creation must surface a failed mint on the default token path."""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    RDSIAMCredentialProvider._token = None
    RDSIAMCredentialProvider._expires_at = 0

    with patch.object(
        RDSIAMCredentialProvider,
        'generate_credentials',
        side_effect=Exception("Failed to generate RDS auth token"),
    ):
        with pytest.raises(Exception, match="Failed to generate RDS auth token"):
            RDSIAMCredentialProvider.create_db_connection_url()

# --- ECS metadata endpoint hardening (see P481207036) ---

def test_get_token_does_not_cache_a_failed_mint():
    """A failed cold-start mint must not populate the cache.

    Regression test for the None-poisoning defect: generate_credentials()
    used to swallow failures and return None, which get_token() cached with a
    full 15-minute lifetime -- every caller in the process was then handed an
    unusable credential until the entry aged out. A failure must propagate
    and leave the cache empty so the next caller retries.
    """
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    try:
        RDSIAMCredentialProvider._token = None
        RDSIAMCredentialProvider._expires_at = 0

        with patch.object(
            RDSIAMCredentialProvider,
            'get_ecs_credentials',
            side_effect=Exception("ECS error"),
        ):
            with pytest.raises(Exception, match="ECS error"):
                RDSIAMCredentialProvider.get_token()

        # The failure left the cache empty rather than caching a dud...
        assert RDSIAMCredentialProvider._token is None
        assert RDSIAMCredentialProvider._expires_at == 0
        # ...so the very next caller retries and succeeds.
        with patch.object(
            RDSIAMCredentialProvider, 'generate_credentials', return_value='recovered'
        ):
            assert RDSIAMCredentialProvider.get_token() == 'recovered'
    finally:
        RDSIAMCredentialProvider._token = None
        RDSIAMCredentialProvider._expires_at = 0



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
        response.status = 200
        response.read.return_value = json.dumps(payload or _CREDS).encode("utf-8")
        opener.open.return_value.__enter__.return_value = response
    return opener


def _reset_token_cache():
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    RDSIAMCredentialProvider._token = None
    RDSIAMCredentialProvider._expires_at = 0


def _join_refresh_thread():
    """Wait for the background refresh thread so assertions are deterministic."""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    thread = RDSIAMCredentialProvider._refresh_thread
    if thread is not None:
        thread.join(timeout=5)
        assert not thread.is_alive(), "background refresh did not finish"


def test_ecs_metadata_fetch_passes_a_timeout():
    """The fetch must be bounded; without a timeout urllib blocks indefinitely.

    It runs inside the SQLAlchemy do_connect listener, so an unbounded hang
    would stall a metadata database connection.
    """
    from mwaa.utils import get_rds_iam_credentials
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    opener = _mock_opener()
    with patch.dict("os.environ", _ECS_ENV), patch(
        "urllib.request.build_opener", return_value=opener
    ):
        assert RDSIAMCredentialProvider.get_ecs_credentials() == _CREDS

    _, kwargs = opener.open.call_args
    assert kwargs.get("timeout") == get_rds_iam_credentials._METADATA_TIMEOUT_SECONDS
    assert kwargs["timeout"] > 0


def test_ecs_metadata_fetch_retries_transient_failure_then_succeeds():
    """A single connect timeout must not fail the caller."""
    import urllib.error

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    response = MagicMock()
    response.status = 200
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
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

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

    from mwaa.utils.get_rds_iam_credentials import _is_transient_metadata_error

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
    still leaves a usable token. The failure happens on the background thread
    and is logged; the cached token stays in place, and the single-flight
    guard is released so a later caller can retry the refresh.
    """
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    try:
        RDSIAMCredentialProvider._token = "still_valid"
        # Inside the refresh window (expires in 60s) but not yet expired.
        expires_at = time.time() + 60
        RDSIAMCredentialProvider._expires_at = expires_at

        with patch.object(
            RDSIAMCredentialProvider,
            "generate_credentials",
            side_effect=Exception("metadata endpoint timed out"),
        ):
            assert RDSIAMCredentialProvider.get_token() == "still_valid"
            _join_refresh_thread()

        # The failed refresh left the cache untouched...
        assert RDSIAMCredentialProvider._token == "still_valid"
        assert RDSIAMCredentialProvider._expires_at == expires_at
        # ...and released the single-flight guard for the next attempt.
        assert RDSIAMCredentialProvider._refresh_lock.acquire(blocking=False)
        RDSIAMCredentialProvider._refresh_lock.release()
    finally:
        _reset_token_cache()


def test_failed_refresh_raises_once_the_token_has_expired():
    """Past expiry there is nothing safe to reuse, so surface the failure."""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

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
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    calls = {"n": 0}

    def stepped_clock():
        calls["n"] += 1
        return 1000.0 if calls["n"] == 1 else 2000.0

    try:
        _reset_token_cache()
        with patch(
            "mwaa.utils.get_rds_iam_credentials.time.time", side_effect=stepped_clock
        ), patch.object(
            RDSIAMCredentialProvider, "generate_credentials", return_value="fresh"
        ):
            assert RDSIAMCredentialProvider.get_token() == "fresh"

        assert RDSIAMCredentialProvider._expires_at == 2000.0 + 15 * 60
    finally:
        _reset_token_cache()


# --- Off-critical-path token refresh ---
#
# The 5-minute refresh buffer on the 15-minute token lifetime exists so the
# refresh can happen without a caller paying for it. These tests pin that
# property: a usable token is always served immediately, the refresh runs on a
# background thread, and only a cold start or a genuinely expired token blocks
# the caller.


def test_due_token_is_served_immediately_and_refreshed_in_background():
    """A caller holding a usable token must not pay for the refresh.

    Regression test: get_token() used to perform the fetch on the calling
    thread while holding the lock, so the connection that arrived at the
    10-minute mark blocked on the ECS metadata endpoint and every other
    caller in the process queued behind it.
    """
    import threading

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    caller = threading.current_thread()
    minting_thread = {}

    def record_thread():
        minting_thread["thread"] = threading.current_thread()
        return "refreshed"

    try:
        RDSIAMCredentialProvider._token = "due_but_valid"
        # Due for refresh (fewer than 5 minutes left) but not expired.
        RDSIAMCredentialProvider._expires_at = time.time() + 60

        with patch.object(
            RDSIAMCredentialProvider, "generate_credentials", side_effect=record_thread
        ):
            # The caller is handed the cached token, not the refreshed one.
            assert RDSIAMCredentialProvider.get_token() == "due_but_valid"
            _join_refresh_thread()

        # The refresh ran on a different thread and updated the cache.
        assert minting_thread["thread"] is not caller
        assert RDSIAMCredentialProvider._token == "refreshed"
        assert RDSIAMCredentialProvider._expires_at > time.time() + 14 * 60
    finally:
        _reset_token_cache()


def test_background_refresh_is_single_flight():
    """Concurrent callers noticing a due token must not multiply fetches.

    Multiplying connects to the ECS credential endpoint under contention is
    the amplification pattern identified in P481207036, so while one refresh
    is in flight further callers must be served the cached token without
    triggering another fetch.
    """
    import threading

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    release = threading.Event()
    calls = []

    def slow_mint():
        calls.append(1)
        assert release.wait(timeout=5), "test never released the mint"
        return "refreshed"

    try:
        RDSIAMCredentialProvider._token = "due_but_valid"
        RDSIAMCredentialProvider._expires_at = time.time() + 60

        with patch.object(
            RDSIAMCredentialProvider, "generate_credentials", side_effect=slow_mint
        ):
            for _ in range(5):
                assert RDSIAMCredentialProvider.get_token() == "due_but_valid"
            release.set()
            _join_refresh_thread()

        assert len(calls) == 1
    finally:
        release.set()
        _reset_token_cache()


def test_cold_start_fetches_synchronously():
    """With nothing cached there is nothing to serve, so the caller waits."""
    import threading

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    minting_thread = {}

    def record_thread():
        minting_thread["thread"] = threading.current_thread()
        return "fresh"

    try:
        _reset_token_cache()
        with patch.object(
            RDSIAMCredentialProvider, "generate_credentials", side_effect=record_thread
        ):
            assert RDSIAMCredentialProvider.get_token() == "fresh"

        assert minting_thread["thread"] is threading.current_thread()
    finally:
        _reset_token_cache()


def test_expired_token_fetches_synchronously():
    """Past expiry the cached token is unusable, so the caller waits."""
    import threading

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    minting_thread = {}

    def record_thread():
        minting_thread["thread"] = threading.current_thread()
        return "fresh"

    try:
        RDSIAMCredentialProvider._token = "expired"
        RDSIAMCredentialProvider._expires_at = time.time() - 1

        with patch.object(
            RDSIAMCredentialProvider, "generate_credentials", side_effect=record_thread
        ):
            assert RDSIAMCredentialProvider.get_token() == "fresh"

        assert minting_thread["thread"] is threading.current_thread()
    finally:
        _reset_token_cache()


def test_locks_are_replaced_after_fork():
    """A child forked while a lock is held must not inherit it locked.

    Airflow forks (Celery prefork workers, DAG processors). A lock held at
    fork time is inherited locked by the child with no thread alive to
    release it, which would wedge every future refresh in that child.
    """
    from mwaa.utils import get_rds_iam_credentials as m

    old_lock = m.RDSIAMCredentialProvider._lock
    old_refresh_lock = m.RDSIAMCredentialProvider._refresh_lock
    old_lock.acquire()
    old_refresh_lock.acquire()
    try:
        m._reset_locks_after_fork()

        assert m.RDSIAMCredentialProvider._lock is not old_lock
        assert m.RDSIAMCredentialProvider._refresh_lock is not old_refresh_lock
        # The replacements are usable.
        assert m.RDSIAMCredentialProvider._lock.acquire(blocking=False)
        m.RDSIAMCredentialProvider._lock.release()
        assert m.RDSIAMCredentialProvider._refresh_lock.acquire(blocking=False)
        m.RDSIAMCredentialProvider._refresh_lock.release()
    finally:
        old_lock.release()
        old_refresh_lock.release()


def test_fork_hook_is_registered_at_import():
    """The at-fork reset must be wired up when the module is imported."""
    import importlib

    from mwaa.utils import get_rds_iam_credentials as m

    try:
        with patch("os.register_at_fork") as mock_register:
            importlib.reload(m)
        mock_register.assert_called_once()
        hook = mock_register.call_args.kwargs["after_in_child"]
        assert hook.__name__ == "_reset_locks_after_fork"
    finally:
        # Restore a clean module (the reload above replaced the class object).
        importlib.reload(m)


def test_failed_thread_start_releases_the_single_flight_guard():
    """If the refresh thread cannot start, later callers must be able to retry."""
    import threading

    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider

    try:
        RDSIAMCredentialProvider._token = "due_but_valid"
        RDSIAMCredentialProvider._expires_at = time.time() + 60

        with patch.object(
            threading.Thread, "start", side_effect=RuntimeError("can't start new thread")
        ):
            # The caller is unaffected: it still gets the cached token.
            assert RDSIAMCredentialProvider.get_token() == "due_but_valid"

        # The guard was released, so the next caller can attempt the refresh.
        assert RDSIAMCredentialProvider._refresh_lock.acquire(blocking=False)
        RDSIAMCredentialProvider._refresh_lock.release()
    finally:
        _reset_token_cache()
