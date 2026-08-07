"""RDS IAM credential provider for generating authentication tokens."""
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import quote_plus

import boto3
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# The ECS task credential endpoint is link-local (169.254.170.2), routed inside
# the task's own network namespace, and normally answers in milliseconds. It can
# still fail with a connect timeout, but per the Fargate Data Plane analysis in
# P481207036 that is not the endpoint being unhealthy or throttling: breaching
# the TMDS limit (80 req/s steady, 120 burst) returns HTTP 429 on a completed
# connection, whereas a connect timeout means the *calling* process could not
# complete connect() in time because it is resource starved. So the call must be
# bounded and retried rather than left to block.
#
# Note botocore's AWS_METADATA_SERVICE_TIMEOUT / _NUM_ATTEMPTS do not apply here:
# this fetch is plain urllib, not botocore, so the values have to be set
# explicitly.
_METADATA_TIMEOUT_SECONDS = float(
    os.environ.get("MWAA__DB__IAM_METADATA_TIMEOUT", "5")
)
_METADATA_ATTEMPTS = int(os.environ.get("MWAA__DB__IAM_METADATA_ATTEMPTS", "3"))

# Tokens are valid for 15 minutes; a refresh is attempted once fewer than 5
# remain. The buffer exists so the refresh can happen off the critical path:
# a caller is handed the still-valid cached token immediately while a
# background thread mints the replacement.
_TOKEN_LIFETIME_SECONDS = 15 * 60
_TOKEN_REFRESH_BUFFER_SECONDS = 5 * 60


def _is_transient_metadata_error(exception):
    """Return True for metadata failures that are worth retrying.

    Retries connect/read timeouts and connection errors, plus HTTP 429 (the
    documented TMDS throttle response) and 5xx. Deliberately does not retry
    configuration mistakes such as a missing environment variable, or client
    errors like 404, where retrying only delays the failure.
    """
    if isinstance(exception, urllib.error.HTTPError):
        return exception.code == 429 or exception.code >= 500
    # socket.timeout is an alias of TimeoutError on Python 3.10+.
    return isinstance(exception, (urllib.error.URLError, TimeoutError, ConnectionError))


_with_metadata_retry = retry(
    stop=stop_after_attempt(_METADATA_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    retry=retry_if_exception(_is_transient_metadata_error),
    reraise=True,
)


class RDSIAMCredentialProvider:
    """Provider for RDS IAM authentication tokens with thread-safe caching."""
    _lock = threading.Lock()
    # Single-flight guard for the background refresh: acquired without
    # blocking by whichever caller notices the token is due, released by the
    # refresh thread when it finishes. (threading.Lock permits releasing from
    # a thread other than the acquirer.)
    _refresh_lock = threading.Lock()
    # The last background refresh thread, kept for introspection and tests.
    _refresh_thread = None
    _token = None
    _expires_at = 0

    @staticmethod
    @_with_metadata_retry
    def get_ecs_credentials():
        """
        Get AWS credentials from ECS task metadata endpoint.

        The request is bounded by MWAA__DB__IAM_METADATA_TIMEOUT and retried up
        to MWAA__DB__IAM_METADATA_ATTEMPTS times on transient failures. Without
        a timeout this call can block indefinitely, and it runs inside the
        SQLAlchemy do_connect listener, so a hang would stall a metadata
        database connection.

        Returns:
            dict: AWS credentials containing AccessKeyId, SecretAccessKey, and Token

        Raises:
            ValueError: If required environment variables are not set
        """
        relative_uri = os.environ.get('AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI')
        if not relative_uri:
            raise ValueError("AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI not set")

        # Extract base URL from ECS_CONTAINER_METADATA_URI instead of hardcoding
        metadata_uri = os.environ.get('ECS_CONTAINER_METADATA_URI')
        if not metadata_uri:
            raise ValueError("ECS_CONTAINER_METADATA_URI not set")

        # Parse the base URL from the metadata URI (e.g., http://169.254.170.2/v3/...)
        from urllib.parse import urlparse
        parsed_uri = urlparse(metadata_uri)
        base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"

        credentials_url = f"{base_url}{relative_uri}"

        # Bypass proxy for ECS metadata endpoint
        request = urllib.request.Request(credentials_url)
        no_proxy_handler = urllib.request.ProxyHandler({})
        opener = urllib.request.build_opener(no_proxy_handler)

        with opener.open(request, timeout=_METADATA_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch ECS credentials: {response.status}")
            credentials_data = json.loads(response.read().decode('utf-8'))

        return credentials_data

    @staticmethod
    def get_rds_iam_token_hostname():
        """
        Get the RDS hostname for IAM token generation from environment variable.
        For RDS Proxy IAM authentication, tokens must be generated using the 
        direct RDS Proxy/Cluster endpoint, not VPC/NLB endpoints used for connections.
        
        Args:
            connection_hostname (str): The connection hostname (unused but kept for compatibility)
            
        Returns:
            str: RDS hostname for token generation
            
        Raises:
            ValueError: If RDS_IAM_TOKEN_HOSTNAME environment variable is not set
        """
        # Get the RDS hostname specifically for IAM token generation
        token_hostname = os.environ.get('RDS_IAM_TOKEN_HOSTNAME', '').strip()
        if token_hostname:
            return token_hostname
        else:
            logger.error("ERROR: RDS_IAM_TOKEN_HOSTNAME not set in environment")
            logger.error("This should be set by the CDK stack to the RDS Cluster/Proxy endpoint")
            raise ValueError("RDS_IAM_TOKEN_HOSTNAME environment variable is required")

    @staticmethod
    def generate_rds_auth_token(credentials, hostname, port, username):
        """
        Generate RDS IAM authentication token using boto3 with explicit credentials.
        
        Args:
            credentials (dict): AWS credentials from ECS task metadata
            hostname (str): RDS hostname for token generation
            port (int): Database port number
            username (str): Database username
            
        Returns:
            str: RDS IAM authentication token
            
        Raises:
            Exception: If token generation fails
        """
        region = os.environ.get('AWS_REGION', 'us-west-2')

        # Create RDS client with explicit credentials from ECS task
        rds_client = boto3.client(
            'rds',
            region_name=region,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['Token']
        )

        # Generate the auth token
        try:
            auth_token = rds_client.generate_db_auth_token(
                DBHostname=hostname,
                Port=port,
                DBUsername=username
            )

            return auth_token

        except Exception as e:
            logger.error(f"Failed to generate RDS auth token: {e}")
            raise

    @staticmethod
    def generate_credentials():
        """
        Generate fresh RDS auth token.

        Raises on failure rather than returning None. Returning None poisoned
        the cache: get_token() stored the None with a full 15-minute lifetime,
        so every caller in the process was handed an unusable credential until
        the entry aged out. Raising also lets get_token() distinguish a failed
        refresh (fall back to the still-valid cached token) from a failed cold
        start (surface the error).

        Returns:
            str: RDS auth token

        Raises:
            Exception: If credential generation fails
        """
        try:
            # Get ECS credentials from task metadata endpoint
            credentials = RDSIAMCredentialProvider.get_ecs_credentials()
            
            # For RDS Proxy with VPC Endpoints, we need to use the direct RDS Proxy hostname
            # for token generation, not the VPC Endpoint hostname used for connections
            iam_token_hostname = RDSIAMCredentialProvider.get_rds_iam_token_hostname()
            
            # Generate RDS auth token using the direct RDS hostname
            auth_token = RDSIAMCredentialProvider.generate_rds_auth_token(
                credentials=credentials,
                hostname=iam_token_hostname,
                port=5432,
                username='airflow_user'
            )

            return auth_token

        except Exception as e:
            logger.error(f"Failed to update credentials: {e}")
            raise

    @classmethod
    def get_token(cls):
        """
        Get cached RDS IAM token, refreshing if due, expired or missing.

        Tokens are valid for 15 minutes and become due for refresh once fewer
        than 5 remain. A caller holding a usable token never pays for the
        refresh: the cached token is returned immediately and the refresh runs
        on a background thread (see ``_start_background_refresh``). This keeps
        the ECS metadata fetch off the connection critical path -- the fetch
        is most likely to be slow exactly when the process is busy
        (P481207036), which is also when connections are most frequent.

        Only when nothing usable is cached -- cold start, or a token that has
        actually expired -- is the fetch performed synchronously on the
        calling thread. That path holds the lock across the mint, which
        serialises concurrent callers into a single fetch (the shared
        credential provider shape the Fargate Data Plane team recommends to
        avoid multiplying connects to the endpoint), and is only safe to hold
        across the network call because the call is bounded by
        MWAA__DB__IAM_METADATA_TIMEOUT.

        Returns:
            str: Cached or freshly generated RDS auth token
        """
        now = time.time()
        token = cls._token

        if token is not None and now < cls._expires_at:
            # The cached token is usable. If it is due for refresh, hand the
            # refresh to a background thread and serve the cached token
            # immediately rather than making this caller -- typically a
            # database connection inside the do_connect listener -- block on
            # the fetch.
            if now > cls._expires_at - _TOKEN_REFRESH_BUFFER_SECONDS:
                cls._start_background_refresh()
            return token

        # Nothing usable: cold start, or the token has expired. Fetch
        # synchronously; the double-check returns without a second fetch for
        # the threads that queued behind the winner.
        with cls._lock:
            if cls._token is None or time.time() >= cls._expires_at:
                token = cls.generate_credentials()
                cls._token = token
                # Timed from after the mint, not from before the lock wait,
                # so a slow refresh cannot overstate the token's lifetime.
                cls._expires_at = time.time() + _TOKEN_LIFETIME_SECONDS
            return cls._token

    @classmethod
    def _start_background_refresh(cls):
        """
        Refresh the token on a background thread, single-flight.

        The non-blocking acquire keeps the refresh single-flight, so
        concurrent callers noticing a due token do not multiply connects to
        the ECS credential endpoint -- multiplying connects under contention
        is the amplification pattern identified in P481207036.

        A refresh failure is logged and the still-valid cached token stays in
        place: tokens become due at the 10-minute mark of their 15-minute
        lifetime precisely so a failed attempt leaves a ~5 minute window in
        which later callers re-trigger the refresh.
        """
        if not cls._refresh_lock.acquire(blocking=False):
            # A refresh is already in flight.
            return

        def _refresh():
            try:
                token = cls.generate_credentials()
                cls._token = token
                cls._expires_at = time.time() + _TOKEN_LIFETIME_SECONDS
            except Exception as e:
                logger.warning(
                    "Could not refresh the RDS IAM token in the background "
                    "(%s); keeping the cached token, which is still valid.",
                    e,
                )
            finally:
                cls._refresh_lock.release()

        try:
            thread = threading.Thread(
                target=_refresh, name="mwaa-rds-iam-token-refresh", daemon=True
            )
            cls._refresh_thread = thread
            thread.start()
        except Exception as e:
            # Could not spawn the thread (e.g. resource exhaustion). Release
            # the guard so a later caller can try again; the cached token is
            # still valid, so this caller is unaffected.
            cls._refresh_lock.release()
            logger.warning("Could not start the RDS IAM token refresh thread: %s", e)

    @classmethod
    def create_db_connection_url(cls, token=None):
        """
        Create a PostgreSQL connection URL for RDS IAM authentication.
        
        Args:
            token (str, optional): RDS IAM auth token. If None, the cached
                token from ``get_token()`` is used.
        
        Returns:
            str: PostgreSQL connection URL with IAM authentication
        """
        if token is None:
            # Route through get_token() rather than generate_credentials() so
            # the default path reuses the thread-safe cache. Calling
            # generate_credentials() directly would mint a new token -- an ECS
            # metadata fetch plus a signed boto3 call -- on every connection.
            token = cls.get_token()

        IAM_POSTGRES_USER = "airflow_user"
        AUTH_TOKEN = quote_plus(token)
        POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
        POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
        POSTGRES_DB = os.environ.get("POSTGRES_DB", "AirflowMetadata")
        SSL_MODE = os.environ.get("SSL_MODE", "require")

        connection_url = (
            f"postgresql+psycopg2://{IAM_POSTGRES_USER}:{AUTH_TOKEN}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
            f"?sslmode={SSL_MODE}"
        )

        return connection_url


def _reset_locks_after_fork():
    """
    Recreate the provider's locks in a forked child.

    Airflow forks (Celery prefork workers, DAG processors). If the parent
    forks while a lock is held, the child inherits it locked with no thread
    alive to release it, which would wedge every future refresh in that child.
    The cached token itself is plain data and remains valid across the fork,
    so only the locks are replaced.
    """
    RDSIAMCredentialProvider._lock = threading.Lock()
    RDSIAMCredentialProvider._refresh_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_locks_after_fork)
