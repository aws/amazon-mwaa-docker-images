"""RDS IAM credential provider for MWAA.

Provides IAM-based authentication tokens for RDS connections,
with thread-safe caching and automatic refresh.
"""

import json
import logging
import os
import threading
import time
import urllib.request
from urllib.parse import quote_plus, urlparse

import boto3

logger = logging.getLogger(__name__)

DB_IAM_USERNAME = "airflow_user"


class RDSIAMCredentialProvider:
    """Provides cached RDS IAM authentication tokens.

    Tokens are generated using ECS task credentials and cached with
    automatic refresh 5 minutes before expiration (tokens valid 15 min).
    """

    _lock = threading.Lock()
    _token: str | None = None
    _expires_at: float = 0

    @staticmethod
    def get_ecs_credentials() -> dict:
        """Get AWS credentials from ECS task metadata endpoint.

        Uses AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI for the credential path
        and ECS_CONTAINER_METADATA_URI for the base URL.

        :returns: AWS credentials dict with AccessKeyId, SecretAccessKey, Token.
        :raises ValueError: If required environment variables are not set.
        """
        relative_uri = os.environ.get("AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI")
        if not relative_uri:
            raise ValueError("AWS_TASK_EXEC_CREDENTIALS_RELATIVE_URI not set")

        metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI")
        if not metadata_uri:
            raise ValueError("ECS_CONTAINER_METADATA_URI not set")

        parsed_uri = urlparse(metadata_uri)
        base_url = f"{parsed_uri.scheme}://{parsed_uri.netloc}"
        credentials_url = f"{base_url}{relative_uri}"

        request = urllib.request.Request(credentials_url)
        no_proxy_handler = urllib.request.ProxyHandler({})
        opener = urllib.request.build_opener(no_proxy_handler)

        with opener.open(request) as response:
            credentials_data = json.loads(response.read().decode("utf-8"))

        return credentials_data

    @staticmethod
    def get_rds_iam_token_hostname() -> str:
        """Get the RDS hostname for IAM token generation.

        For RDS Proxy IAM authentication, tokens must be generated using
        the direct RDS Proxy/Cluster endpoint, not VPC/NLB endpoints.

        :returns: RDS hostname for token generation.
        :raises ValueError: If RDS_IAM_TOKEN_HOSTNAME is not set.
        """
        token_hostname = os.environ.get("RDS_IAM_TOKEN_HOSTNAME", "").strip()
        if token_hostname:
            return token_hostname
        logger.error(
            "RDS_IAM_TOKEN_HOSTNAME not set in environment. "
            "This should be set by the CDK stack to the RDS Cluster/Proxy endpoint."
        )
        raise ValueError("RDS_IAM_TOKEN_HOSTNAME environment variable is required")

    @staticmethod
    def generate_rds_auth_token(
        credentials: dict, hostname: str, port: int, username: str
    ) -> str:
        """Generate RDS IAM authentication token using boto3.

        :param credentials: AWS credentials from ECS task metadata.
        :param hostname: RDS hostname for token generation.
        :param port: Database port number.
        :param username: Database username.
        :returns: RDS IAM authentication token.
        """
        region = os.environ.get("AWS_REGION", "us-west-2")

        rds_client = boto3.client(
            "rds",
            region_name=region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["Token"],
        )

        try:
            auth_token = rds_client.generate_db_auth_token(
                DBHostname=hostname,
                Port=port,
                DBUsername=username,
            )
            return auth_token
        except Exception as e:
            logger.error("Failed to generate RDS auth token: %s", e)
            raise

    @staticmethod
    def generate_credentials() -> str:
        """Generate fresh RDS auth token.

        Orchestrates fetching ECS credentials and generating the RDS token.

        :returns: RDS auth token.
        """
        try:
            credentials = RDSIAMCredentialProvider.get_ecs_credentials()
            iam_token_hostname = RDSIAMCredentialProvider.get_rds_iam_token_hostname()
            auth_token = RDSIAMCredentialProvider.generate_rds_auth_token(
                credentials=credentials,
                hostname=iam_token_hostname,
                port=int(os.environ.get("MWAA__DB__POSTGRES_PORT", "5432")),
                username=DB_IAM_USERNAME,
            )
            return auth_token
        except Exception as e:
            logger.error("Failed to generate credentials: %s", e)
            raise

    @classmethod
    def get_token(cls) -> str:
        """Get cached RDS IAM token, refreshing if expired or missing.

        Uses thread-safe caching with 5-minute refresh buffer before
        the 15-minute token expiration.

        :returns: Cached or freshly generated RDS auth token.
        """
        now = time.time()

        # Refresh token in cache if missing or expires in 5 mins.
        if cls._token is None or now > cls._expires_at - 300:
            with cls._lock:
                if cls._token is None or now > cls._expires_at - 300:
                    cls._token = cls.generate_credentials()
                    cls._expires_at = now + 15 * 60  # 15 mins

        return cls._token

    @classmethod
    def create_db_connection_url(cls, token: str | None = None) -> str:
        """Create a PostgreSQL connection URL for RDS IAM authentication.

        :param token: RDS IAM auth token. If None, generates a fresh token.
        :returns: PostgreSQL connection URL with IAM authentication.
        """
        if token is None:
            token = cls.generate_credentials()

        auth_token = quote_plus(token)
        postgres_host = os.environ.get("MWAA__DB__POSTGRES_HOST", "localhost")
        postgres_port = os.environ.get("MWAA__DB__POSTGRES_PORT", "5432")
        postgres_db = os.environ.get("MWAA__DB__POSTGRES_DB", "AirflowMetadata")
        ssl_mode = os.environ.get("MWAA__DB__POSTGRES_SSLMODE", "require")

        return (
            f"postgresql+psycopg2://{DB_IAM_USERNAME}:{auth_token}"
            f"@{postgres_host}:{postgres_port}/{postgres_db}"
            f"?sslmode={ssl_mode}"
        )


def use_iam_credentials() -> bool:
    """Check if RDS IAM credentials should be used."""
    return os.environ.get("USE_IAM_CREDENTIALS", "").lower() == "true"


def is_local_runner() -> bool:
    """Check if running in local runner mode.

    RDS IAM authentication requires an ECS task metadata endpoint and an RDS
    Proxy, neither of which exist when running the image locally, so the patch
    must not be installed in that case.
    """
    return os.environ.get("MWAA_LOCAL_RUNNER", "").lower() == "true"


def is_using_rds_proxy() -> bool:
    """Check if the environment is using RDS Proxy.

    MWAA__DB__POSTGRES_SSLMODE is set to 'require' only for environments
    using RDS Proxy. RDS IAM authentication is only supported when
    connecting through RDS Proxy.
    """
    ssl_mode = os.environ.get("MWAA__DB__POSTGRES_SSLMODE")
    if ssl_mode is None:
        logger.warning("MWAA__DB__POSTGRES_SSLMODE not set in environment.")
        return False
    return ssl_mode == "require"
