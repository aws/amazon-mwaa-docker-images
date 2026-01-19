"""RDS IAM credential provider for generating authentication tokens."""
import json
import logging
import os
import sys
import threading
import time
import urllib.request
from urllib.parse import quote_plus

import boto3

logger = logging.getLogger(__name__)

class RDSIAMCredentialProvider:
    """Provider for RDS IAM authentication tokens with thread-safe caching."""
    _lock = threading.Lock()
    _token = None
    _expires_at = 0

    @staticmethod
    def get_ecs_credentials():
        """
        Get AWS credentials from ECS task metadata endpoint.
        
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

        with urllib.request.urlopen(credentials_url) as response:
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
        Generate fresh RDS auth token and store in memory

        Returns:
            str or None: RDS auth token if successful, None if generation fails
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
            
            logger.info(f"Successfully generated RDS auth token at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            return auth_token

        except Exception as e:
            logger.error(f"Failed to update credentials: {e}")
            return None

    @classmethod
    def get_token(cls):
        """
        Get cached RDS IAM token, refreshing if expired or missing.
        Uses thread-safe caching with 5-minute refresh buffer before expiration.
        
        Returns:
            str: Cached or freshly generated RDS auth token
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
    def create_db_connection_url(cls, token=None):
        """
        Create a PostgreSQL connection URL for RDS IAM authentication.
        
        Args:
            token (str, optional): RDS IAM auth token. If None, generates a fresh token.
        
        Returns:
            str: PostgreSQL connection URL with IAM authentication
        """
        if token is None:
            token = cls.generate_credentials()
            if token is None:
                raise Exception("Failed to generate RDS auth token")

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