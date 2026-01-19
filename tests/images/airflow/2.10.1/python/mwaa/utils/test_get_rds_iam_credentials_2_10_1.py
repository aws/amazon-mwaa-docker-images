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
    patch('urllib.request.urlopen') as mock_urlopen:
        
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(mock_credentials).encode('utf-8')
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
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
         patch.object(RDSIAMCredentialProvider, 'generate_rds_auth_token', return_value='auth_token'), \
         patch('mwaa.utils.get_rds_iam_credentials.logger') as mock_logger:
        
        result = RDSIAMCredentialProvider.generate_credentials()
        assert result == 'auth_token'
        mock_logger.info.assert_called_with(f"Successfully generated RDS auth token at {time.strftime('%Y-%m-%d %H:%M:%S')}")


def test_generate_credentials_failure():
    """Test credential generation failure"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.object(RDSIAMCredentialProvider, 'get_ecs_credentials', side_effect=Exception("ECS error")), \
         patch('mwaa.utils.get_rds_iam_credentials.logger') as mock_logger:
        
        result = RDSIAMCredentialProvider.generate_credentials()
        assert result is None
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
    """Test database connection URL creation with token generation"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.dict('os.environ', {
        'POSTGRES_HOST': 'test.rds.amazonaws.com',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'airflow',
        'SSL_MODE': 'require'
    }), \
    patch.object(RDSIAMCredentialProvider, 'generate_credentials', return_value='generated_token'):
        
        result = RDSIAMCredentialProvider.create_db_connection_url()
        assert 'postgresql+psycopg2://airflow_user:generated_token@test.rds.amazonaws.com:5432/airflow?sslmode=require' in result


def test_create_db_connection_url_generation_failure():
    """Test database connection URL creation when token generation fails"""
    from mwaa.utils.get_rds_iam_credentials import RDSIAMCredentialProvider
    
    with patch.object(RDSIAMCredentialProvider, 'generate_credentials', return_value=None):
        with pytest.raises(Exception, match="Failed to generate RDS auth token"):
            RDSIAMCredentialProvider.create_db_connection_url()
