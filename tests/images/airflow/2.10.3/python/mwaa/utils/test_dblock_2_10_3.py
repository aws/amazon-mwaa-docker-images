import pytest
from unittest.mock import patch, MagicMock


def test_connect_with_retry_uses_static_credentials_when_iam_disabled():
    """Test that _connect_with_retry uses get_db_connection_string when IAM is disabled."""
    from mwaa.utils.dblock import _connect_with_retry

    with patch('mwaa.config.airflow_rds_iam_patch.use_iam_credentials', return_value=False), \
         patch('mwaa.utils.dblock.create_engine') as mock_create_engine, \
         patch('mwaa.utils.dblock.get_db_connection_string', return_value='postgresql://static/airflow'):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn

        result = _connect_with_retry()

        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == 'postgresql://static/airflow'
        assert result == mock_conn


def test_connect_with_retry_uses_iam_credentials_when_enabled():
    """Test that _connect_with_retry uses RDSIAMCredentialProvider when IAM is enabled."""
    from mwaa.utils.dblock import _connect_with_retry

    with patch('mwaa.config.airflow_rds_iam_patch.use_iam_credentials', return_value=True), \
         patch('mwaa.utils.get_rds_iam_credentials.RDSIAMCredentialProvider') as mock_provider, \
         patch('mwaa.utils.dblock.create_engine') as mock_create_engine, \
         patch('mwaa.utils.dblock.get_db_connection_string', return_value='postgresql://static/airflow'):
        mock_provider.get_token.return_value = 'iam-token-123'
        mock_provider.create_db_connection_url.return_value = 'postgresql://airflow_user:iam-token-123@host/db'
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn

        result = _connect_with_retry()

        mock_provider.get_token.assert_called_once()
        mock_provider.create_db_connection_url.assert_called_once_with('iam-token-123')
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args
        assert call_args[0][0] == 'postgresql://airflow_user:iam-token-123@host/db'
        assert result == mock_conn
