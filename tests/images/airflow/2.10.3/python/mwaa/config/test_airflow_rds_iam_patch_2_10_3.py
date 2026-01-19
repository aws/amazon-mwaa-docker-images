import pytest
from unittest.mock import patch, MagicMock


def test_is_from_migrate_db():
    """Test migrate-db detection"""
    from mwaa.config.airflow_rds_iam_patch import is_from_migrate_db
    
    with patch.dict('os.environ', {'MWAA_AIRFLOW_COMPONENT': 'migrate-db'}):
        assert is_from_migrate_db() is True
    
    with patch.dict('os.environ', {'MWAA_AIRFLOW_COMPONENT': 'scheduler'}):
        assert is_from_migrate_db() is False

    with patch.dict('os.environ', {}, clear=True), patch('mwaa.config.airflow_rds_iam_patch.logger') as mock_logger:
        assert is_from_migrate_db() is False
        mock_logger.error.assert_called_once_with("MWAA_AIRFLOW_COMPONENT does not exist as an environment variable.")


def test_is_using_rds_proxy():
    """Test RDS proxy detection"""
    from mwaa.config.airflow_rds_iam_patch import is_using_rds_proxy
    
    with patch.dict('os.environ', {'SSL_MODE': 'require'}):
        assert is_using_rds_proxy() is True
    
    with patch.dict('os.environ', {'SSL_MODE': 'disable'}):
        assert is_using_rds_proxy() is False

    with patch.dict('os.environ', {}, clear=True), patch('mwaa.config.airflow_rds_iam_patch.logger') as mock_logger:
        assert is_using_rds_proxy() is False
        mock_logger.error.assert_called_once_with("SSL_MODE does not exist as an environment variable.")


def test_is_accessing_metadata_db():
    """Test metadata database detection"""
    from mwaa.config.airflow_rds_iam_patch import is_accessing_metadata_db
    
    # Mock the metadata_url
    with patch('mwaa.config.airflow_rds_iam_patch.metadata_url') as mock_url:
        mock_url.host = 'test.rds.amazonaws.com'
        mock_url.database = 'airflow'
        mock_url.port = 5432
        
        # Test with matching parameters
        cparams = {'host': 'test.rds.amazonaws.com', 'database': 'airflow', 'port': 5432}
        assert is_accessing_metadata_db('postgresql', [], cparams) is True
        
        # Test with non-matching parameters
        cparams = {'host': 'other.rds.amazonaws.com', 'database': 'other', 'port': 3306}
        assert is_accessing_metadata_db('postgresql', [], cparams) is False


def test_is_accessing_metadata_db_with_cargs():
    """Test metadata database detection using cargs"""
    from mwaa.config.airflow_rds_iam_patch import is_accessing_metadata_db
    
    with patch('mwaa.config.airflow_rds_iam_patch.metadata_url') as mock_url:
        mock_url.host = 'test.rds.amazonaws.com'
        mock_url.database = 'airflow'
        mock_url.port = 5432
        
        # Test with matching cargs
        cargs = ['postgresql://user:pass@test.rds.amazonaws.com:5432/airflow']
        assert is_accessing_metadata_db('postgresql', cargs, {}) is True


def test_is_accessing_metadata_db_cargs_exception():
    """Test metadata database detection when cargs parsing fails"""
    from mwaa.config.airflow_rds_iam_patch import is_accessing_metadata_db
    
    with patch('mwaa.config.airflow_rds_iam_patch.metadata_url') as mock_url, \
         patch('mwaa.config.airflow_rds_iam_patch.make_url') as mock_make_url:
        
        mock_url.host = 'test.rds.amazonaws.com'
        mock_url.database = 'airflow'
        mock_url.port = 5432
        
        # Mock make_url to raise an exception when called with cargs
        mock_make_url.side_effect = Exception("Invalid URL")
        
        # Test with cargs that cause exception - should fall back to cparams
        cargs = ['some_url']
        cparams = {'host': 'test.rds.amazonaws.com', 'database': 'airflow', 'port': 5432}
        assert is_accessing_metadata_db('postgresql', cargs, cparams) is True
        
        # Verify make_url was called and raised exception
        mock_make_url.assert_called_once_with('some_url')


def test_event_listener_setup():
    """Test SQLAlchemy event listener setup when conditions are met"""
    with patch.dict('os.environ', {'SSL_MODE': 'require', 'MWAA_AIRFLOW_COMPONENT': 'scheduler'}), \
         patch('mwaa.config.database.get_db_connection_string', return_value='postgresql://test:test@localhost/test'), \
         patch('sqlalchemy.event.listen') as mock_listen:
        
        # Re-import to trigger conditional setup
        import importlib
        import mwaa.config.airflow_rds_iam_patch
        importlib.reload(mwaa.config.airflow_rds_iam_patch)
        
        mock_listen.assert_called()


def test_patch_rds_iam_authentication_function():
    """Test the patch_rds_iam_authentication function execution"""
    with patch.dict('os.environ', {'SSL_MODE': 'require', 'MWAA_AIRFLOW_COMPONENT': 'scheduler'}), \
         patch('mwaa.config.database.get_db_connection_string', return_value='postgresql://test:test@localhost/test'), \
         patch('mwaa.utils.get_rds_iam_credentials.RDSIAMCredentialProvider.get_token', return_value='test_token'), \
         patch('mwaa.utils.get_rds_iam_credentials.RDSIAMCredentialProvider.create_db_connection_url', return_value='postgresql://user:token@host:5432/db?sslmode=require'):
        
        # Re-import to get the patch function
        import importlib
        import mwaa.config.airflow_rds_iam_patch
        importlib.reload(mwaa.config.airflow_rds_iam_patch)
        
        # Get the patch function from the module
        patch_func = getattr(mwaa.config.airflow_rds_iam_patch, 'patch_rds_iam_authentication', None)
        if patch_func:
            # Test the function with metadata DB connection
            cparams = {}
            with patch('mwaa.config.airflow_rds_iam_patch.is_accessing_metadata_db', return_value=True):
                patch_func('postgresql', None, [], cparams)
                # Verify cparams was updated
                assert len(cparams) > 0
            
            # Test the function with non-metadata DB connection (should return early)
            cparams = {}
            with patch('mwaa.config.airflow_rds_iam_patch.is_accessing_metadata_db', return_value=False):
                patch_func('postgresql', None, [], cparams)
                # Verify cparams was not updated
                assert len(cparams) == 0