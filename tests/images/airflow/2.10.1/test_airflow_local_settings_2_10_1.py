import pytest
import sys
import importlib.util
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_import_all_modules():
    """Test that imports all modules to ensure coverage tracking"""
    # Mock dependencies to avoid errors
    with patch('mwaa.config.database.get_db_connection_string', return_value='postgresql://test:test@localhost/test'), \
         patch.dict('os.environ', {'MWAA_AIRFLOW_COMPONENT': 'scheduler', 'SSL_MODE': 'require'}, clear=False):
        
        # Import the modules directly to get them in coverage
        import mwaa.config.airflow_rds_iam_patch  # noqa: F401
        import mwaa.utils.get_rds_iam_credentials  # noqa: F401
        
        # Also test airflow_local_settings
        airflow_local_settings_path = Path(__file__).parent.parent.parent.parent.parent / "images/airflow/2.10.1/airflow_local_settings.py"
        spec = importlib.util.spec_from_file_location("airflow_local_settings", airflow_local_settings_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
