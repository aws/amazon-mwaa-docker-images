"""Airflow local settings configuration for MWAA RDS IAM authentication."""
import logging
import os
import importlib.util

logger = logging.getLogger(__name__)

try:
    import mwaa.config.airflow_rds_iam_patch  # type: ignore[import-untyped]
except Exception as e:
    logger.error(f"Failed to load RDS IAM patch: {e}")
    raise

# Load ${AIRFLOW_HOME}/dags/airflow_local_settings.py and ${AIRFLOW_HOME}/plugins/airflow_local_settings.py if it exists
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
dags_airflow_local_settings_path = os.path.join(AIRFLOW_HOME, 'dags', 'airflow_local_settings.py')
plugins_airflow_local_settings_path = os.path.join(AIRFLOW_HOME, 'plugins', 'airflow_local_settings.py')

# Load dags/airflow_local_settings.py if it exists
if os.path.exists(dags_airflow_local_settings_path):
    try:
        spec = importlib.util.spec_from_file_location("customer_dags_airflow_local_settings", dags_airflow_local_settings_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
    except Exception as e:
        logger.error(f"Failed to load airflow_local_settings from {dags_airflow_local_settings_path}: {e}")
        raise

# Load plugins/airflow_local_settings.py if it exists
if os.path.exists(plugins_airflow_local_settings_path):
    try:
        spec = importlib.util.spec_from_file_location("customer_plugins_airflow_local_settings", plugins_airflow_local_settings_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
    except Exception as e:
        logger.error(f"Failed to load airflow_local_settings from {plugins_airflow_local_settings_path}: {e}")
        raise
