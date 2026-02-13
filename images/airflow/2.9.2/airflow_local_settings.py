"""Airflow local settings configuration for MWAA RDS IAM authentication."""
import logging

logger = logging.getLogger(__name__)

try:
    import mwaa.config.airflow_rds_iam_patch  # type: ignore[import-untyped]
except Exception as e:
    logger.error(f"Failed to load RDS IAM patch: {e}")
    raise

# Load ${AIRFLOW_HOME}/dags/airflow_local_settings.py and ${AIRFLOW_HOME}/plugins/airflow_local_settings.py if it exists
import os
import subprocess

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')
airflow_config_dir = os.path.join(AIRFLOW_HOME, 'config')
dags_airflow_local_settings_path = os.path.join(AIRFLOW_HOME, 'dags', 'airflow_local_settings.py')
plugins_airflow_local_settings_path = os.path.join(AIRFLOW_HOME, 'plugins', 'airflow_local_settings.py')


def _copy_dags_airflow_local_settings():
    """Copy customer's dags/airflow_local_settings.py to config folder."""
    # Copy the customer's dags/airflow_local_settings.py to the config folder to allow for python imports
    dest_config_airflow_local_settings_path = os.path.join(airflow_config_dir, 'dags_airflow_local_settings.py')

    if os.path.exists(dags_airflow_local_settings_path):
        try:
            subprocess.run(["cp", dags_airflow_local_settings_path, dest_config_airflow_local_settings_path], check=True)
        except Exception as err:
            logger.error(f"Error copying airflow_local_settings.py to config folder: {err}")
            raise err
    else:
        if os.path.exists(dest_config_airflow_local_settings_path):
            try:
                subprocess.run(["rm", "-f", dest_config_airflow_local_settings_path], check=True)
            except Exception as err:
                logger.error(f"Error removing dags_airflow_local_settings.py: {err}")

def load_dags_airflow_local_settings():
    """Load customer's airflow_local_settings.py from dags folder."""
    # Copy the customer's /dags/airflow_local_settings.py to /config/dags_airflow_local_settings.py
    try:
        _copy_dags_airflow_local_settings()
    except Exception as e:
        logger.error(f"Failed to copy dags/airflow_local_settings.py to config folder: {e}")
        raise

    # Check if the dags airflow_local_settings.py exists
    if os.path.exists(dags_airflow_local_settings_path):
        # load /config/dags_airflow_local_settings.py if it exists
        try:
            import dags_airflow_local_settings
            logger.info("Successfully imported airflow_local_settings from dags folder")
        except Exception as e:
            logger.error(f"Failed to import airflow_local_settings from {dags_airflow_local_settings_path}: {e}")
            raise

def _copy_plugins_airflow_local_settings():
    """Copy customer's plugins/airflow_local_settings.py to config folder."""
    dest_config_airflow_local_settings_path = os.path.join(airflow_config_dir, 'plugins_airflow_local_settings.py')

    if os.path.exists(plugins_airflow_local_settings_path):
        try:
            subprocess.run(["cp", plugins_airflow_local_settings_path, dest_config_airflow_local_settings_path], check=True)
        except Exception as err:
            logger.error(f"Error copying airflow_local_settings.py to config folder: {err}")
            raise err
    else:
        if os.path.exists(dest_config_airflow_local_settings_path):
            try:
                subprocess.run(["rm", "-f", dest_config_airflow_local_settings_path], check=True)
            except Exception as err:
                logger.error(f"Error removing plugins_airflow_local_settings.py: {err}")

def load_plugins_airflow_local_settings():
    """Load customer's airflow_local_settings.py from plugins folder."""
    try:
        _copy_plugins_airflow_local_settings()
    except Exception as e:
        logger.error(f"Failed to copy plugins/airflow_local_settings.py to config folder: {e}")
        raise

    if os.path.exists(plugins_airflow_local_settings_path):
        try:
            import plugins_airflow_local_settings
            logger.info("Successfully imported airflow_local_settings from plugins folder")
        except Exception as e:
            logger.error(f"Failed to import airflow_local_settings from {plugins_airflow_local_settings_path}: {e}")
            raise

load_dags_airflow_local_settings()
load_plugins_airflow_local_settings()