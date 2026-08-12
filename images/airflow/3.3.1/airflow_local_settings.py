"""Airflow local settings configuration for MWAA RDS IAM authentication.

Airflow imports this module during ``airflow.settings.initialize()``, which runs
unconditionally on ``import airflow``. ``airflow.settings`` appends
``$AIRFLOW_HOME/config`` to ``sys.path`` before doing so, which is where the
Dockerfile installs this file.

This is the only correct place to install the RDS IAM credential rotation patch:
the patch registers a SQLAlchemy ``do_connect`` listener, and that listener lives
in the process that registered it. The Airflow components (scheduler, worker,
api-server, dag-processor, triggerer and task runners) are spawned by the
entrypoint as separate ``subprocess.Popen(["airflow", ...])`` children, each a
fresh Python interpreter. Installing the patch from the entrypoint alone would
therefore leave every process that actually talks to the metadata database
unpatched.
"""

import importlib.util
import logging
import os

# Airflow copies every public name of this module into the `airflow.settings`
# namespace. Declaring an empty `__all__` keeps our internals from clobbering
# Airflow's own globals; we export no policies or settings overrides.
__all__: list[str] = []

_logger = logging.getLogger(__name__)

try:
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    install_rds_iam_patch()
except Exception as e:
    _logger.error(f"Failed to load RDS IAM patch: {e}")
    raise

# Load ${AIRFLOW_HOME}/dags/airflow_local_settings.py and
# ${AIRFLOW_HOME}/plugins/airflow_local_settings.py if they exist, so that
# customer-provided local settings continue to be honoured.
_AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
_CUSTOMER_LOCAL_SETTINGS = [
    (
        "customer_dags_airflow_local_settings",
        os.path.join(_AIRFLOW_HOME, "dags", "airflow_local_settings.py"),
    ),
    (
        "customer_plugins_airflow_local_settings",
        os.path.join(_AIRFLOW_HOME, "plugins", "airflow_local_settings.py"),
    ),
]

for _module_name, _path in _CUSTOMER_LOCAL_SETTINGS:
    if not os.path.exists(_path):
        continue
    try:
        _spec = importlib.util.spec_from_file_location(_module_name, _path)
        if _spec and _spec.loader:
            _module = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_module)
    except Exception as e:
        _logger.error(f"Failed to load airflow_local_settings from {_path}: {e}")
        raise
