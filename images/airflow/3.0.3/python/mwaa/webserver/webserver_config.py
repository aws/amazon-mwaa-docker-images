"""Configuration for the Airflow webserver."""

import os

from airflow.configuration import conf

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = conf.get_mandatory_value("database", "SQL_ALCHEMY_CONN")

# Flask-WTF flag for CSRF
CSRF_ENABLED = True

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = False if os.environ.get("MWAA__WEBSERVER__WTF_CSRF_ENABLED", "").lower() == "false" else True

if os.environ.get("MWAA__CORE__AUTH_TYPE", "").lower() == "mwaa-iam":
    # The auth type is IAM. This is a MWAA-specific type, which relies on a plugin
    # defined in MWAA's sidecar.
    from aws_mwaa.iam import IamSecurityManager  # type: ignore
    from flask_appbuilder.security.manager import AUTH_REMOTE_USER

    AUTH_TYPE = AUTH_REMOTE_USER
    SECURITY_MANAGER_CLASS = IamSecurityManager
else:
    AUTH_TYPE = None
