"""Airflow local settings configuration for MWAA RDS IAM authentication."""
import logging

logger = logging.getLogger(__name__)

try:
    import mwaa.config.airflow_rds_iam_patch  # type: ignore[import-untyped]
except Exception as e:
    logger.error(f"Failed to load RDS IAM patch: {e}")
    raise