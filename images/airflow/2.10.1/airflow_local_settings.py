"""Airflow local settings configuration for MWAA RDS IAM authentication."""
try:
    import mwaa.config.airflow_rds_iam_patch  # type: ignore[import-untyped]
except Exception as e:
    print(f"Failed to load RDS IAM patch: {e}")
    raise