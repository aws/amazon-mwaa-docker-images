try:
    import mwaa.config.airflow_rds_iam_patch
except Exception as e:
    print(f"Failed to load RDS IAM patch: {e}")
    raise