"""Tests for airflow configuration module."""

import json
import pytest

from mwaa.config.airflow import (
    _get_essential_airflow_executor_config,
    _get_essential_airflow_core_config,
    _get_opinionated_airflow_core_config,
    get_user_airflow_config,
    _get_essential_airflow_db_config,
    _get_opinionated_airflow_db_config,
    _get_essential_airflow_auth_config,
    _get_essential_airflow_api_auth_config,
    _get_essential_aiflow_execution_api_config,
    _get_essential_airflow_logging_config,
    _get_mwaa_cloudwatch_integration_config,
    _get_essential_airflow_scheduler_config,
    _get_opinionated_airflow_scheduler_config,
    _get_opinionated_airflow_secrets_config,
    _get_opinionated_airflow_usage_data_config,
    _get_essential_airflow_webserver_config
)

# ---------------------------
# Executor Config Tests
# ---------------------------

def test_local_executor_config():
    result = _get_essential_airflow_executor_config("LocalExecutor")
    assert result == {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
    }

@pytest.mark.parametrize("executor_name, expected_executor", [
    ("LocalExecutor", "LocalExecutor"),
    ("localexecutor", "LocalExecutor"),
    ("CeleryExecutor", "CeleryExecutor"),
    ("celeryexecutor", "CeleryExecutor"),
])
def test_essential_airflow_executor_config(monkeypatch, executor_name, expected_executor):
    if expected_executor == "LocalExecutor":
        result = _get_essential_airflow_executor_config(executor_name)
        assert result == {"AIRFLOW__CORE__EXECUTOR": expected_executor}
    elif expected_executor == "CeleryExecutor":
         # Mock Celery dependencies
        monkeypatch.setattr("mwaa.config.airflow.get_sqs_endpoint", lambda: "sqs://endpoint")
        monkeypatch.setattr("mwaa.config.airflow.get_sqs_queue_name", lambda: "test-queue")
        monkeypatch.setattr("mwaa.config.airflow.get_db_connection_string", lambda: "postgresql://db")

        result = _get_essential_airflow_executor_config(executor_name)
        assert result["AIRFLOW__CORE__EXECUTOR"] == expected_executor
        assert result["AIRFLOW__CELERY__BROKER_URL"] == "sqs://endpoint"
        assert result["AIRFLOW__OPERATORS__DEFAULT_QUEUE"] == "test-queue"
        assert result["AIRFLOW__CELERY__RESULT_BACKEND"] == "db+postgresql://db"
        assert result["AIRFLOW__CELERY__WORKER_ENABLE_REMOTE_CONTROL"] == "False"
        assert result["AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT"] == "43200"
        assert result["AIRFLOW__CELERY__CELERY_CONFIG_OPTIONS"] == "mwaa.config.celery.MWAA_CELERY_CONFIG"

@pytest.mark.parametrize("invalid_executor", ["UnknownExecutor", "unknownexecutor"])
def test_invalid_executor(invalid_executor):
    with pytest.raises(ValueError):
        _get_essential_airflow_executor_config(invalid_executor)

# ---------------------------
# Core Config Tests
# ---------------------------
def test_core_config_with_api_url_and_fernet(env_helper):
    env_helper.set({
        "MWAA__CORE__FERNET_KEY": json.dumps({"FernetKey": "test-key"}),
        "MWAA__CORE__API_SERVER_URL": "api.example.com"
    })

    result = _get_essential_airflow_core_config()

    assert result["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"
    assert result["AIRFLOW__CORE__FERNET_KEY"] == "test-key"
    assert result["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] == \
        "https://api.example.com/execution"
    
def test_core_config_invalid_fernet(env_helper):
    env_helper.set({
        "MWAA__CORE__FERNET_KEY": "invalid-json"
    })

    result = _get_essential_airflow_core_config()

    assert "AIRFLOW__CORE__FERNET_KEY" not in result


# ---------------------------------
# Opinion Airflow Core Config Tests
# ---------------------------------
def test_get_opinionated_airflow_core_config():
    result = _get_opinionated_airflow_core_config()

    assert result["AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER"] == "True"

# ---------------------------------
# User Airflow Config Tests
# ---------------------------------
def test_get_user_airflow_config_valid_json(env_helper):
    user_defined_config = {
        "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "false",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }

    env_helper.set({
        "MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS": json.dumps(user_defined_config)
    })

    result = get_user_airflow_config()

    assert result == user_defined_config


def test_get_user_airflow_config_invalid_json(env_helper):
    env_helper.set({
        "MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS": '{invalid_json}'
    })

    result = get_user_airflow_config()

    # Should fail silently and return empty dict
    assert result == {}

def test_get_user_airflow_config_env_not_set():
    result = get_user_airflow_config()

    assert result == {}

def test_get_user_airflow_config_empty_string(monkeypatch):
    monkeypatch.setenv("MWAA__CORE__CUSTOM_AIRFLOW_CONFIGS", "")

    result = get_user_airflow_config()

    # Empty string behaves like not set
    assert result == {}

# ---------------------------------
# DB Config Tests
# ---------------------------------
def test_db_config(monkeypatch):
    monkeypatch.setattr(
        'mwaa.config.airflow.get_db_connection_string', lambda: "postgres://db"
    )
    result = _get_essential_airflow_db_config()
    assert result == {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgres://db"
    }


# ---------------------------------
# Opinionated Airflow DB Config Tests
# ---------------------------------
def test_get_opinionated_airflow_db_config():
    result = _get_opinionated_airflow_db_config()
    assert result == {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS": "mwaa.config.database.MWAA_CONNECT_ARGS"
    }

# ---------------------------------
# Auth Config Tests (Parametrized)
# ---------------------------------
@pytest.mark.parametrize(
    "auth_env, expected_config",
    [
        (
            "mwaa-iam",
            {
                "AIRFLOW__CORE__AUTH_MANAGER":
                    "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            },
        ),
        (
            "MWAA-IAM",
            {
                "AIRFLOW__CORE__AUTH_MANAGER":
                    "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            },
        ),
        (
            "",
            {
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin,viewer:viewer",
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
            },
        ),
        (
            None,
            {
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin,viewer:viewer",
                "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "True",
            },
        ),
    ],
)
def test_get_essential_airflow_auth_config(env_helper, auth_env, expected_config):
    if auth_env is not None:
        env_helper.set({"MWAA__CORE__AUTH_TYPE": auth_env})

    result = _get_essential_airflow_auth_config()

    assert result == expected_config


# --------------------------------------------
# Essential Airflow API Auth Config Tests 
# --------------------------------------------
def test_get_essential_airflow_api_auth_config(env_helper):
    env_helper.set({
        "MWAA__CORE__FERNET_KEY": json.dumps({"FernetKey": "test-key"}),
    })

    result = _get_essential_airflow_api_auth_config()

    assert result["AIRFLOW__API_AUTH__JWT_SECRET"] == '{"FernetKey": "test-key"}'
    assert result["AIRFLOW__API_AUTH__JWT_ALGORITHM"] == "HS256"


# --------------------------------------------
# Essential Airflow Execution API Config Tests 
# --------------------------------------------
def test_get_essential_aiflow_execution_api_config():
    result = _get_essential_aiflow_execution_api_config()

    assert result["AIRFLOW__EXECUTION_API__JWT_EXPIRATION_TIME"] == "86400"

# --------------------------------------------
# Essential Airflow Logging Config Tests 
# --------------------------------------------
def test_get_essential_airflow_logging_config():
    expected_logging_config = {
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS": "mwaa.logging.config.LOGGING_CONFIG",
        "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG": "False"
    }
    result = _get_essential_airflow_logging_config()

    assert result == expected_logging_config


# --------------------------------------
# CloudWatch Integration Config Tests
# --------------------------------------
@pytest.mark.parametrize("enabled_value", ["false", "False", "FALSE", "", None])
def test_get_mwaa_cloudwatch_integration_config_disabled(env_helper, enabled_value):
    _get_mwaa_cloudwatch_integration_config.cache_clear()
    if enabled_value is not None:
        env_helper.set({"MWAA__CLOUDWATCH_METRICS_INTEGRATION__ENABLED": enabled_value})
    
    result = _get_mwaa_cloudwatch_integration_config()
    
    assert result == {}
    
def test_cloudwatch_metrics_missing_section(monkeypatch):
    from mwaa.config import airflow

    _get_mwaa_cloudwatch_integration_config.cache_clear()

    monkeypatch.setenv(
        "MWAA__CLOUDWATCH_METRICS_INTEGRATION__ENABLED",
        "true",
    )

    monkeypatch.setattr(airflow.conf, "getsection", lambda section: None)

    with pytest.raises(RuntimeError):
        _get_mwaa_cloudwatch_integration_config()

def test_cloudwatch_metrics_enabled(env_helper, monkeypatch):
    from mwaa.config import airflow

    _get_mwaa_cloudwatch_integration_config.cache_clear()

    env_helper.set({"MWAA__CLOUDWATCH_METRICS_INTEGRATION__ENABLED": "true"})

    # Mock metrics section
    monkeypatch.setattr(
        airflow.conf,
        "getsection",
        lambda section: {"statsd_on": "False", "statsd_host": "x", "option1": "value1"},
    )

    monkeypatch.setattr(
        airflow.conf,
        "get_default_value",
        lambda section, option: "default",
    )

    result = _get_mwaa_cloudwatch_integration_config()

    assert result["AIRFLOW__METRICS__STATSD_ON"] == "True"  # forced override
    assert result["AIRFLOW__METRICS__STATSD_HOST"] == "localhost"
    assert result["AIRFLOW__METRICS__STATSD_PORT"] == "8125"
    assert result["AIRFLOW__METRICS__STATSD_PREFIX"] == "airflow"
    assert result["AIRFLOW__METRICS__OPTION1"] == "default"

def test_cloudwatch_metrics_customer_config_write(monkeypatch, env_helper, tmp_path):
    from mwaa.config import airflow

    _get_mwaa_cloudwatch_integration_config.cache_clear()

    env_helper.set({
        "MWAA__CLOUDWATCH_METRICS_INTEGRATION__ENABLED": "true",
        "MWAA__CLOUDWATCH_METRICS_INTEGRATION__CUSTOMER_CONFIG_PATH": str(tmp_path),
    })

    monkeypatch.setattr(
        airflow.conf,
        "getsection",
        lambda section: {"statsd_on": "False"},
    )

    monkeypatch.setattr(
        airflow.conf,
        "get_default_value",
        lambda section, option: "default",
    )

    monkeypatch.setattr(
        airflow,
        "get_user_airflow_config",
        lambda: {
            "AIRFLOW__METRICS__STATSD_ON": "False",
        },
    )

    _get_mwaa_cloudwatch_integration_config()

    # Ensure files were written
    assert (tmp_path / "statsd_on.txt").exists()
    assert (tmp_path / "metrics_block_list.txt").exists()
    assert (tmp_path / "metrics_allow_list.txt").exists()

# --------------------------------------------
# Essential Airflow Scheduler Config Tests 
# --------------------------------------------
def test_get_essential_airflow_scheduler_config():
    result = _get_essential_airflow_scheduler_config()
    
    assert result == {
        "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR": "True",
    }


# --------------------------------------------
# Opinionated Airflow Scheduler Config Tests 
# --------------------------------------------
def test_get_opinionated_airflow_scheduler_config():
    result = _get_opinionated_airflow_scheduler_config()
    
    assert result == {
        "AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION": "False",
        "AIRFLOW__SCHEDULER__TASK_QUEUED_TIMEOUT": "1800.0",
    }


# --------------------------------------------
# Opinionated Airflow Secrets Config Tests 
# --------------------------------------------
def test_get_opinionated_airflow_secrets_config():
    result = _get_opinionated_airflow_secrets_config()
    
    expected_pattern = {"connections_lookup_pattern": "^(?!aws_default$).*$"}
    expected_config = {
        "AIRFLOW__SECRETS__BACKEND_KWARGS": json.dumps(expected_pattern),
        "AIRFLOW__WORKERS__SECRETS_BACKEND_KWARGS": json.dumps(expected_pattern),
    }
    
    assert result == expected_config


# --------------------------------------------
# Opinionated Airflow Usage Data Config Tests 
# --------------------------------------------
def test_get_opinionated_airflow_usage_data_config():
    result = _get_opinionated_airflow_usage_data_config()
    
    assert result == {
        "AIRFLOW__USAGE_DATA_COLLECTION__ENABLED": "False",
    }

# --------------------------------------------
# Webserver Config Tests 
# --------------------------------------------
def test_webserver_config_no_secret():
    result = _get_essential_airflow_webserver_config()

    assert result == {
        "AIRFLOW__FAB__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
    }

def test_webserver_config_valid_secret(env_helper):
    env_helper.set({"MWAA__WEBSERVER__SECRET":  json.dumps({"secret_key": "super-secret"})})

    result = _get_essential_airflow_webserver_config()

    assert result == {
        "AIRFLOW__FAB__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
        "AIRFLOW__API__SECRET_KEY": "super-secret",
    }

def test_webserver_config_invalid_json(env_helper):
    env_helper.set({"MWAA__WEBSERVER__SECRET": "{invalid-json}"})

    result = _get_essential_airflow_webserver_config()

    assert result == {
        "AIRFLOW__FAB__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
    }

def test_webserver_config_missing_secret_key(env_helper):
    env_helper.set({"MWAA__WEBSERVER__SECRET":  json.dumps({"wrong_key": "value"})})

    result = _get_essential_airflow_webserver_config()

    # KeyError is swallowed by broad except
    assert result == {
        "AIRFLOW__FAB__CONFIG_FILE": "/python/mwaa/webserver/webserver_config.py",
    }

# ---------------------------
# Final Merge Tests
# ---------------------------
def test_get_essential_airflow_config_merges_all(monkeypatch):
    from mwaa.config import airflow
    # Clear cached cloudwatch if needed
    if hasattr(airflow._get_mwaa_cloudwatch_integration_config, "cache_clear"):
        airflow._get_mwaa_cloudwatch_integration_config.cache_clear()

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_executor_config",
        lambda executor: {"EXECUTOR": executor},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_core_config",
        lambda: {"CORE": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_db_config",
        lambda: {"DB": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_logging_config",
        lambda: {"LOGGING": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_mwaa_cloudwatch_integration_config",
        lambda: {"CLOUDWATCH": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_scheduler_config",
        lambda: {"SCHEDULER": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_webserver_config",
        lambda: {"WEBSERVER": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_auth_config",
        lambda: {"AUTH": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_airflow_api_auth_config",
        lambda: {"API_AUTH": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_essential_aiflow_execution_api_config",
        lambda: {"EXEC_API": "1"},
    )

    result = airflow.get_essential_airflow_config("CeleryExecutor")

    assert result == {
        "EXECUTOR": "CeleryExecutor",
        "CORE": "1",
        "DB": "1",
        "LOGGING": "1",
        "CLOUDWATCH": "1",
        "SCHEDULER": "1",
        "WEBSERVER": "1",
        "AUTH": "1",
        "API_AUTH": "1",
        "EXEC_API": "1",
    }

def test_get_opinionated_airflow_config_merges(monkeypatch):
    from mwaa.config import airflow
    
    monkeypatch.setattr(
        airflow,
        "_get_opinionated_airflow_core_config",
        lambda: {"CORE": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_opinionated_airflow_scheduler_config",
        lambda: {"SCHEDULER": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_opinionated_airflow_secrets_config",
        lambda: {"SECRETS": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_opinionated_airflow_usage_data_config",
        lambda: {"USAGE": "1"},
    )

    monkeypatch.setattr(
        airflow,
        "_get_opinionated_airflow_db_config",
        lambda: {"DB": "1"},
    )

    result = airflow.get_opinionated_airflow_config()

    assert result == {
        "CORE": "1",
        "SCHEDULER": "1",
        "SECRETS": "1",
        "USAGE": "1",
        "DB": "1",
    }