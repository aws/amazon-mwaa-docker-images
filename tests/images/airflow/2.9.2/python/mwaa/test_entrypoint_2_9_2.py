# test_entrypoint.py
import pytest
import os
import sys
from unittest.mock import patch, MagicMock, mock_open, AsyncMock
from botocore.exceptions import ClientError

import mwaa.entrypoint as entrypoint
from mwaa.entrypoint import (
    _setup_console_log_level,
    _configure_root_logger,
    airflow_db_migrate,
    increase_pool_size_if_insufficient,
    create_airflow_user,
    create_queue,
    main
)


# ------------------------
# Fixtures
# ------------------------
@pytest.fixture
def mock_environ(monkeypatch):
    """Basic environment variables fixture"""
    env_vars = {
        "MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL": "INFO",
        "MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL": "INFO",
        "MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL": "INFO",
        "MWAA__CORE__EXECUTOR_TYPE": "CeleryExecutor",
        "MWAA__CORE__AUTH_TYPE": "testing",
        "MWAA__CORE__CREATED_AT": "Mon Sep 11 00:00:00 UTC 2024",
        "MWAA__CORE__TESTING_MODE": "true",
        # Database configuration
        "MWAA__DB__POSTGRES_HOST": "localhost",
        "MWAA__DB__POSTGRES_PORT": "5432",
        "MWAA__DB__POSTGRES_DB": "airflow",
        "MWAA__DB__POSTGRES_USER": "airflow",
        "MWAA__DB__POSTGRES_PASSWORD": "airflow",
        "MWAA__DB__POSTGRES_SSLMODE": "disable",
        "PYTHONPATH": os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    }
    monkeypatch.setattr(os, "environ", env_vars)
    return env_vars


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 client for SQS operations"""
    with patch('boto3.client') as mock_client:
        yield mock_client


@pytest.fixture
def mock_db_utils():
    """Mock database utilities"""
    with patch('mwaa.utils.dblock.create_engine') as mock_engine, \
            patch('mwaa.utils.dblock.get_db_connection_string', return_value='postgresql://localhost/airflow'):
        engine = MagicMock()
        mock_engine.return_value = engine
        yield engine


# ------------------------
# Test Cases
# ------------------------

@pytest.mark.parametrize("command,expected_level", [
    ("scheduler", "INFO"),
    ("worker", "INFO"),
    ("webserver", "INFO"),
    ("unknown", "INFO"),
])
def test_setup_console_log_level(command, expected_level, mock_environ):
    """Test console log level setup"""
    with patch.dict(os.environ, mock_environ, clear=True):
        _setup_console_log_level(command)
        assert os.environ['AIRFLOW_CONSOLE_LOG_LEVEL'] == expected_level


def test_configure_root_logger(mock_environ):
    """Test root logger configuration"""
    with patch('logging.config.dictConfig') as mock_dict_config, \
            patch.dict(os.environ, mock_environ, clear=True):
        _configure_root_logger("scheduler")
        mock_dict_config.assert_called_once()


@pytest.mark.asyncio
async def test_airflow_db_migrate(mock_db_utils):
    """Test Airflow database initialization"""
    environ = {"PYTHONPATH": os.environ.get("PYTHONPATH", "")}

    async def mock_run_command(cmd, env=None):
        return 0

    with patch('mwaa.entrypoint.run_command', side_effect=mock_run_command) as mock_cmd:
        await airflow_db_migrate(environ)
        mock_cmd.assert_called_once_with(
            "python3 -m mwaa.database.migrate_with_downgrade",
            env=environ
        )


@pytest.fixture
def mock_run_command():
    """Mock run_command with AsyncMock"""

    async def mock_run(*args, **kwargs):
        if 'stdout_logging_method' in kwargs and 'airflow pools get default_pool' in args[0]:
            kwargs['stdout_logging_method']("128")
        return 0

    with patch('mwaa.entrypoint.run_command') as mock:
        mock.side_effect = mock_run
        yield mock


@pytest.mark.asyncio
async def test_increase_pool_size_if_insufficient(mock_environ):
    """Test pool size increase functionality"""

    called_commands = []

    async def mock_run(cmd, env=None, stdout_logging_method=None):
        called_commands.append(cmd)
        if stdout_logging_method and "airflow pools get default_pool" in cmd:
            stdout_logging_method("4000")  # Simulate the default pool size
        return 0

    with patch('mwaa.entrypoint.run_command', new_callable=AsyncMock, side_effect=mock_run) as mock_cmd, \
            patch('mwaa.entrypoint.get_statsd') as mock_statsd:
        mock_stats = MagicMock()
        mock_statsd.return_value = mock_stats

        await increase_pool_size_if_insufficient(mock_environ)

        assert len(called_commands) == 2, f"Expected 2 commands, got {len(called_commands)}: {called_commands}"

        assert "airflow pools get default_pool" in called_commands[0], \
            f"First command should be get pool, got: {called_commands[0]}"

        assert "airflow pools set default_pool 10000" in called_commands[1], \
            f"Second command should be set pool, got: {called_commands[1]}"

        mock_stats.incr.assert_called_once_with("mwaa.pool.increased_default_pool_size", 1)


@pytest.mark.asyncio
async def test_create_airflow_user(mock_db_utils, mock_environ):
    """Test Airflow user creation"""

    async def mock_run_command(cmd, env=None):
        return 0

    with patch('mwaa.entrypoint.run_command', side_effect=mock_run_command) as mock_cmd:
        await create_airflow_user(mock_environ)
        assert mock_cmd.called
        assert "airflow users create" in mock_cmd.call_args[0][0]


def test_create_queue_when_not_required(mock_environ, mock_db_utils):
    """Test queue creation when not required"""
    with patch('mwaa.entrypoint.should_create_queue', return_value=False):
        create_queue()


def test_create_existing_queue(mock_environ, mock_db_utils, mock_boto3_client):
    """Test handling of existing queue"""
    with patch('mwaa.entrypoint.should_create_queue', return_value=True), \
            patch('mwaa.entrypoint.get_sqs_queue_name', return_value='test-queue'):
        mock_sqs = MagicMock()
        mock_boto3_client.return_value = mock_sqs
        mock_sqs.get_queue_url.return_value = {"QueueUrl": "test-url"}

        create_queue()

        mock_sqs.get_queue_url.assert_called_once_with(QueueName='test-queue')
        mock_sqs.create_queue.assert_not_called()


def test_create_new_queue(mock_environ, mock_db_utils, mock_boto3_client):
    """Test creation of new queue"""
    with patch('mwaa.entrypoint.should_create_queue', return_value=True), \
            patch('mwaa.entrypoint.get_sqs_queue_name', return_value='test-queue'):
        mock_sqs = MagicMock()
        mock_boto3_client.return_value = mock_sqs
        mock_sqs.get_queue_url.side_effect = ClientError(
            {'Error': {'Message': 'The specified queue does not exist.'}},
            'GetQueueUrl'
        )

        create_queue()

        mock_sqs.create_queue.assert_called_once_with(QueueName='test-queue')


@pytest.mark.asyncio
async def test_main_valid_command(mock_environ, mock_db_utils):
    """Test main function with valid command"""
    test_args = ['script.py', 'scheduler']

    async def mock_run_command(cmd, env=None):
        return 0

    with patch.dict(os.environ, mock_environ), \
            patch.object(sys, 'argv', test_args), \
            patch('mwaa.entrypoint.run_command', side_effect=mock_run_command), \
            patch('mwaa.entrypoint.setup_environment_variables') as mock_setup_env, \
            patch('mwaa.entrypoint.install_user_requirements') as mock_install_req, \
            patch('mwaa.entrypoint.create_queue') as mock_create_queue, \
            patch('mwaa.entrypoint.execute_command') as mock_execute:
        mock_setup_env.return_value = mock_environ
        mock_install_req.return_value = None

        await main()

        mock_setup_env.assert_called_once()
        mock_install_req.assert_called_once()
        mock_create_queue.assert_called_once()
        mock_execute.assert_called_once()


@pytest.mark.asyncio
async def test_main_invalid_command():
    """Test main function with invalid command"""
    test_args = ['script.py', 'invalid']
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):
            await main()


@pytest.mark.asyncio
async def test_main_test_requirements(mock_environ, mock_db_utils):
    """Test main function with test-requirements command"""
    test_args = ['script.py', 'test-requirements']
    with patch.dict(os.environ, mock_environ), \
            patch.object(sys, 'argv', test_args), \
            patch('mwaa.entrypoint.setup_environment_variables') as mock_setup_env, \
            patch('mwaa.entrypoint.install_user_requirements') as mock_install_req:
        mock_setup_env.return_value = mock_environ

        await main()

        mock_setup_env.assert_called_once()
        mock_install_req.assert_called_once()

@pytest.mark.asyncio
async def test_main_migrate_db(mock_environ, mock_db_utils):
    """Test main function with migrate-db command"""
    test_args = ['script.py', 'migrate-db']
    with patch.dict(os.environ, mock_environ), \
            patch.object(sys, 'argv', test_args), \
            patch('mwaa.entrypoint.setup_environment_variables') as mock_setup_env, \
            patch('mwaa.entrypoint.airflow_db_migrate') as mock_db_migrate, \
            patch('mwaa.entrypoint.increase_pool_size_if_insufficient') as mock_increase_pool:
        mock_setup_env.return_value = mock_environ

        await main()

        mock_setup_env.assert_called_once()
        mock_db_migrate.assert_called_once()
        mock_increase_pool.assert_called_once_with(mock_environ)


@pytest.mark.asyncio
async def test_main_missing_arguments():
    """Test main function with missing arguments"""
    test_args = ['script.py']
    with patch.object(sys, 'argv', test_args):
        with pytest.raises(SystemExit):
            await main()


def test_import_guard():
    """Test import guard functionality"""
    with patch.dict(os.environ, {'MWAA__CORE__TESTING_MODE': 'false'}, clear=True), \
            patch('sys.exit') as mock_exit:
        import importlib
        importlib.reload(entrypoint)
        mock_exit.assert_called_once_with(1)


def test_mark_as_unhealthy(mock_environ):
    """Test basic functionality of _mark_as_unhealthy"""
    with patch('os.makedirs') as mock_makedirs, \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('time.sleep') as mock_sleep:
        
        entrypoint._mark_as_unhealthy()

        # Verify directory creation
        mock_makedirs.assert_called_once_with('/tmp/mwaa', exist_ok=True)
        
        # Verify file creation
        mock_file.assert_called_once_with('/tmp/mwaa/container_unhealthy', 'w')
        
        # Verify sleep was called (assuming new container)
        mock_sleep.assert_called_once_with(1100)


def test_mark_as_unhealthy_error(mock_environ):
    """Test error handling in _mark_as_unhealthy"""
    with patch('os.makedirs', side_effect=Exception("Directory creation failed")), \
         patch('time.sleep') as mock_sleep:
        
        entrypoint._mark_as_unhealthy()
        
        # Verify sleep was still called
        mock_sleep.assert_called_once_with(1100)
