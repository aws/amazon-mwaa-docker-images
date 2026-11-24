# test_user_requirements.py
import pytest
import os
from unittest.mock import patch, mock_open, MagicMock

from mwaa.utils.user_requirements import (
    install_user_requirements,
    _read_requirements_file,
    _requirements_has_constraints,
    USER_REQUIREMENTS_MAX_INSTALL_TIME
)


# ------------------------
# Fixtures
# ------------------------
@pytest.fixture
def mock_environ():
    """Basic environment variables fixture"""
    return {
        "MWAA__CORE__REQUIREMENTS_PATH": "/path/to/requirements.txt",
        "AIRFLOW_CONSTRAINTS_FILE": "/path/to/constraints.txt",
        "MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL": "INFO",
        "MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL": "INFO",
    }


@pytest.fixture
def mock_subprocess():
    """Mock Subprocess class"""
    subprocess_instance = MagicMock()
    subprocess_instance.process = MagicMock()
    subprocess_instance.process.returncode = 0
    subprocess_instance.start = MagicMock(return_value=0)

    with patch('mwaa.utils.user_requirements.Subprocess', return_value=subprocess_instance) as mock:
        yield mock, subprocess_instance


@pytest.fixture
def mock_composite_logger():
    """Mock CompositeLogger"""
    logger_instance = MagicMock()
    with patch('mwaa.utils.user_requirements.CompositeLogger', return_value=logger_instance) as mock:
        yield mock, logger_instance


# ------------------------
# Test Cases
# ------------------------

def test_read_requirements_file():
    """Test reading requirements file"""
    content = "package1==1.0.0\npackage2>=2.0.0"
    with patch('builtins.open', mock_open(read_data=content.encode())):
        result = _read_requirements_file("requirements.txt")
        assert result == content


@pytest.mark.parametrize("content,expected", [
    ("package1==1.0.0\n-c constraints.txt", True),
    ("package1==1.0.0\npackage2>=2.0.0", False),
    ("# -c constraints.txt\npackage1==1.0.0", True),
])
def test_requirements_has_constraints(content, expected):
    """Test constraint detection in requirements file"""
    with patch('mwaa.utils.user_requirements._read_requirements_file', return_value=content):
        assert _requirements_has_constraints("requirements.txt") == expected


@pytest.mark.asyncio
async def test_install_user_requirements_no_file(mock_environ):
    """Test when no requirements file is specified"""
    with patch.dict(os.environ, {}, clear=True):
        await install_user_requirements("worker", {})
        # Should complete without error


@pytest.mark.asyncio
async def test_install_user_requirements_with_constraints(mock_environ):
    """Test installation with constraints"""
    requirements_content = "package1==1.0.0\n-c constraints.txt"

    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value=requirements_content), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
        mock_process = MagicMock()
        mock_process.process = MagicMock()
        mock_subprocess.return_value = mock_process

        await install_user_requirements("worker", mock_environ)

        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args[1]
        assert call_args['cmd'] == ['safe-pip-install', '-r', mock_environ['MWAA__CORE__REQUIREMENTS_PATH']]


@pytest.mark.asyncio
async def test_install_user_requirements_without_constraints(mock_environ):
    """Test installation without constraints"""
    requirements_content = "package1==1.0.0"

    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value=requirements_content), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
        mock_process = MagicMock()
        mock_process.process = MagicMock()
        mock_subprocess.return_value = mock_process

        await install_user_requirements("worker", mock_environ)

        mock_subprocess.assert_called_once()
        call_args = mock_subprocess.call_args[1]
        assert '-c' in call_args['cmd']
        assert mock_environ['AIRFLOW_CONSTRAINTS_FILE'] in call_args['cmd']


@pytest.mark.asyncio
async def test_install_user_requirements_installation_error(mock_environ):
    """Test handling of installation errors"""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess, \
            patch('mwaa.utils.user_requirements.CompositeLogger') as mock_logger:
        mock_process = MagicMock()
        mock_process.process = MagicMock()
        mock_process.process.returncode = 1
        mock_subprocess.return_value = mock_process

        logger_instance = MagicMock()
        mock_logger.return_value = logger_instance

        await install_user_requirements("worker", mock_environ)
        logger_instance.error.assert_called()


@pytest.mark.asyncio
async def test_install_user_requirements_hybrid_mode(mock_environ):
    """Test installation in hybrid mode"""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
        mock_process = MagicMock()
        mock_subprocess.return_value = mock_process

        await install_user_requirements("hybrid", mock_environ)

        assert mock_subprocess.call_args[1]['friendly_name'] == "worker_requirements"


@pytest.mark.asyncio
async def test_subprocess_timeout_configuration(mock_environ):
    """Test subprocess timeout configuration"""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
        await install_user_requirements("worker", mock_environ)

        conditions = mock_subprocess.call_args[1]['conditions']
        timeout_condition = next(c for c in conditions if hasattr(c, 'timeout'))
        assert timeout_condition.timeout == USER_REQUIREMENTS_MAX_INSTALL_TIME


@pytest.mark.asyncio
async def test_install_user_requirements_file_read_error(mock_environ):
    """Test handling of file read errors"""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', side_effect=Exception("Read error")), \
            patch('mwaa.utils.user_requirements.CompositeLogger') as mock_logger:
        logger_instance = MagicMock()
        mock_logger.return_value = logger_instance

        await install_user_requirements("worker", mock_environ)
        logger_instance.warning.assert_called()


@pytest.mark.asyncio
async def test_install_user_requirements_command_types(mock_environ):
    """Test different command types"""
    commands = ["worker", "scheduler", "webserver"]
    for cmd in commands:
        with patch.dict(os.environ, mock_environ, clear=True), \
                patch('os.path.isfile', return_value=True), \
                patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
                patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
            mock_process = MagicMock()
            mock_subprocess.return_value = mock_process

            await install_user_requirements(cmd, mock_environ)

            assert mock_subprocess.call_args[1]['friendly_name'] == f"{cmd}_requirements"


@pytest.mark.asyncio
async def test_install_user_requirements_returns_true_without_a_file():
    """No requirements file counts as success."""
    with patch.dict(os.environ, {}, clear=True):
        assert await install_user_requirements("test-requirements", {}) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode,expected", [(0, True), (1, False), (-15, False), (None, False)])
async def test_install_user_requirements_returns_the_pip_outcome(mock_environ, returncode, expected):
    """The return value follows pip's exit code, read after Subprocess cleared its handle."""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess:
        mock_process = MagicMock()
        mock_process.process = None
        mock_process.returncode = returncode
        mock_subprocess.return_value = mock_process

        assert await install_user_requirements("test-requirements", mock_environ) is expected


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode,logs_error", [(0, False), (1, True), (-15, True)])
async def test_install_user_requirements_logs_a_pip_failure(mock_environ, returncode, logs_error):
    """The pip failure line follows the exit code Subprocess keeps after it clears its handle."""
    with patch.dict(os.environ, mock_environ, clear=True), \
            patch('os.path.isfile', return_value=True), \
            patch('mwaa.utils.user_requirements._read_requirements_file', return_value="package1==1.0.0"), \
            patch('mwaa.utils.user_requirements.Subprocess') as mock_subprocess, \
            patch('mwaa.utils.user_requirements.CompositeLogger') as mock_logger:
        mock_process = MagicMock()
        mock_process.process = None
        mock_process.returncode = returncode
        mock_subprocess.return_value = mock_process
        logger_instance = MagicMock()
        mock_logger.return_value = logger_instance

        await install_user_requirements("worker", mock_environ)

        messages = [str(call.args[0]) for call in logger_instance.error.call_args_list]
        assert any("pip installation exited with a non-zero error code" in m for m in messages) is logs_error
