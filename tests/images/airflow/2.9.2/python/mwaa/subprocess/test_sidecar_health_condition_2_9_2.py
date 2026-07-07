import time
import socket
import pytest
from unittest.mock import patch, MagicMock

from mwaa.subprocess.conditions import SidecarHealthCondition, ProcessStatus, ProcessConditionResponse


# ------------------------
# Fixtures
# ------------------------
@pytest.fixture
def sidecar_condition():
    """Create a SidecarHealthCondition with no replacement threshold (default behavior)."""
    condition = SidecarHealthCondition(
        airflow_component="worker",
        container_start_time=1000.0,
        port=8200,
        replacement_threshold=0,
    )
    condition.socket = MagicMock()
    return condition


@pytest.fixture
def sidecar_condition_with_threshold():
    """Create a SidecarHealthCondition with a 60-second replacement threshold."""
    condition = SidecarHealthCondition(
        airflow_component="worker",
        container_start_time=1000.0,
        port=8200,
        replacement_threshold=60,
    )
    condition.socket = MagicMock()
    return condition


@pytest.fixture
def sidecar_condition_scheduler_with_threshold():
    """Create a SidecarHealthCondition for scheduler with a 60-second replacement threshold."""
    condition = SidecarHealthCondition(
        airflow_component="scheduler",
        container_start_time=1000.0,
        port=8200,
        replacement_threshold=60,
    )
    condition.socket = MagicMock()
    return condition


# ------------------------
# Test: Initialization
# ------------------------
def test_initialization_default_threshold():
    """Test that SidecarHealthCondition initializes with correct defaults."""
    condition = SidecarHealthCondition(
        airflow_component="worker",
        container_start_time=1000.0,
    )
    assert condition.airflow_component == "worker"
    assert condition.container_start_time == 1000.0
    assert condition.last_healthy_time == 1000.0
    assert condition.replacement_threshold == 0
    assert condition.port == 8200


def test_initialization_with_threshold():
    """Test that SidecarHealthCondition initializes with custom threshold."""
    condition = SidecarHealthCondition(
        airflow_component="worker",
        container_start_time=500.0,
        port=9000,
        replacement_threshold=1800,
    )
    assert condition.last_healthy_time == 500.0
    assert condition.replacement_threshold == 1800
    assert condition.port == 9000


# ------------------------
# Test: Healthy status
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_healthy_status_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that 'Healthy' status returns SUCCESS and updates last_healthy_time."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket.recvfrom.return_value = (b"Healthy", ("127.0.0.1", 8200))

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "Status received from sidecar: Healthy" in response.message
    assert sidecar_condition.last_healthy_time == 2000.0
    mock_logger.info.assert_called()


# ------------------------
# Test: Red status
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_red_status_returns_fail(mock_logger, mock_time, sidecar_condition):
    """Test that 'Red' status returns FAIL and generates autorestart plog."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket.recvfrom.return_value = (b"Red", ("127.0.0.1", 8200))

    with patch('builtins.print') as mock_print:
        response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is False
    assert "Status received from sidecar: Red" in response.message
    mock_logger.error.assert_called()
    # Autorestart plog should be generated for failed responses
    mock_print.assert_called()


# ------------------------
# Test: Blue/Yellow with no threshold (default behavior)
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_blue_status_no_threshold_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that 'Blue' with no threshold configured returns SUCCESS."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket.recvfrom.return_value = (b"Blue", ("127.0.0.1", 8200))

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "Status received from sidecar: Blue" in response.message
    mock_logger.warning.assert_called()


@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_yellow_status_no_threshold_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that 'Yellow' with no threshold configured returns SUCCESS."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket.recvfrom.return_value = (b"Yellow", ("127.0.0.1", 8200))

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "Status received from sidecar: Yellow" in response.message
    mock_logger.warning.assert_called()


# ------------------------
# Test: Blue/Yellow with threshold NOT exceeded
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_blue_status_threshold_not_exceeded_returns_success(mock_logger, mock_time, sidecar_condition_with_threshold):
    """Test that 'Blue' with threshold configured but not exceeded returns SUCCESS."""
    # last_healthy_time = 1000.0, threshold = 60s, current time = 1030.0 → 30s < 60s
    mock_time.time.return_value = 1030.0
    sidecar_condition_with_threshold.socket.recvfrom.return_value = (b"Blue", ("127.0.0.1", 8200))

    response = sidecar_condition_with_threshold._check.__wrapped__(
        sidecar_condition_with_threshold, ProcessStatus.RUNNING
    )

    assert response.successful is True
    assert "Status received from sidecar: Blue" in response.message
    mock_logger.warning.assert_called()


# ------------------------
# Test: Blue/Yellow with threshold exceeded (worker)
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_blue_status_threshold_exceeded_worker_returns_fail(mock_logger, mock_time, sidecar_condition_with_threshold):
    """Test that 'Blue' with threshold exceeded on a worker returns FAIL."""
    # last_healthy_time = 1000.0, threshold = 60s, current time = 1060.0 → 60s >= 60s
    mock_time.time.return_value = 1060.0
    sidecar_condition_with_threshold.socket.recvfrom.return_value = (b"Blue", ("127.0.0.1", 8200))

    with patch('builtins.print'):
        response = sidecar_condition_with_threshold._check.__wrapped__(
            sidecar_condition_with_threshold, ProcessStatus.RUNNING
        )

    assert response.successful is False
    assert "No healthy heartbeat for 60s" in response.message
    assert "configured threshold: 60s" in response.message
    assert "Last sidecar status: Blue" in response.message
    mock_logger.error.assert_called()


@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_yellow_status_threshold_exceeded_worker_returns_fail(mock_logger, mock_time, sidecar_condition_with_threshold):
    """Test that 'Yellow' with threshold exceeded on a worker returns FAIL."""
    # last_healthy_time = 1000.0, threshold = 60s, current time = 1120.0 → 120s >= 60s
    mock_time.time.return_value = 1120.0
    sidecar_condition_with_threshold.socket.recvfrom.return_value = (b"Yellow", ("127.0.0.1", 8200))

    with patch('builtins.print'):
        response = sidecar_condition_with_threshold._check.__wrapped__(
            sidecar_condition_with_threshold, ProcessStatus.RUNNING
        )

    assert response.successful is False
    assert "No healthy heartbeat for 120s" in response.message
    assert "Last sidecar status: Yellow" in response.message
    mock_logger.error.assert_called()


# ------------------------
# Test: Blue/Yellow with threshold exceeded (scheduler — should NOT trigger)
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_blue_status_threshold_exceeded_scheduler_returns_success(
    mock_logger, mock_time, sidecar_condition_scheduler_with_threshold
):
    """Test that 'Blue' with threshold exceeded on a scheduler still returns SUCCESS."""
    # Threshold only applies to workers, not schedulers
    mock_time.time.return_value = 1120.0
    sidecar_condition_scheduler_with_threshold.socket.recvfrom.return_value = (b"Blue", ("127.0.0.1", 8200))

    response = sidecar_condition_scheduler_with_threshold._check.__wrapped__(
        sidecar_condition_scheduler_with_threshold, ProcessStatus.RUNNING
    )

    assert response.successful is True
    assert "Status received from sidecar: Blue" in response.message
    mock_logger.warning.assert_called()


# ------------------------
# Test: Unknown/unexpected status
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_unknown_status_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that an unexpected status string returns SUCCESS with a warning."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket.recvfrom.return_value = (b"SomethingUnexpected", ("127.0.0.1", 8200))

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "Unexpected response retrieved from sidecar" in response.message
    mock_logger.warning.assert_called()


# ------------------------
# Test: Socket timeout after warmup period
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_socket_timeout_after_warmup_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that a socket timeout after the warmup period returns SUCCESS with error log."""
    # container_start_time = 1000.0, _SIDECAR_WAIT_PERIOD = 300s
    # current time = 1400.0 → 400s > 300s (past warmup)
    mock_time.time.return_value = 1400.0
    sidecar_condition.socket.recvfrom.side_effect = socket.timeout("timed out")

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "timed out" in response.message.lower()
    mock_logger.error.assert_called()


# ------------------------
# Test: Socket timeout during warmup period
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_socket_timeout_during_warmup_returns_success(mock_logger, mock_time, sidecar_condition):
    """Test that a socket timeout during warmup returns SUCCESS with info log."""
    # container_start_time = 1000.0, _SIDECAR_WAIT_PERIOD = 300s
    # current time = 1100.0 → 100s < 300s (within warmup)
    mock_time.time.return_value = 1100.0
    sidecar_condition.socket.recvfrom.side_effect = socket.timeout("timed out")

    response = sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)

    assert response.successful is True
    assert "just started" in response.message or "ignoring" in response.message.lower()
    mock_logger.info.assert_called()


# ------------------------
# Test: Socket is None raises RuntimeError
# ------------------------
@patch('mwaa.subprocess.conditions.time')
def test_socket_none_raises_runtime_error(mock_time, sidecar_condition):
    """Test that _check raises RuntimeError when socket is None."""
    mock_time.time.return_value = 2000.0
    sidecar_condition.socket = None

    with pytest.raises(RuntimeError, match="socket object and start time shouldn't be None"):
        sidecar_condition._check.__wrapped__(sidecar_condition, ProcessStatus.RUNNING)


# ------------------------
# Test: Healthy status resets last_healthy_time (enables threshold recovery)
# ------------------------
@patch('mwaa.subprocess.conditions.time')
@patch('mwaa.subprocess.conditions.logger')
def test_healthy_resets_timer_preventing_threshold_trigger(mock_logger, mock_time, sidecar_condition_with_threshold):
    """Test that receiving 'Healthy' resets last_healthy_time so threshold doesn't trigger later."""
    # First call: receive Healthy at t=1050 → updates last_healthy_time
    mock_time.time.return_value = 1050.0
    sidecar_condition_with_threshold.socket.recvfrom.return_value = (b"Healthy", ("127.0.0.1", 8200))

    response = sidecar_condition_with_threshold._check.__wrapped__(
        sidecar_condition_with_threshold, ProcessStatus.RUNNING
    )
    assert response.successful is True
    assert sidecar_condition_with_threshold.last_healthy_time == 1050.0

    # Second call: receive Blue at t=1090 → only 40s since last healthy (< 60s threshold)
    mock_time.time.return_value = 1090.0
    sidecar_condition_with_threshold.socket.recvfrom.return_value = (b"Blue", ("127.0.0.1", 8200))

    response = sidecar_condition_with_threshold._check.__wrapped__(
        sidecar_condition_with_threshold, ProcessStatus.RUNNING
    )
    assert response.successful is True
