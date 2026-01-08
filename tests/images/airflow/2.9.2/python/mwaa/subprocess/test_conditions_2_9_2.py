import pytest
from unittest.mock import patch, MagicMock
from mwaa.subprocess.conditions import AirflowDbReachableCondition, ProcessStatus, ProcessConditionResponse


# ------------------------
# Fixtures
# ------------------------
@pytest.fixture
def mock_db_connection():
    """Mock database connection and engine"""
    mock_connection = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    return mock_connection, mock_engine


@pytest.fixture
def airflow_db_condition():
    """Fixture to create AirflowDbReachableCondition instance"""
    return AirflowDbReachableCondition("scheduler")


# ------------------------
# Test Cases
# ------------------------
def test_airflow_db_reachable_condition_success(airflow_db_condition, mock_db_connection):
    """Test successful database connection with current_user query"""
    mock_connection, mock_engine = mock_db_connection
    
    # Mock the SELECT 1 query
    mock_connection.execute.side_effect = [
        MagicMock(),  # SELECT 1 result
        MagicMock(scalar=lambda: "test_user")  # SELECT current_user result
    ]
    
    airflow_db_condition.engine = mock_engine
    
    with patch('mwaa.subprocess.conditions.logger') as mock_logger:
        response = airflow_db_condition._check(ProcessStatus.RUNNING)
    
    # Verify both queries were executed
    assert mock_connection.execute.call_count == 2
    mock_connection.execute.assert_any_call("SELECT 1")
    mock_connection.execute.assert_any_call("SELECT current_user;")
    
    # Verify response
    assert response.successful is True
    assert "Successfully connected to database as user: test_user" in response.message
    
    # Verify logging
    mock_logger.info.assert_called_with("Successfully connected to database as user: test_user")


def test_airflow_db_reachable_condition_failure(airflow_db_condition, mock_db_connection):
    """Test database connection failure"""
    mock_connection, mock_engine = mock_db_connection
    
    # Mock connection failure
    mock_engine.connect.side_effect = Exception("Connection failed")
    airflow_db_condition.engine = mock_engine
    
    with patch('mwaa.subprocess.conditions.logger') as mock_logger:
        response = airflow_db_condition._check(ProcessStatus.RUNNING)
    
    # Verify response
    assert response.successful is True  # Condition is informational only
    assert "Couldn't connect to database. Error: Connection failed" in response.message
    
    # Verify logging
    mock_logger.error.assert_called_with("Couldn't connect to database. Error: Connection failed")


def test_airflow_db_condition_health_plog_generation(airflow_db_condition, mock_db_connection):
    """Test health plog generation for database condition"""
    mock_connection, mock_engine = mock_db_connection
    mock_connection.execute.side_effect = [
        MagicMock(),
        MagicMock(scalar=lambda: "test_user")
    ]
    
    airflow_db_condition.engine = mock_engine
    airflow_db_condition.healthy = False  # Previous state was unhealthy
    
    with patch('mwaa.subprocess.conditions.generate_plog') as mock_generate_plog, \
         patch('builtins.print') as mock_print:
        
        mock_generate_plog.return_value = "test_plog_message"
        airflow_db_condition._check(ProcessStatus.RUNNING)
        
        # Verify plog generation for health change
        mock_generate_plog.assert_called_once_with(
            "RDSHealthLogsProcessor",
            "[scheduler] connection with RDS Meta DB is CONNECTION_BECAME_HEALTHY."
        )
        mock_print.assert_called_once_with("test_plog_message")
