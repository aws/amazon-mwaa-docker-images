"""
Unit tests for task_monitor.py focusing on the type annotation fix for _get_next_unprocessed_signal method.
"""

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, mock_open, MagicMock

from dateutil.tz import tz
from mwaa.celery.task_monitor import WorkerTaskMonitor, SignalData, SignalType, MWAA_SIGNALS_DIRECTORY


@pytest.fixture
def task_monitor():
    """Create a minimal WorkerTaskMonitor instance for testing"""
    with patch('mwaa.celery.task_monitor._create_shared_mem_celery_state'), \
         patch('mwaa.celery.task_monitor._create_shared_mem_work_consumption_block'), \
         patch('mwaa.celery.task_monitor._create_shared_mem_cleanup_celery_state'), \
         patch('mwaa.celery.task_monitor.get_statsd'):
        monitor = WorkerTaskMonitor(mwaa_signal_handling_enabled=True, idleness_verification_interval=5)
        yield monitor


def test_get_next_unprocessed_signal_returns_none_tuple_no_directory(task_monitor):
    """Test returns tuple[None, None] when signals directory doesn't exist"""
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=False):
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert result == (None, None)


def test_get_next_unprocessed_signal_returns_none_tuple_empty_directory(task_monitor):
    """Test returns tuple[None, None] when signals directory is empty"""
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=True), \
         patch('mwaa.celery.task_monitor.os.listdir', return_value=[]):
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert result == (None, None)


def test_get_next_unprocessed_signal_returns_valid_tuple(task_monitor):
    """Test returns tuple[str, SignalData] when valid signal exists"""
    signal_data = {
        'executionId': 'test-123', 
        'signalType': 'activation', 
        'createdAt': int(datetime.now(tz=tz.tzutc()).timestamp()), 
        'processed': False
    }
    
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=True), \
         patch('mwaa.celery.task_monitor.os.listdir', return_value=['signal1.json']), \
         patch('mwaa.celery.task_monitor.os.path.getctime', return_value=datetime.now(tz=tz.tzutc()).timestamp()), \
         patch('mwaa.celery.task_monitor.os.path.join', return_value=f'{MWAA_SIGNALS_DIRECTORY}/signal1.json'), \
         patch('builtins.open', mock_open(read_data=json.dumps(signal_data))):
        
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert isinstance(result[0], str) and isinstance(result[1], SignalData)
        assert result[0] == f'{MWAA_SIGNALS_DIRECTORY}/signal1.json'
        assert result[1].executionId == 'test-123'


def test_get_next_unprocessed_signal_skips_processed_signals(task_monitor):
    """Test returns tuple[None, None] when signals are already processed"""
    processed_signal = {
        'executionId': 'test-processed', 
        'signalType': 'kill', 
        'createdAt': int(datetime.now(tz=tz.tzutc()).timestamp()), 
        'processed': True
    }
    
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=True), \
         patch('mwaa.celery.task_monitor.os.listdir', return_value=['processed.json']), \
         patch('mwaa.celery.task_monitor.os.path.getctime', return_value=datetime.now(tz=tz.tzutc()).timestamp()), \
         patch('mwaa.celery.task_monitor.os.path.join', return_value=f'{MWAA_SIGNALS_DIRECTORY}/processed.json'), \
         patch('builtins.open', mock_open(read_data=json.dumps(processed_signal))):
        
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert result == (None, None)


def test_get_next_unprocessed_signal_handles_old_signals(task_monitor):
    """Test returns tuple[None, None] when signals are too old"""
    old_timestamp = (datetime.now(tz=tz.tzutc()) - timedelta(hours=2)).timestamp()
    
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=True), \
         patch('mwaa.celery.task_monitor.os.listdir', return_value=['old_signal.json']), \
         patch('mwaa.celery.task_monitor.os.path.getctime', return_value=old_timestamp):
        
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert result == (None, None)


def test_get_next_unprocessed_signal_handles_file_read_error(task_monitor):
    """Test returns tuple[None, None] and increments error metric when file read fails"""
    mock_stats = MagicMock()
    task_monitor.stats = mock_stats
    
    with patch('mwaa.celery.task_monitor.os.path.exists', return_value=True), \
         patch('mwaa.celery.task_monitor.os.listdir', return_value=['corrupt.json']), \
         patch('mwaa.celery.task_monitor.os.path.getctime', return_value=datetime.now(tz=tz.tzutc()).timestamp()), \
         patch('mwaa.celery.task_monitor.os.path.join', return_value=f'{MWAA_SIGNALS_DIRECTORY}/corrupt.json'), \
         patch('builtins.open', mock_open(read_data='invalid json')):
        
        result = task_monitor._get_next_unprocessed_signal()
        assert isinstance(result, tuple) and len(result) == 2
        assert result == (None, None)
        mock_stats.incr.assert_called_with("mwaa.task_monitor.signal_read_error", 1)


@pytest.mark.parametrize("signal_type", ["activation", "kill", "termination", "resume"])
def test_signal_data_from_json_string_all_types(signal_type):
    """Test SignalData.from_json_string works correctly for all signal types"""
    json_data = {
        'executionId': f'test-{signal_type}-456', 
        'signalType': signal_type, 
        'createdAt': 1234567890
    }
    
    signal_data = SignalData.from_json_string(json.dumps(json_data))
    
    assert signal_data.executionId == f'test-{signal_type}-456'
    assert signal_data.signalType == SignalType.from_string(signal_type)
    assert signal_data.createdAt == 1234567890
    assert signal_data.processed is False  # Default value
