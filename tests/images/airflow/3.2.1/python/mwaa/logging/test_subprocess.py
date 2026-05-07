import pytest
from unittest.mock import patch, Mock
import os
import logging
from io import BytesIO
from subprocess import Popen
from mwaa.subprocess.subprocess import (
    Subprocess,
    _parse_log_level,
)


@pytest.fixture
def subprocess_instance():
    return Subprocess(cmd="test_command")


class TestParseLogLevel:
    """Tests for _parse_log_level: structlog (padded), structlog (compact), legacy, and fallback."""

    @pytest.mark.parametrize("line,expected", [
        # Structlog with timestamp and padding (production format in 3.1+)
        ("2026-05-06T01:00:14.610844Z [info     ] Filling up the DagBag", logging.INFO),
        ("2026-05-06T01:00:14.610844Z [warning  ] Something concerning", logging.WARNING),
        ("2026-05-06T01:00:14.610844Z [error    ] Failed to process", logging.ERROR),
        ("2026-05-06T01:00:14.610844Z [critical ] Fatal error", logging.CRITICAL),
        ("2026-05-06T01:00:14.610844Z [debug    ] Verbose output", logging.DEBUG),
        # Structlog compact (no padding)
        ("[info] request finished [http.access]", logging.INFO),
        ("[warning] something concerning [some.logger]", logging.WARNING),
        ("[error] something failed [some.logger]", logging.ERROR),
        # Legacy Airflow format
        ("[2025-01-01 00:00:00 +0000] {scheduler_job_runner.py:123} INFO - msg", logging.INFO),
        ("[2025-01-01 00:00:00 +0000] {scheduler_job_runner.py:123} WARNING - msg", logging.WARNING),
        ("[2025-01-01 00:00:00 +0000] {scheduler_job_runner.py:123} ERROR - msg", logging.ERROR),
        # Unrecognized defaults to INFO
        ("Some random output without a level indicator", logging.INFO),
        ("================================================================================", logging.INFO),
    ])
    def test_parse_log_level(self, line, expected):
        assert _parse_log_level(line) == expected


class TestReadSubprocessLogStream:
    """Tests for _read_subprocess_log_stream filtering behavior."""

    def test_null_and_closed_stream(self, subprocess_instance):
        """No logging when stream is None or closed"""
        subprocess_instance.process_logger = Mock()

        for stdout in [None, Mock(closed=True)]:
            mock_process = Mock(spec=Popen)
            mock_process.stdout = stdout
            subprocess_instance._read_subprocess_log_stream(mock_process)

        subprocess_instance.process_logger.info.assert_not_called()

    def test_mixed_levels_filtered_at_warning(self, subprocess_instance):
        """Only WARNING+ lines pass when threshold is WARNING"""
        mock_process = Mock(spec=Popen)
        mock_process.stdout = BytesIO(
            b"2026-05-06T01:00:14Z [info     ] should be filtered\n"
            b"2026-05-06T01:00:15Z [warning  ] should pass as warning\n"
            b"2026-05-06T01:00:16Z [error    ] should pass as error\n"
            b"[2025-01-01 00:00:00 +0000] {m.py:1} INFO - filtered legacy\n"
            b"[2025-01-01 00:00:00 +0000] {m.py:2} ERROR - pass legacy\n"
            b"Some unrecognized line\n"
        )
        mock_process.poll.return_value = 0

        subprocess_instance.process_logger = Mock()
        with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'WARNING'}):
            subprocess_instance._read_subprocess_log_stream(mock_process)

        subprocess_instance.process_logger.info.assert_not_called()
        assert subprocess_instance.process_logger.warning.call_count == 1
        assert subprocess_instance.process_logger.error.call_count == 2

    def test_all_lines_pass_at_info_level(self, subprocess_instance):
        """All lines pass when threshold is INFO (including unrecognized)"""
        mock_process = Mock(spec=Popen)
        mock_process.stdout = BytesIO(
            b"2026-05-06T01:00:14Z [info     ] structlog info\n"
            b"Some random output\n"
        )
        mock_process.poll.return_value = 0

        subprocess_instance.process_logger = Mock()
        with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'INFO'}):
            subprocess_instance._read_subprocess_log_stream(mock_process)

        assert subprocess_instance.process_logger.info.call_count == 2

    def test_empty_reads_while_running(self, mocker, subprocess_instance):
        """Empty reads while process is running trigger sleep, not exit"""
        mock_process = mocker.Mock(spec=Popen)
        mock_process.stdout = mocker.Mock()
        mock_process.stdout.closed = False
        mock_process.poll.side_effect = [None, None, 0]
        mock_process.stdout.readline.side_effect = [
            b"2026-05-06T01:00:14Z [warning  ] test\n",
            b'',
            b'',
            b''
        ]

        subprocess_instance.process_logger = Mock()
        with patch.dict(os.environ, {'AIRFLOW_CONSOLE_LOG_LEVEL': 'WARNING'}):
            subprocess_instance._read_subprocess_log_stream(mock_process)

        assert mock_process.poll.call_count == 3
        subprocess_instance.process_logger.warning.assert_called_once()
