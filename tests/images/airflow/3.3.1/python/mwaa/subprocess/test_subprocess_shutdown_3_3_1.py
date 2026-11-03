"""Tests for the shutdown logic in mwaa.subprocess.subprocess."""
import signal
import subprocess as py_subprocess
from datetime import timedelta
from unittest.mock import MagicMock, patch

from mwaa.subprocess.subprocess import Subprocess


def _make_subprocess_under_test():
    sp = Subprocess.__new__(Subprocess)
    sp.cmd = ["airflow", "celery", "worker"]
    sp.friendly_name = "worker"
    sp.name = 'Process "worker" (PID 12345)'
    sp.sigterm_patience_interval = timedelta(seconds=0.01)
    sp.process_logger = MagicMock()
    return sp


def _make_fake_process():
    process = MagicMock()
    process.pid = 12345
    process.poll.return_value = None
    process.returncode = None
    process.communicate.side_effect = py_subprocess.TimeoutExpired(
        cmd=["airflow", "celery", "worker"], timeout=0.01
    )
    return process


def test_sigkill_fallback_tolerates_process_lookup_error():
    """SIGKILL fallback must not raise when the process group is already gone."""
    sp = _make_subprocess_under_test()
    process = _make_fake_process()
    sp.process = process

    killpg_calls = []

    def fake_killpg(pgid, sig):
        killpg_calls.append(sig)
        if sig == signal.SIGKILL:
            raise ProcessLookupError(3, "No such process")

    with patch("os.getpgid", return_value=12345), patch(
        "os.killpg", side_effect=fake_killpg
    ):
        sp._shutdown_python_subprocess(process)

    assert killpg_calls == [signal.SIGTERM, signal.SIGKILL]


def test_sigkill_fallback_still_kills_live_process_group():
    """SIGKILL fallback must still be issued when the process group is alive."""
    sp = _make_subprocess_under_test()
    process = _make_fake_process()
    sp.process = process

    killpg_calls = []

    def fake_killpg(pgid, sig):
        killpg_calls.append(sig)

    with patch("os.getpgid", return_value=12345), patch(
        "os.killpg", side_effect=fake_killpg
    ):
        sp._shutdown_python_subprocess(process)

    assert killpg_calls == [signal.SIGTERM, signal.SIGKILL]
