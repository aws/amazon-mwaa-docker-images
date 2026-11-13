"""Tests that Subprocess keeps the exit code after the process finishes."""

from types import SimpleNamespace

import pytest

from mwaa.subprocess.subprocess import Subprocess


class _FailingCondition:
    """A process condition that fails its first check, the way an expired timeout does."""

    def prepare(self):
        """Nothing to prepare."""

    def check(self, process_status):
        """Report failure so Subprocess stops the process."""
        return SimpleNamespace(successful=False, message="stopped by the test")

    def close(self):
        """Nothing to release."""


@pytest.fixture(autouse=True)
def _console_log_level(monkeypatch):
    # Subprocess reads this variable when it forwards output lines.
    monkeypatch.setenv("AIRFLOW_CONSOLE_LOG_LEVEL", "INFO")


def _run(cmd, conditions=None):
    process = Subprocess(cmd=cmd, friendly_name="returncode_test", conditions=conditions or [])
    process.start()
    return process


def test_returncode_is_none_before_start():
    """A process that has not run has no exit code."""
    assert Subprocess(cmd=["/bin/true"], friendly_name="returncode_test").returncode is None


def test_returncode_is_kept_after_success():
    """The exit code survives shutdown clearing the process handle."""
    process = _run(["/bin/sh", "-c", "exit 0"])
    assert process.process is None
    assert process.returncode == 0


def test_returncode_is_kept_after_failure():
    """A non-zero exit code is kept."""
    process = _run(["/bin/sh", "-c", "exit 3"])
    assert process.process is None
    assert process.returncode == 3


def test_returncode_is_non_zero_after_a_failed_condition():
    """A process stopped by a failed condition, such as the install timeout, does not report success."""
    process = _run(["/bin/sh", "-c", "sleep 30"], conditions=[_FailingCondition()])
    assert process.process is None
    assert process.returncode is not None
    assert process.returncode != 0


def test_returncode_is_none_when_the_command_cannot_start():
    """A command that cannot be started has no exit code."""
    assert _run(["/nonexistent/command-for-returncode-test"]).returncode is None
