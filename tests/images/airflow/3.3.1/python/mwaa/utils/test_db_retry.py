"""Tests for mwaa.utils.db_retry module."""

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError, ProgrammingError


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure MWAA_DB_* env vars don't leak between tests."""
    monkeypatch.delenv("MWAA_DB_RETRY_ATTEMPTS", raising=False)
    monkeypatch.delenv("MWAA_DB_RETRY_MIN_WAIT", raising=False)
    monkeypatch.delenv("MWAA_DB_RETRY_MAX_WAIT", raising=False)


@pytest.fixture(autouse=True)
def _no_retry_backoff_sleep(monkeypatch):
    """Neutralize tenacity's exponential backoff during tests.

    tenacity's wait strategy ultimately calls ``time.sleep``; without this the
    exhaustion test would block for the full backoff sequence (~64s for 7
    attempts: 2+2+4+8+16+32). Patching ``time.sleep`` keeps retry logic intact
    while removing the real wall-clock delay.
    """
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)


def _make_operational_error():
    return OperationalError("connection failed", params=None, orig=Exception("timeout"))



def _make_interface_error():
    return InterfaceError("connection closed", params=None, orig=Exception("closed"))


class TestWithDbRetry:
    """Test the with_db_retry decorator behavior."""

    def test_succeeds_without_retry(self):
        """Function succeeds on first call — no retry triggered."""
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert fn() == "ok"
        assert call_count == 1

    def test_retries_on_operational_error(self):
        """OperationalError triggers retry, succeeds on 3rd attempt."""
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise _make_operational_error()
            return "recovered"

        assert fn() == "recovered"
        assert call_count == 3

    def test_retries_on_interface_error(self):
        """InterfaceError triggers retry."""
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise _make_interface_error()
            return "ok"

        assert fn() == "ok"
        assert call_count == 2

    def test_does_not_retry_programming_error(self):
        """ProgrammingError (non-retryable) propagates immediately."""
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            raise ProgrammingError("syntax error", params=None, orig=Exception("bad sql"))

        with pytest.raises(ProgrammingError):
            fn()
        assert call_count == 1

    def test_exhausts_retries_then_raises(self):
        """After max attempts, the exception is reraised."""
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            raise _make_operational_error()

        with pytest.raises(OperationalError):
            fn()
        # Default is 7 attempts (1 initial + 6 retries); final retry lands at
        # t~64s, covering the >60s RDS Proxy failover / SSL-reset window.
        assert call_count == 7


class TestEnvVarOverrides:
    """Test MWAA_DB_* environment variable overrides."""

    def test_retry_attempts_override(self, monkeypatch):
        """MWAA_DB_RETRY_ATTEMPTS limits retry count."""
        monkeypatch.setenv("MWAA_DB_RETRY_ATTEMPTS", "2")

        # Re-import to pick up new env var
        import importlib
        import mwaa.utils.db_retry
        importlib.reload(mwaa.utils.db_retry)
        from mwaa.utils.db_retry import with_db_retry

        call_count = 0

        @with_db_retry
        def fn():
            nonlocal call_count
            call_count += 1
            raise _make_operational_error()

        with pytest.raises(OperationalError):
            fn()
        assert call_count == 2


class TestMaintenanceEngineKwargs:
    """Test MAINTENANCE_ENGINE_KWARGS configuration."""

    def test_uses_null_pool(self):
        from sqlalchemy.pool import NullPool
        from mwaa.utils.db_retry import MAINTENANCE_ENGINE_KWARGS

        assert MAINTENANCE_ENGINE_KWARGS["poolclass"] is NullPool

    def test_connect_timeout(self):
        from mwaa.utils.db_retry import MAINTENANCE_ENGINE_KWARGS

        assert MAINTENANCE_ENGINE_KWARGS["connect_args"]["connect_timeout"] == 3
