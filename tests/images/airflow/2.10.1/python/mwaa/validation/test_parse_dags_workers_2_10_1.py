"""Tests for the forked workers and the subprocess fallback in mwaa.validation.parse_dags."""

import os
import signal
import subprocess
import sys
import time

import pytest

from mwaa.validation import parse_dags


def _entry(file, import_error):
    return {"file": file, "dag_ids": [], "import_error": import_error, "import_error_traceback": None, "warnings": []}


def test_worker_returns_the_parse_result(write_dag, dags_folder, good_dag_source):
    """A worker returns the same entry an in-process parse would."""
    entry = parse_dags.run_in_worker(write_dag("good.py", good_dag_source), str(dags_folder), 60)
    assert entry["dag_ids"] == ["good"]
    assert entry["import_error"] is None


def test_each_worker_sees_shared_helper_warnings(write_dag, dags_folder):
    """A helper imported by two DAG files warns for both, because each worker imports it fresh."""
    write_dag("shared_helper.py", '"""Helper."""\nimport warnings\n\nwarnings.warn("shared helper", DeprecationWarning)\n')
    for name in ("first.py", "second.py"):
        path = write_dag(name, '"""An airflow dag file."""\nimport shared_helper  # noqa: F401\n')
        assert parse_dags.run_in_worker(path, str(dags_folder), 60)["warnings"] == [
            {"category": "DeprecationWarning", "message": "shared helper", "lineno": None, "location": "shared_helper.py:4"}
        ]
    assert "shared_helper" not in sys.modules


def test_worker_is_killed_after_the_timeout(write_dag, dags_folder):
    """A hanging DAG file becomes an import error instead of blocking validation."""
    path = write_dag("hangs.py", '"""An airflow dag file."""\nimport time\n\ntime.sleep(60)\n')
    started = time.monotonic()
    assert parse_dags.run_in_worker(path, str(dags_folder), 1.0) == _entry("hangs.py", "Worker timed out after 1s")
    assert time.monotonic() - started < 30


def test_worker_killed_when_its_process_group_is_gone(monkeypatch, write_dag, dags_folder):
    """If the worker's process group does not exist yet, the parent kills it by pid and does not hang."""
    def raise_no_process_group(*_):
        raise ProcessLookupError

    monkeypatch.setattr(parse_dags.os, "killpg", raise_no_process_group)
    path = write_dag("hangs.py", '"""An airflow dag file."""\nimport time\n\ntime.sleep(60)\n')
    started = time.monotonic()
    assert parse_dags.run_in_worker(path, str(dags_folder), 1.0) == _entry("hangs.py", "Worker timed out after 1s")
    assert time.monotonic() - started < 30


def test_worker_killed_by_a_signal(monkeypatch, dags_folder):
    """A worker that dies, for example in a C extension, is reported with its signal."""
    monkeypatch.setattr(parse_dags, "parse_one_file", lambda *_: os.kill(os.getpid(), signal.SIGKILL))
    path = str(dags_folder / "crash.py")
    assert parse_dags.run_in_worker(path, str(dags_folder), 30) == _entry("crash.py", "Worker exited with signal 9")


def test_worker_without_a_result(monkeypatch, dags_folder):
    """A worker that exits without writing a result is reported as such."""
    monkeypatch.setattr(parse_dags, "parse_one_file", lambda *_: object())
    path = str(dags_folder / "odd.py")
    assert parse_dags.run_in_worker(path, str(dags_folder), 30) == _entry(
        "odd.py", "Worker exited with code 1 without a result"
    )


def test_result_larger_than_the_pipe_buffer(monkeypatch, dags_folder):
    """A large result is read while the worker writes it, so neither side blocks."""
    big = _entry("big.py", "x" * 1_000_000)
    monkeypatch.setattr(parse_dags, "parse_one_file", lambda *_: big)
    assert parse_dags.run_in_worker(str(dags_folder / "big.py"), str(dags_folder), 30) == big


@pytest.mark.parametrize("payload,exit_code,expected", [
    (b"not json", 0, "Worker returned an invalid result"),
    (b"", 0, "Worker exited with code 0 without a result"),
    (b"", 5, "Worker exited with code 5 without a result"),
])
def test_collect_worker_result_reports_bad_workers(dags_folder, payload, exit_code, expected):
    """Invalid output and a non-zero exit without output are both import errors."""
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the forked child
        os.close(read_fd)
        os.write(write_fd, payload)
        os._exit(exit_code)
    os.close(write_fd)
    entry = parse_dags.collect_worker_result(pid, read_fd, str(dags_folder / "a.py"), str(dags_folder), 30)
    assert entry == _entry("a.py", expected)


def test_parse_all_forks_when_single_threaded(monkeypatch):
    """With one thread, every file is parsed in a forked worker."""
    monkeypatch.setattr(parse_dags.threading, "active_count", lambda: 1)
    monkeypatch.setattr(parse_dags, "run_in_worker", lambda f, d, t: {"file": f})
    monkeypatch.setattr(parse_dags, "run_in_subprocess", lambda *_: pytest.fail("must not use a subprocess"))
    assert parse_dags.parse_all(["/d/a.py", "/d/b.py"], "/d", 5) == ([{"file": "/d/a.py"}, {"file": "/d/b.py"}], "fork")


def test_parse_all_uses_a_subprocess_when_threads_are_running(monkeypatch):
    """With more than one thread, files are parsed in a fresh interpreter instead of forking."""
    monkeypatch.setattr(parse_dags.threading, "active_count", lambda: 2)
    monkeypatch.setattr(parse_dags, "run_in_subprocess", lambda f, d, t: {"file": f})
    monkeypatch.setattr(parse_dags, "run_in_worker", lambda *_: pytest.fail("must not fork"))
    assert parse_dags.parse_all(["/d/a.py"], "/d", 5) == ([{"file": "/d/a.py"}], "subprocess")


def test_run_in_subprocess_parses_in_a_fresh_interpreter(monkeypatch, write_dag, dags_folder, good_dag_source):
    """The fallback runs this module with --one and reads its entry."""
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
    entry = parse_dags.run_in_subprocess(write_dag("good.py", good_dag_source), str(dags_folder), 60)
    assert entry["dag_ids"] == ["good"]
    assert entry["import_error"] is None


def test_run_in_subprocess_timeout(monkeypatch, dags_folder):
    """A fallback interpreter that does not finish in time becomes an import error."""
    def fake_run(command, check, timeout):
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(parse_dags.subprocess, "run", fake_run)
    assert parse_dags.run_in_subprocess(str(dags_folder / "a.py"), str(dags_folder), 5) == _entry(
        "a.py", "Worker timed out after 5s"
    )


def test_run_in_subprocess_without_a_result(monkeypatch, dags_folder):
    """A fallback interpreter that exits without writing its entry becomes an import error."""
    monkeypatch.setattr(
        parse_dags.subprocess, "run", lambda command, check, timeout: subprocess.CompletedProcess(command, 1)
    )
    assert parse_dags.run_in_subprocess(str(dags_folder / "a.py"), str(dags_folder), 5) == _entry(
        "a.py", "Worker exited with code 1 without a result"
    )
