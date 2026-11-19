"""
Parse DAG files one at a time and record import errors and warnings.

The test-dags command runs this module as a child process::

    python3 -W default -m mwaa.validation.parse_dags \
        --dags-folder DIR --plugins-folder DIR --output FILE --ready-fd FD

``-W default`` must be on the command line. It puts an entry in ``sys.warnoptions``
before Airflow is imported, so ``airflow/configuration.py`` does not install its own
warning filters. The process imports Airflow and loads plugins once, then forks one
worker per DAG file, the way Airflow's DAG processor does. Each worker parses its
file with a fresh module cache, so every warning raised while that file is imported
belongs to that file.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import warnings
from typing import Any, Dict, List, Optional, Sequence, Tuple

from airflow.configuration import conf

from mwaa.validation import compat
from mwaa.validation.report import (
    PARSE_MODE_FORK,
    PARSE_MODE_SUBPROCESS,
    error_summary,
)

# Extra seconds the parent waits beyond Airflow's own dagbag_import_timeout before it
# kills a worker. DagBag enforces its own timeout with SIGALRM; this one catches hangs
# that SIGALRM cannot interrupt, like a blocking call inside a C extension.
WORKER_TIMEOUT_MARGIN_SECONDS = 10.0
# Extra seconds a fallback subprocess gets to import Airflow and load plugins.
SUBPROCESS_STARTUP_SECONDS = 120.0
_PIPE_READ_SIZE = 65536


def display_path(path: str, base_folder: str) -> str:
    """
    Return path relative to base_folder when it is inside it, else path unchanged.

    The path as given is preferred, so a symlink inside the folder keeps its own name.
    Resolved paths are compared only when the given path is outside the folder, for
    example when the base folder itself is reached through a symlink.

    :param path: A file path, or a pseudo path like ``<frozen importlib._bootstrap>``.
    :param base_folder: The DAGs or plugins folder.
    """
    if not os.path.isabs(path):
        return path
    for candidate, base in (
        (os.path.abspath(path), os.path.abspath(base_folder)),
        (os.path.realpath(path), os.path.realpath(base_folder)),
    ):
        if candidate.startswith(base + os.sep):
            return os.path.relpath(candidate, base)
    return path


def _empty_entry(dag_file: str, dags_folder: str) -> Dict[str, Any]:
    return {
        "file": display_path(dag_file, dags_folder),
        "dag_ids": [],
        "import_error": None,
        "import_error_traceback": None,
        "warnings": [],
    }


def failed_entry(dag_file: str, dags_folder: str, message: str) -> Dict[str, Any]:
    """
    Return a report entry for a file whose worker failed without a result.

    :param dag_file: The DAG file.
    :param dags_folder: The DAGs folder.
    :param message: The import error to record.
    """
    entry = _empty_entry(dag_file, dags_folder)
    entry["import_error"] = message
    return entry


def normalize_warnings(
    recorded: Sequence[warnings.WarningMessage],
    dag_file: str,
    dags_folder: str,
    dagbag: Any,
) -> List[Dict[str, Any]]:
    """
    Turn recorded warnings into report entries.

    ``lineno`` is set only when Python points into the DAG file itself; ``location``
    always holds where Python pointed. Identical (category, message, location)
    entries are reported once, in the order first seen.

    :param recorded: Warnings recorded while the file was parsed.
    :param dag_file: The DAG file that was parsed.
    :param dags_folder: The DAGs folder, used to shorten paths.
    :param dagbag: The DagBag that parsed the file, used to recover categories.
    """
    real_dag_file = os.path.realpath(dag_file)
    seen: set[Tuple[str, str, str]] = set()
    entries: List[Dict[str, Any]] = []
    for warning in recorded:
        message = str(warning.message)
        category = compat.resolve_category(
            warning.category, warning.filename, warning.lineno, message, dagbag, dag_file
        )
        location = f"{display_path(warning.filename, dags_folder)}:{warning.lineno}"
        key = (category, message, location)
        if key in seen:
            continue
        seen.add(key)
        in_dag_file = (
            warning.lineno > 0
            and os.path.isabs(warning.filename)
            and os.path.realpath(warning.filename) == real_dag_file
        )
        entries.append(
            {
                "category": category,
                "message": message,
                "lineno": warning.lineno if in_dag_file else None,
                "location": location,
            }
        )
    return entries


def parse_one_file(dag_file: str, dags_folder: str) -> Dict[str, Any]:
    """
    Parse one DAG file in this process and return its report entry.

    Any exception, including SystemExit raised by DAG code, becomes the file's import
    error. Only warnings raised inside ``DagBag.process_file`` are recorded, so
    warnings Airflow raises while building the DagBag are not charged to the file.

    :param dag_file: Absolute path of the DAG file.
    :param dags_folder: The DAGs folder.
    """
    entry = _empty_entry(dag_file, dags_folder)
    recorded: List[warnings.WarningMessage] = []
    dagbag: Any = None
    try:
        dagbag = compat.make_dagbag(dag_file)
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            dags = dagbag.process_file(
                dag_file, only_if_updated=False, safe_mode=compat.safe_mode()
            )
        entry["dag_ids"] = sorted({dag.dag_id for dag in dags})
        error_text = next(iter(dagbag.import_errors.values()), None)
        if error_text:
            entry["import_error"] = error_summary(error_text)
            entry["import_error_traceback"] = error_text
    except BaseException:  # DAG code may raise SystemExit or KeyboardInterrupt
        error_text = traceback.format_exc()
        entry["import_error"] = error_summary(error_text)
        entry["import_error_traceback"] = error_text
    entry["warnings"] = normalize_warnings(recorded, dag_file, dags_folder, dagbag)
    return entry


def _worker_main(dag_file: str, dags_folder: str, write_fd: int) -> None:  # pragma: no cover - runs in the forked worker
    try:
        os.setsid()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        compat.reset_orm_after_fork()
        payload = json.dumps(parse_one_file(dag_file, dags_folder)).encode()
        with os.fdopen(write_fd, "wb") as pipe:
            pipe.write(payload)
    except BaseException:
        # setsid, the ORM reset, JSON encoding or the pipe write failed; report it as
        # a non-zero exit instead of silently exiting 0 and losing the error.
        traceback.print_exc()
        sys.stdout.flush()
        sys.stderr.flush()
        # Skip the atexit handlers inherited from the parent.
        os._exit(1)
    # Flush any output the DAG code wrote, since os._exit skips buffer flushing.
    sys.stdout.flush()
    sys.stderr.flush()
    # Skip the atexit handlers inherited from the parent.
    os._exit(0)


def _kill_worker(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
        return
    except (ProcessLookupError, PermissionError):
        # The worker had not called os.setsid() yet, so its process group does not
        # exist; fall back to killing the process directly so os.waitpid cannot hang.
        pass
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def collect_worker_result(
    pid: int, read_fd: int, dag_file: str, dags_folder: str, timeout_seconds: float
) -> Dict[str, Any]:
    """
    Read a worker's result from its pipe, enforcing the timeout, then reap it.

    The pipe is read before waiting for the worker, so a result larger than the pipe
    buffer cannot deadlock the two processes.

    :param pid: The worker's process ID.
    :param read_fd: The read end of the worker's result pipe. It is closed here.
    :param dag_file: The DAG file the worker parses.
    :param dags_folder: The DAGs folder.
    :param timeout_seconds: How long to wait for the result.
    """
    chunks: List[bytes] = []
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    with os.fdopen(read_fd, "rb", buffering=0) as pipe:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            ready, _, _ = select.select([pipe], [], [], remaining)
            if not ready:
                continue
            chunk = pipe.read(_PIPE_READ_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
    if timed_out:
        _kill_worker(pid)
    _, status = os.waitpid(pid, 0)
    if timed_out:
        return failed_entry(dag_file, dags_folder, f"Worker timed out after {timeout_seconds:g}s")
    if chunks:
        try:
            return json.loads(b"".join(chunks))
        except ValueError:
            return failed_entry(dag_file, dags_folder, "Worker returned an invalid result")
    if os.WIFSIGNALED(status):
        return failed_entry(
            dag_file, dags_folder, f"Worker exited with signal {os.WTERMSIG(status)}"
        )
    return failed_entry(
        dag_file,
        dags_folder,
        f"Worker exited with code {os.waitstatus_to_exitcode(status)} without a result",
    )


def run_in_worker(dag_file: str, dags_folder: str, timeout_seconds: float) -> Dict[str, Any]:
    """
    Parse one DAG file in a forked worker and return its report entry.

    :param dag_file: Absolute path of the DAG file.
    :param dags_folder: The DAGs folder.
    :param timeout_seconds: How long to wait before killing the worker.
    """
    read_fd, write_fd = os.pipe()
    # Flush before forking so buffered parent output is not duplicated by the child.
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the forked worker
        os.close(read_fd)
        _worker_main(dag_file, dags_folder, write_fd)
    os.close(write_fd)
    return collect_worker_result(pid, read_fd, dag_file, dags_folder, timeout_seconds)


def run_in_subprocess(dag_file: str, dags_folder: str, timeout_seconds: float) -> Dict[str, Any]:
    """
    Parse one DAG file in a fresh interpreter and return its report entry.

    Used instead of fork when this process runs more than one thread, because forking
    a multi-threaded process can deadlock.

    :param dag_file: Absolute path of the DAG file.
    :param dags_folder: The DAGs folder.
    :param timeout_seconds: The parse timeout; the interpreter also gets
      SUBPROCESS_STARTUP_SECONDS to import Airflow.
    """
    with tempfile.TemporaryDirectory(prefix="mwaa-parse-one-") as tmp_dir:
        output = os.path.join(tmp_dir, "entry.json")
        command = [
            sys.executable, "-W", "default", "-m", "mwaa.validation.parse_dags",
            "--dags-folder", dags_folder, "--one", dag_file, "--output", output,
        ]
        try:
            completed = subprocess.run(
                command, check=False, timeout=timeout_seconds + SUBPROCESS_STARTUP_SECONDS
            )
        except subprocess.TimeoutExpired:
            return failed_entry(dag_file, dags_folder, f"Worker timed out after {timeout_seconds:g}s")
        try:
            with open(output, encoding="utf-8") as result_file:
                return json.load(result_file)
        except (OSError, ValueError):
            return failed_entry(
                dag_file,
                dags_folder,
                f"Worker exited with code {completed.returncode} without a result",
            )


def parse_all(
    dag_files: Sequence[str], dags_folder: str, timeout_seconds: float
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Parse every DAG file in its own worker, one file at a time.

    :param dag_files: The files to parse.
    :param dags_folder: The DAGs folder.
    :param timeout_seconds: The per-file timeout.
    :returns: The entries and the parse mode (PARSE_MODE_SUBPROCESS if any file had
      to be parsed in a fresh interpreter).
    """
    entries: List[Dict[str, Any]] = []
    parse_mode = PARSE_MODE_FORK
    for dag_file in dag_files:
        if threading.active_count() > 1:
            parse_mode = PARSE_MODE_SUBPROCESS
            entries.append(run_in_subprocess(dag_file, dags_folder, timeout_seconds))
        else:
            entries.append(run_in_worker(dag_file, dags_folder, timeout_seconds))
    return entries, parse_mode


def collect_plugin_import_errors(plugins_folder: str) -> List[Dict[str, Any]]:
    """
    Load plugins the way Airflow does and return their import errors.

    :param plugins_folder: The plugins folder, used to shorten paths.
    """
    errors = compat.plugin_import_errors()
    return sorted(
        (
            {"file": display_path(name, plugins_folder), "error": error_summary(message) or ""}
            for name, message in errors.items()
        ),
        key=lambda entry: entry["file"],
    )


def warm_up() -> None:
    """
    Import the modules Airflow's DAG processor has loaded before it parses a file.

    Their import-time warnings, for example Airflow 2.11's timer deprecation warning,
    are then raised here and not charged to the first DAG file that imports them.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for module in compat.WARM_UP_MODULES:
            importlib.import_module(module)


def worker_timeout_seconds() -> float:
    """Return how long the parent waits for one worker."""
    return conf.getfloat("core", "dagbag_import_timeout") + WORKER_TIMEOUT_MARGIN_SECONDS


def file_timeout_seconds() -> float:
    """
    Return the longest one DAG file can take, in either parse mode.

    A file parsed in a fresh interpreter also gets SUBPROCESS_STARTUP_SECONDS.
    """
    return worker_timeout_seconds() + SUBPROCESS_STARTUP_SECONDS


def send_ready(ready_fd: int, file_count: int) -> None:
    """
    Tell the test-dags process that setup is done, then close the descriptor.

    The message holds the number of DAG files and the per-file limit, from which the
    test-dags process works out how long parsing may take. Closing the descriptor
    keeps the forked workers from inheriting it.

    :param ready_fd: The write end of the pipe from the test-dags process.
    :param file_count: How many DAG files will be parsed.
    """
    message = json.dumps(
        {"files": file_count, "file_timeout_seconds": file_timeout_seconds()}
    ).encode() + b"\n"
    try:
        while message:
            message = message[os.write(ready_fd, message):]
    finally:
        os.close(ready_fd)


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as output:
        json.dump(payload, output)


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse DAG files for test-dags.")
    parser.add_argument("--dags-folder", required=True)
    parser.add_argument("--plugins-folder", default="")
    parser.add_argument("--output", required=True)
    parser.add_argument("--one", help="Parse only this file in this process.")
    parser.add_argument(
        "--ready-fd", type=int, help="Write the ready message to this file descriptor."
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Run the parser and write its result as JSON to --output.

    :param argv: Command-line arguments; defaults to sys.argv[1:].
    :returns: The process exit code.
    """
    args = _parse_args(argv)
    dags_folder = os.path.abspath(args.dags_folder)
    # Airflow 3's DAG processor puts the bundle folder on sys.path; Airflow 2 puts
    # the DAGs folder there. Do the same so DAGs can import sibling modules.
    if dags_folder not in sys.path:
        sys.path.append(dags_folder)
    plugin_errors = collect_plugin_import_errors(args.plugins_folder)
    warm_up()
    if args.one:
        _write_json(args.output, parse_one_file(os.path.abspath(args.one), dags_folder))
        return 0
    dag_files = compat.list_dag_files(dags_folder)
    if args.ready_fd is not None:
        send_ready(args.ready_fd, len(dag_files))
    entries, parse_mode = parse_all(dag_files, dags_folder, worker_timeout_seconds())
    _write_json(
        args.output,
        {"plugins": {"import_errors": plugin_errors}, "dags": entries, "parse_mode": parse_mode},
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
