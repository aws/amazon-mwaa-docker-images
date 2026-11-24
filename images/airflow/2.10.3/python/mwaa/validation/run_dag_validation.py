"""
Run the test-dags command.

The command checks a customer's requirements, plugins and DAGs against this image's
Airflow version without a metadata database:

1. Copy the mounted plugins folder, or unzip a mounted plugins.zip, into the plugins
   folder, as MWAA does (prepare_plugins). The entrypoint does this before it runs the
   startup script, because MWAA's startup script can use files from plugins.zip, and
   packaged requirements install from the plugins folder.
2. Install the requirements with install_user_requirements, as test-requirements does.
3. Load plugins and parse DAGs in a child process (mwaa.validation.parse_dags).
4. Write the JSON report, print a summary and return the exit code.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import select
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from mwaa.utils.user_requirements import install_user_requirements
from mwaa.validation.report import (
    REQUIREMENTS_FAILED,
    REQUIREMENTS_OK,
    REQUIREMENTS_SKIPPED,
    build_report,
    exit_code,
    format_summary,
)

logger = logging.getLogger(__name__)

AIRFLOW_HOME = "/usr/local/airflow"
REQUIREMENTS_FAILED_MESSAGE = "pip install failed; see the pip output in the container log"

# How long the parser may take to start, import Airflow, load plugins and list the DAG
# files. It then reports how many files it found and how long one file may take.
PARSER_SETUP_TIMEOUT_SECONDS = 300.0
# Extra time the parser gets beyond its per-file limits, to write its result.
PARSER_FINISH_MARGIN_SECONDS = 60.0
# The only AWS variables DAG and plugin code sees while it is parsed. A region lets code
# create AWS clients at import; credentials are left out so it can't call AWS.
PARSER_AWS_VARIABLES = ("AWS_DEFAULT_REGION", "AWS_REGION")


@dataclass(frozen=True)
class ValidationPaths:
    """Container paths used by the test-dags command."""

    dags_folder: str = f"{AIRFLOW_HOME}/dags"
    plugins_input: str = f"{AIRFLOW_HOME}/plugins-input"
    plugins_folder: str = f"{AIRFLOW_HOME}/plugins"
    report_path: str = f"{AIRFLOW_HOME}/validation-report.json"


class ValidationInputError(Exception):
    """Raised when a mounted input cannot be used."""


class ParserChildError(Exception):
    """Raised when the DAG parser child process does not produce a result."""


def _extract_zip(zip_path: str, destination: str) -> None:
    real_destination = os.path.realpath(destination)
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            target = os.path.realpath(os.path.join(real_destination, name))
            if target != real_destination and not target.startswith(real_destination + os.sep):
                raise ValidationInputError(
                    f"plugins.zip entry {name!r} would be extracted outside the plugins folder."
                )
        archive.extractall(real_destination)


def materialize_plugins(source: str, destination: str) -> None:
    """
    Copy a plugins folder, or extract a plugins.zip, into the plugins folder.

    :param source: The mounted plugins input. It may be missing.
    :param destination: The plugins folder Airflow loads plugins from.
    :raises ValidationInputError: If the input is neither a folder nor a zip file, or
      a zip entry would land outside the destination.
    """
    os.makedirs(destination, exist_ok=True)
    if not os.path.exists(source):
        return
    if os.path.isdir(source):
        shutil.copytree(source, destination, dirs_exist_ok=True)
    elif zipfile.is_zipfile(source):
        _extract_zip(source, destination)
    else:
        raise ValidationInputError(f"Plugins input {source} is neither a folder nor a zip file.")


def prepare_plugins(paths: ValidationPaths = ValidationPaths()) -> Optional[str]:
    """
    Copy or unzip the mounted plugins input into the plugins folder.

    :param paths: The container paths; tests pass temporary ones.
    :returns: None, or an error message if the plugins input can't be used.
    """
    try:
        materialize_plugins(paths.plugins_input, paths.plugins_folder)
    except (ValidationInputError, OSError, zipfile.BadZipFile) as error:
        logger.error("Could not prepare the plugins folder: %s", error)
        return str(error)
    return None


def parser_environment(environ: Dict[str, str], paths: ValidationPaths) -> Dict[str, str]:
    """
    Return the environment for the parser child.

    DAG and plugin code runs in the child, so it gets no AWS credentials: every AWS_*
    variable except the region is removed, and boto3 is told not to fetch credentials
    from the EC2 instance metadata service, which would give it the host's role.

    :param environ: The environment from setup_environment_variables.
    :param paths: The container paths.
    """
    env = {
        name: value
        for name, value in environ.items()
        if not name.startswith("AWS_") or name in PARSER_AWS_VARIABLES
    }
    env.update(
        {
            "AWS_EC2_METADATA_DISABLED": "true",
            "AIRFLOW__CORE__DAGS_FOLDER": paths.dags_folder,
            "AIRFLOW__CORE__PLUGINS_FOLDER": paths.plugins_folder,
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            # The DAGs folder is mounted read-only.
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def _read_line(read_fd: int, timeout_seconds: float) -> Tuple[bytes, bool]:
    """
    Read from read_fd until a newline, end of file or the timeout.

    :returns: The bytes read, and whether the timeout ran out first.
    """
    deadline = time.monotonic() + timeout_seconds
    data = b""
    while b"\n" not in data:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([read_fd], [], [], remaining)[0]:
            return data, True
        chunk = os.read(read_fd, 4096)
        if not chunk:
            break
        data += chunk
    return data, False


def _stop(child: subprocess.Popen[bytes]) -> None:
    # The parser's workers run in their own sessions. They stop with the container,
    # which exits right after test-dags returns.
    child.kill()
    child.wait()


def _parse_timeout_seconds(ready_line: bytes) -> float:
    """
    Return how long the parser may take after it is ready.

    :param ready_line: The parser's ready message, or b"" if it exited before sending one.
    :raises ParserChildError: If the message is not valid.
    """
    if not ready_line:
        # The parser exited during setup; waiting for it returns right away.
        return PARSER_FINISH_MARGIN_SECONDS
    try:
        ready = json.loads(ready_line)
        return PARSER_FINISH_MARGIN_SECONDS + float(ready["files"]) * float(
            ready["file_timeout_seconds"]
        )
    except (ValueError, KeyError, TypeError) as error:
        raise ParserChildError(f"DAG parser sent an invalid ready message: {error}") from error


def run_parser_child(environ: Dict[str, str], paths: ValidationPaths) -> Dict[str, Any]:
    """
    Run mwaa.validation.parse_dags in a child process and return its result.

    The child is started with ``-W default`` so Airflow does not install its own
    warning filters. Its stdout and stderr go to the container log.

    The child gets two time limits. Starting, importing Airflow, loading plugins and
    listing the DAG files must finish within PARSER_SETUP_TIMEOUT_SECONDS. The child
    then reports how many files it found and how long one file may take, and parsing
    must finish within that many per-file limits plus PARSER_FINISH_MARGIN_SECONDS.

    :param environ: The environment from setup_environment_variables.
    :param paths: The container paths.
    :raises ParserChildError: If the child fails, runs out of time or writes no valid
      result.
    """
    with tempfile.TemporaryDirectory(prefix="mwaa-test-dags-") as tmp_dir:
        output = os.path.join(tmp_dir, "result.json")
        read_fd, write_fd = os.pipe()
        command = [
            sys.executable, "-W", "default", "-m", "mwaa.validation.parse_dags",
            "--dags-folder", paths.dags_folder,
            "--plugins-folder", paths.plugins_folder,
            "--output", output,
            "--ready-fd", str(write_fd),
        ]
        try:
            child = subprocess.Popen(
                command, env=parser_environment(environ, paths), pass_fds=(write_fd,)
            )
        except BaseException:
            os.close(read_fd)
            raise
        finally:
            os.close(write_fd)
        try:
            ready_line, timed_out = _read_line(read_fd, PARSER_SETUP_TIMEOUT_SECONDS)
        finally:
            os.close(read_fd)
        if timed_out:
            _stop(child)
            raise ParserChildError(
                "Starting Airflow and loading plugins did not finish within "
                f"{PARSER_SETUP_TIMEOUT_SECONDS:g}s; see the container log."
            )
        try:
            parse_timeout = _parse_timeout_seconds(ready_line)
        except ParserChildError:
            _stop(child)
            raise
        try:
            returncode = child.wait(timeout=parse_timeout)
        except subprocess.TimeoutExpired as error:
            _stop(child)
            raise ParserChildError(
                f"Parsing the DAG files did not finish within {parse_timeout:g}s; "
                "see the container log."
            ) from error
        if returncode != 0:
            raise ParserChildError(
                f"DAG parser exited with code {returncode}; see the container log."
            )
        try:
            with open(output, encoding="utf-8") as result_file:
                return json.load(result_file)
        except (OSError, ValueError) as error:
            raise ParserChildError(f"DAG parser did not write a valid result: {error}") from error


def write_report(report: Dict[str, Any], report_path: str) -> None:
    """
    Write the report in place.

    The host bind-mounts this exact file, so it is truncated and rewritten rather than
    replaced.

    :param report: The report to write.
    :param report_path: Where to write it.
    """
    with open(report_path, "w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2)
        report_file.write("\n")


def _finish(environ: Dict[str, str], paths: ValidationPaths, **fields: Any) -> int:
    """
    Build and write the report, print the summary and return the exit code.

    :param environ: The container environment. It gives the Airflow version and the
      host paths run.sh passes for the summary.
    :param paths: The container paths.
    :param fields: The report fields, as build_report takes them.
    """
    report = build_report(
        airflow_version=environ.get("AIRFLOW_VERSION", "unknown"),
        python_version=platform.python_version(),
        **fields,
    )
    write_report(report, paths.report_path)
    # run.sh passes the host paths of the report and the DAGs folder, which are the ones
    # users can open.
    display_path = environ.get("MWAA_TEST_REPORT_DISPLAY_PATH") or paths.report_path
    dags_display = environ.get("MWAA_TEST_DAGS_DISPLAY_PATH") or paths.dags_folder
    print(format_summary(report, display_path, dags_display), flush=True)
    return exit_code(report)


def write_error_report(
    environ: Dict[str, str], error: str, paths: ValidationPaths = ValidationPaths()
) -> int:
    """
    Write a report for a check that failed before validation started, and return 1.

    The entrypoint uses this when prepare_plugins fails, before the startup script runs.

    :param environ: The container environment.
    :param error: Why the check failed. It becomes the report's top-level error.
    :param paths: The container paths; tests pass temporary ones.
    """
    return _finish(environ, paths, requirements_status=REQUIREMENTS_SKIPPED, error=error)


async def run_dag_validation(
    environ: Dict[str, str], paths: ValidationPaths = ValidationPaths()
) -> int:
    """
    Validate requirements, plugins and DAGs, write the report and return the exit code.

    Call prepare_plugins first.

    :param environ: The environment from setup_environment_variables.
    :param paths: The container paths; tests pass temporary ones.
    :returns: 0, 1 or 2, as decided by mwaa.validation.report.exit_code.
    """

    def finish(**fields: Any) -> int:
        return _finish(environ, paths, **fields)

    requirements_status = REQUIREMENTS_SKIPPED
    requirements_file = environ.get("MWAA__CORE__REQUIREMENTS_PATH", "")
    if requirements_file and os.path.isfile(requirements_file):
        if not await install_user_requirements("test-dags", environ):
            return finish(
                requirements_status=REQUIREMENTS_FAILED,
                requirements_error=REQUIREMENTS_FAILED_MESSAGE,
            )
        requirements_status = REQUIREMENTS_OK

    try:
        result = run_parser_child(environ, paths)
    except ParserChildError as error:
        logger.error("%s", error)
        return finish(requirements_status=requirements_status, error=str(error))

    return finish(
        requirements_status=requirements_status,
        plugin_import_errors=result["plugins"]["import_errors"],
        dags=result["dags"],
        parse_mode=result["parse_mode"],
    )
