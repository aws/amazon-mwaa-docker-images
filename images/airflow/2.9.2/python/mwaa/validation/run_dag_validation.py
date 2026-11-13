"""
Run the test-dags command.

The command checks a customer's requirements, plugins and DAGs against this image's
Airflow version without a metadata database:

1. Copy the mounted plugins folder, or unzip a mounted plugins.zip, into the plugins
   folder, as MWAA does. This happens first because packaged requirements install
   from the plugins folder.
2. Install the requirements with install_user_requirements, as test-requirements does.
3. Load plugins and parse DAGs in a child process (mwaa.validation.parse_dags).
4. Write the JSON report, print a summary and return the exit code.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from typing import Any, Dict

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


def run_parser_child(environ: Dict[str, str], paths: ValidationPaths) -> Dict[str, Any]:
    """
    Run mwaa.validation.parse_dags in a child process and return its result.

    The child is started with ``-W default`` so Airflow does not install its own
    warning filters. Its stdout and stderr go to the container log.

    :param environ: The environment from setup_environment_variables.
    :param paths: The container paths.
    :raises ParserChildError: If the child fails or writes no valid result.
    """
    with tempfile.TemporaryDirectory(prefix="mwaa-test-dags-") as tmp_dir:
        output = os.path.join(tmp_dir, "result.json")
        env = {
            **environ,
            "AIRFLOW__CORE__DAGS_FOLDER": paths.dags_folder,
            "AIRFLOW__CORE__PLUGINS_FOLDER": paths.plugins_folder,
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            # The DAGs folder is mounted read-only.
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        command = [
            sys.executable, "-W", "default", "-m", "mwaa.validation.parse_dags",
            "--dags-folder", paths.dags_folder,
            "--plugins-folder", paths.plugins_folder,
            "--output", output,
        ]
        completed = subprocess.run(command, env=env, check=False)
        if completed.returncode != 0:
            raise ParserChildError(
                f"DAG parser exited with code {completed.returncode}; see the container log."
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


async def run_dag_validation(
    environ: Dict[str, str], paths: ValidationPaths = ValidationPaths()
) -> int:
    """
    Validate requirements, plugins and DAGs, write the report and return the exit code.

    :param environ: The environment from setup_environment_variables.
    :param paths: The container paths; tests pass temporary ones.
    :returns: 0, 1 or 2, as decided by mwaa.validation.report.exit_code.
    """
    airflow_version = environ.get("AIRFLOW_VERSION", "unknown")
    python_version = platform.python_version()

    def finish(**fields: Any) -> int:
        report = build_report(
            airflow_version=airflow_version, python_version=python_version, **fields
        )
        write_report(report, paths.report_path)
        print(format_summary(report, paths.report_path), flush=True)
        return exit_code(report)

    try:
        materialize_plugins(paths.plugins_input, paths.plugins_folder)
    except (ValidationInputError, OSError, zipfile.BadZipFile) as error:
        logger.error("Could not prepare the plugins folder: %s", error)
        return finish(requirements_status=REQUIREMENTS_SKIPPED, error=str(error))

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
