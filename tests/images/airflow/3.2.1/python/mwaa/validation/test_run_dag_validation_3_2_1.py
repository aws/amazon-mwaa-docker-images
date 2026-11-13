"""Tests for mwaa.validation.run_dag_validation."""

import json
import os
import subprocess
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from mwaa.validation import run_dag_validation as rdv

GOOD_ENTRY = {"file": "good.py", "dag_ids": ["good"], "import_error": None, "import_error_traceback": None, "warnings": []}
BROKEN_ENTRY = {**GOOD_ENTRY, "file": "broken.py", "dag_ids": [], "import_error": "ModuleNotFoundError: No module named 'x'"}


def _child_result(*dags, plugin_errors=()):
    return {"plugins": {"import_errors": list(plugin_errors)}, "dags": list(dags), "parse_mode": "fork"}


@pytest.fixture
def paths(tmp_path):
    """Temporary container paths, with an empty report file as run.sh creates it."""
    report = tmp_path / "validation-report.json"
    report.write_text("")
    (tmp_path / "dags").mkdir()
    return rdv.ValidationPaths(
        dags_folder=str(tmp_path / "dags"),
        plugins_input=str(tmp_path / "plugins-input"),
        plugins_folder=str(tmp_path / "plugins"),
        report_path=str(report),
    )


@pytest.fixture
def environ(tmp_path):
    """The environment setup_environment_variables would return."""
    return {"AIRFLOW_VERSION": "3.3.1", "MWAA__CORE__REQUIREMENTS_PATH": str(tmp_path / "requirements.txt")}


def _read_report(paths):
    with open(paths.report_path) as report_file:
        return json.load(report_file)


@pytest.mark.asyncio
async def test_requirements_failure_exits_2_without_parsing(paths, environ, tmp_path, capsys):
    """A failed install writes the report and skips parsing."""
    (tmp_path / "requirements.txt").write_text("requests==0.0.0\n")
    with patch.object(rdv, "install_user_requirements", AsyncMock(return_value=False)) as install, \
            patch.object(rdv, "run_parser_child") as child:
        assert await rdv.run_dag_validation(environ, paths) == 2
    install.assert_awaited_once_with("test-dags", environ)
    child.assert_not_called()
    report = _read_report(paths)
    assert report["requirements"] == {"status": "failed", "error": rdv.REQUIREMENTS_FAILED_MESSAGE}
    assert report["airflow_version"] == "3.3.1"
    assert report["dags"] == []
    assert report["plugins"] == {"import_errors": []}
    assert report["summary"]["files"] == 0
    assert "exit code: 2" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_installed_requirements_then_parse(paths, environ, tmp_path, capsys):
    """After a successful install the parser result becomes the report."""
    (tmp_path / "requirements.txt").write_text("requests\n")
    with patch.object(rdv, "install_user_requirements", AsyncMock(return_value=True)), \
            patch.object(rdv, "run_parser_child", return_value=_child_result(GOOD_ENTRY)) as child:
        assert await rdv.run_dag_validation(environ, paths) == 0
    child.assert_called_once_with(environ, paths)
    report = _read_report(paths)
    assert report["requirements"]["status"] == "ok"
    assert report["dags"] == [GOOD_ENTRY]
    assert report["summary"] == {"files": 1, "dags": 1, "import_errors": 0, "plugin_import_errors": 0, "warnings": 0}
    assert capsys.readouterr().out.splitlines()[-1] == "exit code: 0"


@pytest.mark.asyncio
async def test_missing_requirements_file_is_skipped(paths, environ):
    """With no requirements file, nothing is installed."""
    with patch.object(rdv, "install_user_requirements", AsyncMock()) as install, \
            patch.object(rdv, "run_parser_child", return_value=_child_result(GOOD_ENTRY)):
        assert await rdv.run_dag_validation(environ, paths) == 0
    install.assert_not_awaited()
    assert _read_report(paths)["requirements"] == {"status": "skipped", "error": None}


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [
    _child_result(GOOD_ENTRY, BROKEN_ENTRY),
    _child_result(GOOD_ENTRY, plugin_errors=[{"file": "bad_plugin.py", "error": "boom"}]),
])
async def test_import_errors_exit_1(paths, environ, result):
    """A DAG import error or a plugin import error gives exit code 1."""
    with patch.object(rdv, "run_parser_child", return_value=result):
        assert await rdv.run_dag_validation(environ, paths) == 1


@pytest.mark.asyncio
async def test_parser_failure_is_a_top_level_error(paths, environ):
    """If the parser child fails, the report carries the error and the exit code is 1."""
    with patch.object(rdv, "run_parser_child", side_effect=rdv.ParserChildError("DAG parser exited with code 1")):
        assert await rdv.run_dag_validation(environ, paths) == 1
    report = _read_report(paths)
    assert report["error"] == "DAG parser exited with code 1"
    assert report["dags"] == []


@pytest.mark.asyncio
async def test_report_is_written_in_place(paths, environ):
    """The bind-mounted report file keeps its inode."""
    inode = os.stat(paths.report_path).st_ino
    with patch.object(rdv, "run_parser_child", return_value=_child_result(GOOD_ENTRY)):
        await rdv.run_dag_validation(environ, paths)
    assert os.stat(paths.report_path).st_ino == inode
    assert _read_report(paths)["schema_version"] == 1


@pytest.mark.asyncio
async def test_bad_plugins_input_is_a_top_level_error(paths, environ, tmp_path):
    """A plugins input that is neither a folder nor a zip stops validation before install."""
    (tmp_path / "plugins-input").write_text("not a zip")
    (tmp_path / "requirements.txt").write_text("requests\n")
    with patch.object(rdv, "install_user_requirements", AsyncMock()) as install, \
            patch.object(rdv, "run_parser_child") as child:
        assert await rdv.run_dag_validation(environ, paths) == 1
    install.assert_not_awaited()
    child.assert_not_called()
    report = _read_report(paths)
    assert "neither a folder nor a zip file" in report["error"]
    assert report["requirements"]["status"] == "skipped"


def test_plugins_folder_is_copied(tmp_path):
    """A plugins folder is copied, subfolders included."""
    (tmp_path / "in" / "sub").mkdir(parents=True)
    (tmp_path / "in" / "sub" / "p.py").write_text("x = 1\n")
    rdv.materialize_plugins(str(tmp_path / "in"), str(tmp_path / "out"))
    assert (tmp_path / "out" / "sub" / "p.py").read_text() == "x = 1\n"


def test_plugins_zip_is_extracted(tmp_path):
    """A plugins.zip is extracted into the plugins folder."""
    with zipfile.ZipFile(tmp_path / "plugins.zip", "w") as archive:
        archive.writestr("p.py", "x = 1\n")
        archive.writestr("pkg/__init__.py", "")
    rdv.materialize_plugins(str(tmp_path / "plugins.zip"), str(tmp_path / "out"))
    assert (tmp_path / "out" / "p.py").read_text() == "x = 1\n"
    assert (tmp_path / "out" / "pkg" / "__init__.py").exists()


@pytest.mark.parametrize("entry", ["../evil.py", "/abs_evil.py"])
def test_zip_entry_outside_the_plugins_folder_is_rejected(tmp_path, entry):
    """A zip entry like ../evil.py is refused and nothing is extracted."""
    with zipfile.ZipFile(tmp_path / "plugins.zip", "w") as archive:
        archive.writestr("ok.py", "")
        archive.writestr(entry, "")
    with pytest.raises(rdv.ValidationInputError, match="outside the plugins folder"):
        rdv.materialize_plugins(str(tmp_path / "plugins.zip"), str(tmp_path / "out"))
    assert not (tmp_path / "evil.py").exists() and not os.path.exists("/abs_evil.py")
    assert not (tmp_path / "out" / "ok.py").exists()


def test_missing_plugins_input_leaves_an_empty_plugins_folder(tmp_path):
    """Without a plugins input, the plugins folder exists and is empty."""
    rdv.materialize_plugins(str(tmp_path / "missing"), str(tmp_path / "out"))
    assert list((tmp_path / "out").iterdir()) == []


def test_run_parser_child_runs_parse_dags_with_warnings_enabled(paths, environ):
    """The child runs with -W default and with Airflow pointed at the test folders."""
    seen = {}

    def fake_run(command, env, check):
        seen.update(command=command, env=env)
        with open(command[command.index("--output") + 1], "w") as output:
            json.dump(_child_result(GOOD_ENTRY), output)
        return subprocess.CompletedProcess(command, 0)

    with patch.object(rdv.subprocess, "run", side_effect=fake_run):
        assert rdv.run_parser_child(environ, paths) == _child_result(GOOD_ENTRY)
    assert seen["command"][1:5] == ["-W", "default", "-m", "mwaa.validation.parse_dags"]
    assert seen["env"]["AIRFLOW__CORE__DAGS_FOLDER"] == paths.dags_folder
    assert seen["env"]["AIRFLOW__CORE__PLUGINS_FOLDER"] == paths.plugins_folder
    assert seen["env"]["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False"
    assert seen["env"]["PYTHONDONTWRITEBYTECODE"] == "1"
    assert seen["env"]["AIRFLOW_VERSION"] == "3.3.1"


def test_run_parser_child_non_zero_exit(paths, environ):
    """A child that exits non-zero is a parser error."""
    with patch.object(rdv.subprocess, "run", return_value=subprocess.CompletedProcess([], 3)):
        with pytest.raises(rdv.ParserChildError, match="exited with code 3"):
            rdv.run_parser_child(environ, paths)


def test_run_parser_child_without_output(paths, environ):
    """A child that exits 0 without writing its result is a parser error."""
    with patch.object(rdv.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
        with pytest.raises(rdv.ParserChildError, match="did not write a valid result"):
            rdv.run_parser_child(environ, paths)
