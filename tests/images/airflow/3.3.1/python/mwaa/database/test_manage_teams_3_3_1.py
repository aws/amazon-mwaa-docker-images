# test_manage_teams_3_3_1.py
"""Tests for mwaa.database.manage_teams team reconciliation logic."""

import json

import pytest
from unittest.mock import patch, MagicMock

from mwaa.database.manage_teams import (
    _parse_team_names,
    _list_existing_teams,
    reconcile_teams,
)


@pytest.fixture
def mock_db_lock():
    """Replace the @with_db_lock DB connection with a mock, so no database is needed."""
    with patch('mwaa.utils.dblock.create_engine') as mock_engine, \
            patch('mwaa.utils.dblock.get_db_connection_string',
                  return_value='postgresql://localhost/airflow'):
        engine = MagicMock()
        mock_engine.return_value = engine
        yield engine


# ------------------------
# _parse_team_names
# ------------------------
@pytest.mark.parametrize("raw,expected", [
    (None, []),
    ("", []),
    ("   ", []),
    ("team_a", ["team_a"]),
    ("team_a,team_b", ["team_a", "team_b"]),
    (" team_a , team_b ", ["team_a", "team_b"]),
    ("team_a,,team_b,", ["team_a", "team_b"]),
    ("team_a,team_a,team_b", ["team_a", "team_b"]),  # de-duplicated, order preserved
])
def test_parse_team_names(raw, expected):
    """Team name parsing handles blanks, whitespace, and duplicates."""
    assert _parse_team_names(raw) == expected


# ------------------------
# _list_existing_teams
# ------------------------
@pytest.mark.asyncio
async def test_list_existing_teams_parses_json():
    """_list_existing_teams parses the JSON output of 'airflow teams list'."""
    teams_json = json.dumps([{"name": "team_a"}, {"name": "team_b"}])

    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        assert "airflow teams list --output json" == cmd
        for line in teams_json.splitlines():
            stdout_logging_method(line)
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        result = await _list_existing_teams({})
        assert result == {"team_a", "team_b"}


@pytest.mark.asyncio
async def test_list_existing_teams_empty():
    """_list_existing_teams returns an empty set when no teams exist."""
    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        stdout_logging_method("[]")
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        result = await _list_existing_teams({})
        assert result == set()


@pytest.mark.parametrize("lines", [
    ["No teams found."],
    ["WARNING: some deprecation notice", "No teams found."],
])
@pytest.mark.asyncio
async def test_list_existing_teams_no_teams_message(lines):
    """_list_existing_teams treats Airflow's plain-text 'No teams found.' as no teams."""
    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        for line in lines:
            stdout_logging_method(line)
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        result = await _list_existing_teams({})
        assert result == set()


@pytest.mark.asyncio
async def test_list_existing_teams_unparseable_output_raises():
    """Output that is neither JSON nor the no-teams message still fails loudly."""
    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        stdout_logging_method("something unexpected")
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        with pytest.raises(ValueError, match="Could not parse"):
            await _list_existing_teams({})


@pytest.mark.asyncio
async def test_list_existing_teams_ignores_leading_noise():
    """_list_existing_teams recovers the JSON array when warnings precede it."""
    teams_json = json.dumps([{"name": "team_a"}])

    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        stdout_logging_method("WARNING: some deprecation notice")
        stdout_logging_method(teams_json)
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        result = await _list_existing_teams({})
        assert result == {"team_a"}


@pytest.mark.parametrize("noise", [
    "[2026-10-08T00:00:00.000+0000] {logging_mixin.py:190} WARNING - deprecated",
    "[INFO] something",
    "[1, 2]",
])
@pytest.mark.asyncio
async def test_list_existing_teams_ignores_bracketed_noise(noise):
    """Lines starting with '[' before the JSON array are skipped."""
    teams_json = json.dumps([{"name": "team_a"}])

    async def mock_run_command(cmd, env=None, stdout_logging_method=None, **kwargs):
        stdout_logging_method(noise)
        stdout_logging_method(teams_json)
        return 0

    with patch('mwaa.database.manage_teams.run_command', side_effect=mock_run_command):
        result = await _list_existing_teams({})
        assert result == {"team_a"}


# ------------------------
# reconcile_teams
# ------------------------
@pytest.mark.parametrize("environ", [
    {},
    {"USE_MULTI_TEAM": "true"},
    {"USE_MULTI_TEAM": "true", "TEAM_NAMES": ""},
    {"USE_MULTI_TEAM": "true", "TEAM_NAMES": " , "},
    {"TEAM_NAMES": "team_a"},
    {"USE_MULTI_TEAM": "false", "TEAM_NAMES": "team_a"},
    {"USE_MULTI_TEAM": "", "TEAM_NAMES": "team_a"},
    {"USE_MULTI_TEAM": "true", "TEAM_NAMES": "", "DELETE_ALL_TEAMS": "false"},
    {"USE_MULTI_TEAM": "true", "TEAM_NAMES": "", "DELETE_ALL_TEAMS": ""},
])
@pytest.mark.asyncio
async def test_reconcile_teams_noop_skips_db_lock(environ):
    """Unset/empty/disabled config -> no DB connection, no lock, no CLI calls."""
    with patch('mwaa.utils.dblock._connect_with_retry') as mock_connect, \
            patch('mwaa.database.manage_teams._list_existing_teams') as mock_list, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_connect.assert_not_called()
        mock_list.assert_not_called()
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_takes_db_lock_when_configured():
    """Configured multi-team env -> reconciliation runs under the DB lock."""
    environ = {"USE_MULTI_TEAM": "true", "TEAM_NAMES": "team_a"}
    with patch('mwaa.utils.dblock._connect_with_retry') as mock_connect, \
            patch('mwaa.utils.dblock._obtain_db_lock') as mock_obtain, \
            patch('mwaa.utils.dblock._release_db_lock') as mock_release, \
            patch('mwaa.database.manage_teams._list_existing_teams', return_value={"team_a"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_connect.assert_called_once()
        assert mock_obtain.call_args.args[1] == 9012
        mock_release.assert_called_once()
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_noop_when_var_unset(mock_db_lock):
    """TEAM_NAMES unset -> no CLI calls."""
    environ = {"USE_MULTI_TEAM": "true"}
    with patch('mwaa.database.manage_teams._list_existing_teams') as mock_list, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_list.assert_not_called()
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_noop_when_multi_team_disabled(mock_db_lock):
    """Var provided but multi-team disabled -> no CLI calls."""
    environ = {
        "USE_MULTI_TEAM": "false",
        "TEAM_NAMES": "team_a,team_b",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams') as mock_list, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_list.assert_not_called()
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_creates_missing_only(mock_db_lock):
    """Teams in the desired list but missing from Airflow are created."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_a,team_b,team_c",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_b"}) as mock_list, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        mock_list.assert_awaited_once()
        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert "airflow teams create team_a" in calls
        assert "airflow teams create team_c" in calls
        assert "airflow teams create team_b" not in calls
        assert not any("delete" in c for c in calls)
        assert len(calls) == 2


@pytest.mark.asyncio
async def test_reconcile_teams_deletes_excluded(mock_db_lock):
    """Existing teams NOT in the desired list are deleted (by exclusion)."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_a",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_a", "team_b", "team_c"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        # All excluded teams are deleted by a single delete_teams process.
        assert calls == ["python3 -m mwaa.database.delete_teams team_b team_c"]


@pytest.mark.asyncio
async def test_reconcile_teams_create_and_delete_together(mock_db_lock):
    """A single desired list drives both creation and deletion in one run."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_new",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_old"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == [
            "airflow teams create team_new",
            "python3 -m mwaa.database.delete_teams team_old",
        ]


@pytest.mark.parametrize("raw", ["", "  ", " , ,"])
@pytest.mark.asyncio
async def test_reconcile_teams_noop_when_var_empty(mock_db_lock, raw):
    """Empty or blank-only desired list -> no CLI calls (never deletes all teams)."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": raw,
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_a", "team_b"}) as mock_list, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_list.assert_not_called()
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_fully_reconciled_noop(mock_db_lock):
    """Desired list exactly matches existing teams -> no create/delete calls."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_a,team_b",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_a", "team_b"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_create_failure_propagates(mock_db_lock):
    """A failing create stops reconciliation (no later creates or deletes) and fails migrate-db."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_a,team_b",
    }

    async def mock_run_command(cmd, env=None, **kwargs):
        if cmd == "airflow teams create team_a":
            raise RuntimeError("create failed")
        return 0

    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_old"}), \
            patch('mwaa.database.manage_teams.run_command',
                  side_effect=mock_run_command) as mock_cmd:
        with pytest.raises(RuntimeError, match="create failed"):
            await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == ["airflow teams create team_a"]


@pytest.mark.asyncio
async def test_reconcile_teams_delete_failure_propagates(mock_db_lock):
    """A failing delete fails migrate-db; creates that already ran are kept."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_new",
    }

    async def mock_run_command(cmd, env=None, **kwargs):
        if cmd.startswith("python3 -m mwaa.database.delete_teams"):
            raise RuntimeError("delete failed")
        return 0

    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_old"}), \
            patch('mwaa.database.manage_teams.run_command',
                  side_effect=mock_run_command) as mock_cmd:
        with pytest.raises(RuntimeError, match="delete failed"):
            await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == [
            "airflow teams create team_new",
            "python3 -m mwaa.database.delete_teams team_old",
        ]


@pytest.mark.asyncio
async def test_reconcile_teams_quotes_shell_special_characters(mock_db_lock):
    """Team names are shell-quoted, so special characters can't inject commands.

    (Airflow then rejects them, as names must match ^[a-zA-Z0-9_-]{3,50}$.)
    """
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team a;rm -rf /",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"old $(whoami)"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == [
            "airflow teams create 'team a;rm -rf /'",
            "python3 -m mwaa.database.delete_teams 'old $(whoami)'",
        ]


@pytest.mark.asyncio
async def test_reconcile_teams_names_are_case_sensitive(mock_db_lock):
    """Team names are matched exactly: 'Team_A' and 'team_a' are different teams."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "Team_A",
    }
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_a"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == [
            "airflow teams create Team_A",
            "python3 -m mwaa.database.delete_teams team_a",
        ]


@pytest.mark.parametrize("multi_team", [{"USE_MULTI_TEAM": "true"}, {"USE_MULTI_TEAM": "false"}, {}])
@pytest.mark.parametrize("flag", ["true", "TRUE", "True"])
@pytest.mark.parametrize("raw", [None, "", " , "])
@pytest.mark.asyncio
async def test_reconcile_teams_delete_all_teams(mock_db_lock, multi_team, flag, raw):
    """DELETE_ALL_TEAMS with an empty TEAM_NAMES deletes every team, even if multi-team is off."""
    environ = {**multi_team, "DELETE_ALL_TEAMS": flag}
    if raw is not None:
        environ["TEAM_NAMES"] = raw
    with patch('mwaa.database.manage_teams._list_existing_teams',
               return_value={"team_a", "team_b"}), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)

        calls = [c.args[0] for c in mock_cmd.call_args_list]
        assert calls == ["python3 -m mwaa.database.delete_teams team_a team_b"]


@pytest.mark.asyncio
async def test_reconcile_teams_delete_all_teams_when_none_exist(mock_db_lock):
    """DELETE_ALL_TEAMS with no existing teams does nothing."""
    environ = {"USE_MULTI_TEAM": "true", "TEAM_NAMES": "", "DELETE_ALL_TEAMS": "true"}
    with patch('mwaa.database.manage_teams._list_existing_teams', return_value=set()), \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        await reconcile_teams(environ)
        mock_cmd.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_teams_delete_all_teams_with_team_names_raises():
    """DELETE_ALL_TEAMS with a non-empty TEAM_NAMES is contradictory and fails migrate-db."""
    environ = {
        "USE_MULTI_TEAM": "true",
        "TEAM_NAMES": "team_a",
        "DELETE_ALL_TEAMS": "true",
    }
    with patch('mwaa.utils.dblock._connect_with_retry') as mock_connect, \
            patch('mwaa.database.manage_teams.run_command') as mock_cmd:
        with pytest.raises(ValueError, match="DELETE_ALL_TEAMS"):
            await reconcile_teams(environ)
        mock_connect.assert_not_called()
        mock_cmd.assert_not_called()
