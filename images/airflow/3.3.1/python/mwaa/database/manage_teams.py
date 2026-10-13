"""
Reconcile Airflow teams (multi-team mode) with ``TEAM_NAMES`` during ``migrate-db``.

``TEAM_NAMES`` is the comma-separated list of desired teams:

* Listed teams that don't exist are created (``airflow teams create``).
* Existing teams that aren't listed are deleted, with the resources they own
  (see ``mwaa.database.delete_teams``).

An empty ``TEAM_NAMES`` deletes nothing. Deleting all teams requires
``DELETE_ALL_TEAMS=true`` with an empty ``TEAM_NAMES``.

Imported and awaited from ``entrypoint.py``. For local testing, it can also be
run directly::

    python3 -m mwaa.database.manage_teams
"""

# Python imports
import asyncio
import json
import logging
import os
import shlex

# Our imports
from mwaa.utils.cmd import run_command
from mwaa.utils.dblock import with_db_lock

# Hard-code the module path for logging since __name__ will be '__main__' when
# run as a script.
logger = logging.getLogger("mwaa.database.manage_teams")

# Comma-separated list of desired teams. The environment template defaults it to
# "", so empty means "teams are not managed here" rather than "delete all teams".
TEAM_NAMES_ENV_VAR = "TEAM_NAMES"

# Must be "true" (with an empty TEAM_NAMES) to delete all teams.
DELETE_ALL_TEAMS_ENV_VAR = "DELETE_ALL_TEAMS"

# Printed by `airflow teams list` (regardless of --output) when no teams exist.
NO_TEAMS_LIST_MSG = "No teams found."

# Must be "true" for teams to be managed; it drives AIRFLOW__CORE__MULTI_TEAM.
USE_MULTI_TEAM_ENV_VAR = "USE_MULTI_TEAM"


def _parse_team_names(raw: str | None) -> list[str]:
    """
    Parse a comma-separated list of team names.

    Whitespace and blank entries are dropped, and duplicates removed, keeping the
    original order.

    :param raw: The raw environment variable value, or None if unset.

    :returns: The team names.
    """
    if not raw:
        return []
    names = (part.strip() for part in raw.split(","))
    return list(dict.fromkeys(name for name in names if name))


async def _list_existing_teams(environ: dict[str, str]) -> set[str]:
    """
    Return the names of the teams that exist in Airflow.

    :param environ: A dictionary containing the environment variables.

    :returns: The existing team names.
    """
    captured: list[str] = []

    await run_command(
        "airflow teams list --output json",
        env=environ,
        # Capture stdout to parse it; stderr is still logged.
        stdout_logging_method=captured.append,
    )

    output = "\n".join(captured).strip()
    if not output:
        return set()

    # With no teams, a plain-text message is printed instead of JSON.
    if NO_TEAMS_LIST_MSG in output.splitlines():
        return set()

    teams = _find_json_list_of_objects(output)
    if teams is None:
        logger.error(
            "Could not parse the output of 'airflow teams list'. Output was: %s",
            output,
        )
        raise ValueError("Could not parse the output of 'airflow teams list'.")

    return {team["name"] for team in teams if team.get("name")}


def _find_json_list_of_objects(output: str) -> list[dict] | None:
    """
    Return the first JSON list of objects in ``output``, or None if there is none.

    Non-JSON lines (e.g. warnings, which can themselves start with ``[``) can
    precede the JSON, so each ``[`` is tried in turn.
    """
    decoder = json.JSONDecoder()
    start = output.find("[")
    while start != -1:
        try:
            value, _ = decoder.raw_decode(output, start)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(value, list) and all(isinstance(v, dict) for v in value):
                return value
        start = output.find("[", start + 1)
    return None


async def _create_team(team: str, environ: dict[str, str]) -> None:
    """Create a team via the Airflow CLI."""
    logger.info("Creating team '%s'.", team)
    await run_command(
        f"airflow teams create {shlex.quote(team)}",
        env=environ,
    )


async def _delete_teams(teams: list[str], environ: dict[str, str]) -> None:
    """Delete teams and the resources they own, in a single transaction."""
    logger.info("Teams %s are not in the desired list. Deleting.", teams)
    await run_command(
        "python3 -m mwaa.database.delete_teams " + " ".join(shlex.quote(t) for t in teams),
        env=environ,
    )


async def reconcile_teams(environ: dict[str, str]) -> None:
    """
    Make Airflow's teams match ``TEAM_NAMES``.

    Does nothing, without connecting to the database, when multi-team mode is
    disabled or ``TEAM_NAMES`` is empty, unless ``DELETE_ALL_TEAMS`` is "true".
    Deleting doesn't need multi-team mode, so all teams can be deleted in the
    same update that disables it.

    :param environ: A dictionary containing the environment variables.

    :raises ValueError: If ``DELETE_ALL_TEAMS`` is "true" but ``TEAM_NAMES`` is
      not empty.
    """
    multi_team_enabled = environ.get(USE_MULTI_TEAM_ENV_VAR, "").lower() == "true"
    delete_all_teams = environ.get(DELETE_ALL_TEAMS_ENV_VAR, "").lower() == "true"

    desired_teams = _parse_team_names(environ.get(TEAM_NAMES_ENV_VAR))
    if delete_all_teams and desired_teams:
        raise ValueError(
            f"{DELETE_ALL_TEAMS_ENV_VAR} is 'true' but {TEAM_NAMES_ENV_VAR} is not "
            f"empty ({desired_teams}). Set {TEAM_NAMES_ENV_VAR} to empty to delete "
            f"all teams, or unset {DELETE_ALL_TEAMS_ENV_VAR}."
        )

    if not desired_teams and not delete_all_teams:
        logger.info("%s is not set or empty. Skipping team reconciliation.", TEAM_NAMES_ENV_VAR)
        return

    if not multi_team_enabled and not delete_all_teams:
        logger.warning(
            "%s was provided but multi-team mode is disabled "
            "(%s is not 'true'). Skipping team reconciliation.",
            TEAM_NAMES_ENV_VAR,
            USE_MULTI_TEAM_ENV_VAR,
        )
        return

    if delete_all_teams:
        logger.warning("%s is 'true'. Deleting all teams.", DELETE_ALL_TEAMS_ENV_VAR)

    # With DELETE_ALL_TEAMS, desired_teams is empty, so every team is deleted.
    await _reconcile_teams_with_lock(desired_teams, environ)


@with_db_lock(9012)
async def _reconcile_teams_with_lock(desired_teams: list[str], environ: dict[str, str]) -> None:
    """Create and delete teams so Airflow matches ``desired_teams``, under a DB lock."""
    desired_set = set(desired_teams)
    logger.info("Desired Airflow teams: %s", desired_teams)

    existing_teams = await _list_existing_teams(environ)
    logger.info("Existing Airflow teams: %s", sorted(existing_teams) or "none")

    # Each create is a separate Airflow process, so time grows linearly with the
    # number of teams created (fine for the few teams expected). Deletes run in
    # a single process.
    for team in desired_teams:
        if team in existing_teams:
            logger.info("Team '%s' already exists. Skipping creation.", team)
            continue
        await _create_team(team, environ)

    teams_to_delete = sorted(existing_teams - desired_set)
    if teams_to_delete:
        await _delete_teams(teams_to_delete, environ)


def _main() -> None:
    """Entry point when run directly via ``python3 -m mwaa.database.manage_teams``."""
    # Only entrypoint.py configures logging, so INFO logs would be dropped here.
    logging.basicConfig(level=logging.INFO)
    asyncio.run(reconcile_teams({**os.environ}))


if __name__ == "__main__":
    _main()
