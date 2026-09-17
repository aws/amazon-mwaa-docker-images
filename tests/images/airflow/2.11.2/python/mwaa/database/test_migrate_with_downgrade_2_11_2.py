"""Tests for downgrade target-revision resolution and the no-op downgrade skip.

Airflow 2.x resolves ``db downgrade --to-version`` through
``db_command.get_version_revision``, which decrements only the patch component and so
can never step back to an earlier minor. Any ``x.y.0`` release absent from
``_REVISION_HEADS_MAP`` therefore resolves to ``None`` and the command aborts with
``SystemExit: Downgrading to version <v> is not supported.``. 2.11.0 is exactly that
case, which broke every 2.11.2 -> 2.11.0 downgrade.

``_resolve_target_revision`` backports the resolver Airflow ships in 3.x. These tests
pin the fix, and assert parity with upstream on every target upstream can already
resolve so the change cannot regress a working downgrade path.

migrate_with_downgrade.py deliberately refuses to be imported (it ends in an
``else: sys.exit(1)`` guard so it can only be run as a script), so these tests execute
the module body up to that guard in a throwaway namespace.
"""

import os
import types
from collections import Counter

import pytest
from unittest.mock import MagicMock, patch
from packaging.version import Version

from airflow.cli.commands.db_command import get_version_revision as upstream_resolve
from airflow.utils.db import _REVISION_HEADS_MAP

_AIRFLOW_VERSION = "2.11.2"

# Every Airflow version MWAA can downgrade *to* from 2.11.2, with the head each one
# must resolve to. 2.11.0 is the entry upstream cannot resolve.
MWAA_DOWNGRADE_TARGETS = {
    "2.7.2": "405de8318b3a",   # no 2.7.2 key -> 2.7.0
    "2.8.1": "88344c1d9134",   # direct hit
    "2.9.2": "686269002441",   # direct hit
    "2.10.1": "22ed7efa9da2",  # no 2.10.1 key -> 2.10.0
    "2.10.3": "5f2621c13b39",  # direct hit
    "2.11.0": "5f2621c13b39",  # no 2.11.x key at all -> 2.10.3
}

# The head shared by 2.10.3 and every 2.11.x release, since no 2.11.x release
# introduced a migration.
_HEAD_2_10_3 = "5f2621c13b39"


def _load_module():
    """Execute migrate_with_downgrade.py up to its no-import guard."""
    path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "..", "..", "..", "..",
            "images", "airflow", _AIRFLOW_VERSION,
            "python", "mwaa", "database", "migrate_with_downgrade.py",
        )
    )
    assert os.path.exists(path), f"missing module: {path}"
    source = open(path).read()
    guard = source.index('if __name__ == "__main__":')
    module = types.ModuleType("mwaa_migrate_with_downgrade_probe")
    module.__file__ = path
    exec(compile(source[:guard], path, "exec"), module.__dict__)
    return module


@pytest.fixture(scope="module")
def module():
    return _load_module()


# --- Revision resolution ---


def test_upstream_map_symbol_is_still_available():
    """Fail loudly if upstream moves, renames or empties the private map.

    _resolve_target_revision reads airflow.utils.db._REVISION_HEADS_MAP, which is
    private. Upstream's own db_command imports it too, so the coupling already exists,
    but we want a clear failure here rather than a confusing one in production.
    """
    assert _REVISION_HEADS_MAP, "airflow.utils.db._REVISION_HEADS_MAP is missing or empty"
    assert all(
        isinstance(k, str) and isinstance(v, str) for k, v in _REVISION_HEADS_MAP.items()
    )


def test_map_tops_out_below_2_11(module):
    """Documents the precondition that makes 2.11.0 unresolvable upstream."""
    assert max(_REVISION_HEADS_MAP, key=Version) == "2.10.3"
    assert not any(Version(k).release[:2] == (2, 11) for k in _REVISION_HEADS_MAP)
    assert _REVISION_HEADS_MAP["2.10.3"] == _HEAD_2_10_3


@pytest.mark.parametrize("version,expected", sorted(MWAA_DOWNGRADE_TARGETS.items()))
def test_resolves_every_mwaa_downgrade_target(module, version, expected):
    assert module._resolve_target_revision(version) == expected


@pytest.mark.parametrize("version", sorted(MWAA_DOWNGRADE_TARGETS))
def test_parity_with_upstream_where_upstream_resolves(module, version):
    """No regression: identical to upstream on every target upstream can resolve."""
    expected = upstream_resolve(version)
    if expected is None:
        pytest.skip(f"upstream cannot resolve {version} (the defect under test)")
    assert module._resolve_target_revision(version) == expected


@pytest.mark.parametrize("version", ["2.11.0", "2.11.1", "2.11.2"])
def test_regression_upstream_cannot_resolve_2_11_x(module, version):
    """Pin the defect itself.

    If upstream ever backports its 3.x resolver to the 2.11 line this test fails,
    which is the signal that _resolve_target_revision can be dropped.
    """
    assert upstream_resolve(version) is None
    assert module._resolve_target_revision(version) == _HEAD_2_10_3


def test_exhaustive_parity_sweep(module):
    """Sweep the whole 2.x space: never regress upstream, always resolve.

    Cheap insurance against a future map shape we have not anticipated.
    """
    lowest = Version(min(_REVISION_HEADS_MAP, key=Version))
    for minor in range(0, 12):
        for patch in range(0, 7):
            version = f"2.{minor}.{patch}"
            if Version(version) < lowest:
                continue
            ours = module._resolve_target_revision(version)
            assert ours, f"failed to resolve {version}"
            theirs = upstream_resolve(version)
            if theirs is not None:
                assert ours == theirs, f"diverged from upstream at {version}"


def test_below_lowest_mapped_version_raises_rather_than_guessing(module):
    with pytest.raises(RuntimeError, match="No Alembic revision mapping"):
        module._resolve_target_revision("1.10.15")


def test_resolution_is_independent_of_dict_ordering(module):
    """We sort explicitly; upstream's 3.x resolver relies on insertion order.

    Its docstring states the ordering requirement is never checked, so a shuffled map
    must not change our answer.
    """
    shuffled = dict(reversed(list(_REVISION_HEADS_MAP.items())))
    with patch.dict(module._REVISION_HEADS_MAP, shuffled, clear=True):
        assert module._resolve_target_revision("2.11.0") == _HEAD_2_10_3
        assert module._resolve_target_revision("2.10.1") == "22ed7efa9da2"


# --- The same-minor trap ---


def test_same_minor_releases_can_have_different_heads(module):
    """A same-minor pair is NOT safe to treat as schema-identical.

    2.10.3 and 2.10.1 share minor 2.10 but resolve to different revisions, so
    2.10.3 -> 2.10.1 must actually revert 5f2621c13b39. This is why the skip below
    compares resolved revisions and never version numbers.
    """
    assert module._resolve_target_revision("2.10.3") != module._resolve_target_revision("2.10.1")


def test_map_contains_multiple_entries_in_several_minors():
    """Evidence for the assertion above: within-minor migrations are the norm."""
    per_minor = Counter(
        Version(k).release[:2] for k in _REVISION_HEADS_MAP
    )
    assert sum(1 for count in per_minor.values() if count > 1) >= 5


# --- The downgrade skip ---


def _run_check_downgrade(module, *, current, target, db_heads):
    """Call _check_downgrade_db with a stubbed DB state, returning the downgrade mock."""
    downgrade_mock = MagicMock()
    env = {"AIRFLOW_VERSION": current}
    if target is not None:
        env["MWAA__DB__AIRFLOW_TARGET_VERSION"] = target

    with patch.dict("os.environ", env, clear=False), patch.object(
        module, "_current_db_heads", return_value=db_heads
    ), patch.object(module, "airflow_db_command") as db_cmd:
        db_cmd.downgrade = downgrade_mock
        if target is None:
            os.environ.pop("MWAA__DB__AIRFLOW_TARGET_VERSION", None)
        module._check_downgrade_db()
    return downgrade_mock


def test_skips_downgrade_when_db_already_at_resolved_target(module):
    """2.11.2 -> 2.11.0: same head, so there is nothing to revert.

    Before the fix this reached upstream's resolver and killed the migrate container.
    """
    downgrade = _run_check_downgrade(
        module, current="2.11.2", target="2.11.0", db_heads={_HEAD_2_10_3}
    )
    downgrade.assert_not_called()


def test_runs_downgrade_when_resolved_heads_differ(module):
    """2.10.3 -> 2.10.1: heads differ, so the downgrade must still run."""
    downgrade = _run_check_downgrade(
        module, current="2.10.3", target="2.10.1", db_heads={_HEAD_2_10_3}
    )
    downgrade.assert_called_once()
    args = downgrade.call_args.args[0]
    assert args.to_revision == "22ed7efa9da2"
    assert args.to_version is None, "must address the revision, not the version string"
    assert args.yes is True


def test_no_downgrade_when_target_is_not_lower(module):
    downgrade = _run_check_downgrade(
        module, current="2.11.2", target="2.11.2", db_heads={_HEAD_2_10_3}
    )
    downgrade.assert_not_called()


def test_no_downgrade_when_target_version_is_unset(module):
    downgrade = _run_check_downgrade(
        module, current="2.11.2", target=None, db_heads={_HEAD_2_10_3}
    )
    downgrade.assert_not_called()


def test_downgrade_runs_when_db_sits_at_an_unexpected_head(module):
    """A DB at some other single head must not be mistaken for "already there"."""
    downgrade = _run_check_downgrade(
        module, current="2.11.2", target="2.11.0", db_heads={"686269002441"}
    )
    downgrade.assert_called_once()
    assert downgrade.call_args.args[0].to_revision == _HEAD_2_10_3


def test_downgrade_runs_when_db_reports_multiple_heads(module):
    """Multiple heads is never equal to the single resolved target."""
    downgrade = _run_check_downgrade(
        module,
        current="2.11.2",
        target="2.11.0",
        db_heads={_HEAD_2_10_3, "686269002441"},
    )
    downgrade.assert_called_once()
