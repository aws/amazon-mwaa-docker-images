"""Tests for the connections MWAA creates in the metadata database."""

from unittest.mock import MagicMock, patch

from mwaa.database.default_connections import DUCKDB_DEFAULT, create_default_connections


def _session(exists):
    """A session whose lookup of a connection id finds one if ``exists``."""
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = (
        MagicMock() if exists else None
    )
    return session


def _create(session):
    """Run create_default_connections against ``session``."""
    context = MagicMock()
    context.__enter__.return_value = session
    with patch("airflow.utils.session.create_session", return_value=context):
        create_default_connections()


def test_duckdb_default_is_the_aws_connection_type_with_no_extras():
    """duckdb_default uses the duckdb_aws type, with no extras and no host."""
    assert DUCKDB_DEFAULT["conn_id"] == "duckdb_default"
    assert DUCKDB_DEFAULT["conn_type"] == "duckdb_aws"
    # AwsDuckDBHook loads httpfs and aws itself, and the image bakes them in, so no extras.
    assert "extra" not in DUCKDB_DEFAULT
    # No host, so DuckDB stays in memory.
    assert "host" not in DUCKDB_DEFAULT


def test_creates_missing_connection():
    """A missing duckdb_default is created."""
    session = _session(exists=False)

    _create(session)

    session.add.assert_called_once()
    created = session.add.call_args.args[0]
    assert created.conn_id == "duckdb_default"
    assert created.conn_type == "duckdb_aws"
    assert created.description.startswith("Created by Amazon MWAA.")


def test_leaves_existing_connection_unchanged():
    """An existing duckdb_default is not overwritten."""
    session = _session(exists=True)

    _create(session)

    session.add.assert_not_called()
