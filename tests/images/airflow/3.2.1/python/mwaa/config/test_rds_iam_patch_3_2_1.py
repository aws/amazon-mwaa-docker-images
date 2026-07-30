import pytest
from unittest.mock import patch, MagicMock


def test_is_from_migrate_db():
    """Test migrate-db detection"""
    import importlib
    import mwaa.config.rds_iam_patch

    with patch.dict("os.environ", {"MWAA_AIRFLOW_COMPONENT": "migrate-db"}):
        importlib.reload(mwaa.config.rds_iam_patch)
        from mwaa.config.rds_iam_patch import _is_from_migrate_db

        assert _is_from_migrate_db() is True

    with patch.dict("os.environ", {"MWAA_AIRFLOW_COMPONENT": "scheduler"}):
        importlib.reload(mwaa.config.rds_iam_patch)
        from mwaa.config.rds_iam_patch import _is_from_migrate_db

        assert _is_from_migrate_db() is False

    with patch.dict("os.environ", {}, clear=True):
        importlib.reload(mwaa.config.rds_iam_patch)
        from mwaa.config.rds_iam_patch import _is_from_migrate_db

        assert _is_from_migrate_db() is False


def test_is_using_rds_proxy():
    """Test RDS proxy detection via is_using_rds_proxy helper"""
    from mwaa.config.rds_iam_credentials import is_using_rds_proxy

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "require"}):
        assert is_using_rds_proxy() is True

    with patch.dict("os.environ", {"MWAA__DB__POSTGRES_SSLMODE": "disable"}):
        assert is_using_rds_proxy() is False

    with patch.dict("os.environ", {}, clear=True):
        assert is_using_rds_proxy() is False


def test_use_iam_credentials():
    """Test IAM credentials flag detection"""
    from mwaa.config.rds_iam_credentials import use_iam_credentials

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "true"}):
        assert use_iam_credentials() is True

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}):
        assert use_iam_credentials() is False

    with patch.dict("os.environ", {}, clear=True):
        assert use_iam_credentials() is False


def test_is_accessing_metadata_db():
    """Test metadata database detection"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url"
    ) as mock_get_url:
        mock_url = MagicMock()
        mock_url.host = "test.rds.amazonaws.com"
        mock_url.database = "airflow"
        mock_url.port = 5432
        mock_get_url.return_value = mock_url

        # Test with matching parameters including username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is True

        # Test with adminuser username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "adminuser",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is True

        # Test with non-matching parameters
        cparams = {
            "host": "other.rds.amazonaws.com",
            "database": "other",
            "port": 3306,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False

        # Test with missing username
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False


def test_is_accessing_metadata_db_with_cargs():
    """Test metadata database detection using cargs"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url"
    ) as mock_get_url:
        mock_url = MagicMock()
        mock_url.host = "test.rds.amazonaws.com"
        mock_url.database = "airflow"
        mock_url.port = 5432
        mock_get_url.return_value = mock_url

        # Test with matching cargs including airflow_user
        cargs = ["postgresql://airflow_user:pass@test.rds.amazonaws.com:5432/airflow"]
        assert _is_accessing_metadata_db("postgresql", cargs, {}) is True

        # Test with matching cargs including adminuser
        cargs = ["postgresql://adminuser:pass@test.rds.amazonaws.com:5432/airflow"]
        assert _is_accessing_metadata_db("postgresql", cargs, {}) is True


def test_is_accessing_metadata_db_no_conn_str():
    """Test metadata database detection when no connection string is set"""
    from mwaa.config.rds_iam_patch import _is_accessing_metadata_db

    with patch(
        "mwaa.config.rds_iam_patch._get_metadata_url", return_value=None
    ):
        cparams = {
            "host": "test.rds.amazonaws.com",
            "database": "airflow",
            "port": 5432,
            "username": "airflow_user",
        }
        assert _is_accessing_metadata_db("postgresql", [], cparams) is False


def test_install_rds_iam_patch_not_installed_no_iam():
    """Test patch not installed when USE_IAM_CREDENTIALS is not true"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict("os.environ", {"USE_IAM_CREDENTIALS": "false"}, clear=True), patch(
        "sqlalchemy.event.listen"
    ) as mock_listen:
        install_rds_iam_patch()
        mock_listen.assert_not_called()


def test_install_rds_iam_patch_not_installed_no_rds_proxy():
    """Test patch not installed when not using RDS Proxy"""
    from mwaa.config.rds_iam_patch import install_rds_iam_patch

    with patch.dict(
        "os.environ",
        {"USE_IAM_CREDENTIALS": "true", "MWAA__DB__POSTGRES_SSLMODE": "disable"},
    ), patch("sqlalchemy.event.listen") as mock_listen:
        install_rds_iam_patch()
        mock_listen.assert_not_called()


def test_install_rds_iam_patch_not_installed_migrate_db():
    """Test patch not installed for migrate-db process"""
    import importlib
    import mwaa.config.rds_iam_patch

    with patch.dict(
        "os.environ",
        {
            "USE_IAM_CREDENTIALS": "true",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
            "MWAA_AIRFLOW_COMPONENT": "migrate-db",
        },
    ):
        importlib.reload(mwaa.config.rds_iam_patch)
        from mwaa.config.rds_iam_patch import install_rds_iam_patch

        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            mock_listen.assert_not_called()


def test_install_rds_iam_patch_installed():
    """Test patch installed when all conditions are met"""
    import importlib
    import mwaa.config.rds_iam_patch

    with patch.dict(
        "os.environ",
        {
            "USE_IAM_CREDENTIALS": "true",
            "MWAA__DB__POSTGRES_SSLMODE": "require",
            "MWAA_AIRFLOW_COMPONENT": "scheduler",
        },
    ):
        importlib.reload(mwaa.config.rds_iam_patch)
        from mwaa.config.rds_iam_patch import install_rds_iam_patch

        with patch("sqlalchemy.event.listen") as mock_listen:
            install_rds_iam_patch()
            mock_listen.assert_called_once()
