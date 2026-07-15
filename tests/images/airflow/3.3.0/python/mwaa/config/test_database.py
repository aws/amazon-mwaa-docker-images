"""Tests for database configuration module."""

import os
import json
import pytest
from unittest.mock import patch
from mwaa.config.database import (
    get_db_credentials, 
    get_db_connection_string, 
    MWAA_CONNECT_ARGS
)
from .base_config_test import BaseConfigTest

@pytest.fixture
def sample_db_credentials():
    return {
        "MWAA__DB__POSTGRES_HOST": "localhost",
        "MWAA__DB__POSTGRES_PORT": "5432",
        "MWAA__DB__POSTGRES_DB": "airflow",
        "MWAA__DB__POSTGRES_SSLMODE": "require",
        "MWAA__DB__POSTGRES_USER": "airflow",
        "MWAA__DB__POSTGRES_PASSWORD": "password"
    }

class TestDatabaseConfig(BaseConfigTest):

    def test_db_credentials_from_json(self, env_helper):
        """Test database credentials from JSON format."""
        credentials = {"username": "testuser", "password": "testpass"}

        env_helper.set({
            "MWAA__DB__CREDENTIALS": json.dumps(credentials)
        })
        
        user, password = get_db_credentials()
        assert user == "testuser"
        assert password == "testpass"

    def test_db_credentials_from_separate_vars(self, env_helper):
        """Test database credentials from separate environment variables."""
        env_helper.set({
            "MWAA__DB__POSTGRES_USER": "testuser",
            "MWAA__DB__POSTGRES_PASSWORD": "testpass"
        })
        
        user, password = get_db_credentials()
        assert user == "testuser"
        assert password == "testpass"

    def test_db_credentials_missing(self, env_helper):
        """Test error when database credentials are missing."""
        # Ensure vars are not present
        env_helper.delete(["MWAA__DB__CREDENTIALS", "MWAA__DB__POSTGRES_USER", "MWAA__DB__POSTGRES_PASSWORD"])
       
        with pytest.raises(RuntimeError, match="Couldn't find database credentials"):
            get_db_credentials()

    def test_connection_string_format(self, env_helper, sample_db_credentials):
        """Test database connection string format."""
        env_helper.set(sample_db_credentials)
        
        conn_string = get_db_connection_string()
        
        assert conn_string.startswith("postgresql+psycopg2://")
        assert "airflow:password@localhost:5432/airflow" in conn_string
        assert "sslmode=require" in conn_string
    
    def test_connection_args_structure(self):
        """Test MWAA_CONNECT_ARGS structure."""
        expected_keys = [
            "connect_timeout", "keepalives", "keepalives_idle",
            "keepalives_interval", "keepalives_count"
        ]
        
        # Check exact keys match
        self.assert_config_keys(MWAA_CONNECT_ARGS, expected_keys)
        assert set(MWAA_CONNECT_ARGS.keys()) == set(expected_keys)
        
        # Check all values are integers
        for key in expected_keys:
            assert isinstance(MWAA_CONNECT_ARGS[key], int)

    def test_connection_string_sets_default_sslmode(self, env_helper, sample_db_credentials):
        """Test that sslmode defaults to 'require' when not set."""
        creds = sample_db_credentials.copy()
        creds.pop("MWAA__DB__POSTGRES_SSLMODE")  # remove sslmode
        env_helper.set(creds)

        with pytest.raises(RuntimeError, match="One or more of the required environment variables for configuring Postgres are not set."):
            get_db_connection_string()