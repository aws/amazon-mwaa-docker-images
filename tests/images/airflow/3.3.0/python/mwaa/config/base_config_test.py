
import os
from typing import Dict

class BaseConfigTest:
    """Base class for config tests with common utilities."""

    def assert_config_keys(self, config: Dict[str, str], expected_keys: list) -> None:
        """Assert that config contains expected keys."""
        for key in expected_keys:
            assert key in config, f"Expected key '{key}' not found in config"
    
    def assert_airflow_config_format(self, config: Dict[str, str]) -> None:
        """Assert that config keys follow Airflow format."""
        for key in config.keys():
            assert key.startswith("AIRFLOW__"), f"Key '{key}' doesn't follow Airflow format"