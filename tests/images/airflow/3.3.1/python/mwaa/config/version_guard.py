"""Guard against version-specific code being copied into another image version."""

import inspect
from pathlib import Path


def image_airflow_version(obj) -> str:
    """Return VERSION from the ``images/airflow/<VERSION>/...`` path ``obj`` was loaded from."""
    parts = Path(inspect.getsourcefile(obj)).resolve().parts
    for i in range(len(parts) - 2):
        if parts[i : i + 2] == ("images", "airflow"):
            return parts[i + 2]
    raise AssertionError(f"{obj.__qualname__} is not under images/airflow/<VERSION>/")


def assert_loaded_from_airflow_version(obj, expected_version: str) -> None:
    """
    Fail if ``obj`` is loaded from an image folder other than ``expected_version``.

    Use for code written for a specific Airflow version: if the code (and its tests)
    is copied into another image version, the test fails so the copy is reviewed
    rather than carried forward blindly. Once reviewed, update ``expected_version``.
    """
    version = image_airflow_version(obj)
    assert version == expected_version, (
        f"{obj.__module__}.{obj.__qualname__} is loaded from images/airflow/{version}/ "
        f"but was written for Airflow {expected_version}. Review whether it is still "
        "needed for this version, then update this test."
    )
