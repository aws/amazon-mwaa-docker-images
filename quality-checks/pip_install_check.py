#!/usr/bin/env python3
"""
This module verifies there are no direct use of "pip install" in the code.

Direct use of "pip install" could easily result in broken Airflow dependencies. As such,
we always want to use a special script, safe-pip-install, which ensure Airflow and its
dependencies are protected.
"""

import os
import sys
from pathlib import Path


EMJOI_CHECK_MARK_BUTTON = "\u2705"
EMJOI_CROSS_MARK = "\u274c"


# List of files that are allowed to use `pip install` directly, instead of
# `safe-pip-install`.
PIP_INSTALL_ALLOWLIST = [
    # Bootstrap steps that install Python will usually include updating `pip`
    # itself so they need to make direct use of `pip`.
    "images/airflow/*/bootstrap/*/*-install-python.sh",
    "images/airflow/*/bootstrap/03-root-secondpass/999-generate-bill-of-materials.sh",
]


def check_file_for_pip_install(filepath: Path) -> bool:
    """
    Check if the file contains 'pip install'.

    :param filepath: The path of the file to check.

    :returns True if the check passes (no 'pip install' found), else False.
    """
    with open(filepath, "r") as file:
        for line in file:
            if "pip install" in line or "pip3 install" in line:
                return False
    return True


def verify_no_pip_install(directory: Path) -> bool:
    """
    Verify there is no direct use of `pip install` in the directory tree.

    :param directory: The directory to scan.

    :returns True if the verification succeeds, otherwise False.
    """
    # Check if the directory exists
    if not directory.is_dir():
        print(f"The directory {directory} does not exist.")
        return True

    # Walk through the shell scripts in the directory tree.
    ret_code = True
    for filepath in directory.glob("**/*.sh"):
        if any(filepath.match(p) for p in PIP_INSTALL_ALLOWLIST):
            print(f"Ignoring {filepath} since it is in the allowlist.")
            continue
        if check_file_for_pip_install(filepath):
            print(f"{EMJOI_CHECK_MARK_BUTTON} {filepath}")
        else:
            print(f"{EMJOI_CROSS_MARK} {filepath}.")
            ret_code = False

    return ret_code


def verify_in_repo_root() -> None:
    """Verify the script is executed from the repository root, or exit with non-zero."""
    # Determine the script's directory and the parent directory (which should
    # be <repo root>)
    script_dir = os.path.dirname(os.path.realpath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))

    # Check if the current working directory is the repo root
    if os.getcwd() != repo_root:
        print(
            "The script must be run from the repo root. Please cd into "
            "the repo root directory and then type: "
            f"./quality-checks/{os.path.basename(__file__)}."
        )
        sys.exit(1)


def main() -> None:
    """Start execution of the script."""
    verify_in_repo_root()

    if verify_no_pip_install(Path("./images/airflow")):
        sys.exit(0)
    else:
        print(
            "Some files failed the check. Please ensure you are using "
            "`safe-pip-install` in those files instead of directly "
            "calling `pip install`."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
