"""
Create the virtual environments required to develop with this package.

This module should be executed after cloning the repository to create the following
virtual environments:

- One virtual environment at the root package.
- One per each Docker image

Those environments are used for many tasks, most importantly allow the IDE to use the
right Python environment for the different folders in this repository. This is necessary
since the Python packages required to develop the different Airflow versions are
different from the packages that we need for the various scripts in this repository.
"""

import argparse
import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path


def verify_python_version():
    """Check if the current Python version is at least 3.9."""
    major, minor, *_ = sys.version_info

    if major != 3 or minor < 11:
        print("Python 3.11 or higher is required.")
        sys.exit(1)


def create_venv(path: Path, recreate: bool = False):
    """
    Create a venv in the given directory and optionally recreate it if it already exists.

    :param path: The path to create the venv in.
    :param recreate: Whether to recreate the venv if it already exists.
    """
    venv_path = path / ".venv"
    print(f">>> Creating a virtual environment under the path {venv_path}...")

    if recreate and venv_path.exists():
        print(f"> Deleting existing virtualenv in {venv_path}")
        shutil.rmtree(venv_path)  # Delete the existing environment

    if not venv_path.exists():
        print(f"> Creating virtualenv in directory: {venv_path}")
        venv.create(venv_path, with_pip=True)
    else:
        print(f"> Virtualenv already exists in {venv_path}")

    print("> Upgrade pip...")
    pip_install(venv_path, "-U", "pip")
    print("")

    requirements_path = str(path / "requirements.txt")
    print(f"> Install dependencies from {requirements_path}...")
    pip_install(venv_path, "-r", requirements_path)
    print("")

    print("> Install/Upgrade development tools: pydocstyle, pyright, ruff...")
    pip_install(venv_path, "-U", "pydocstyle", "pyright", "ruff")
    print("")

    print(f">>> Finished creating a virtual environment under the path {venv_path}.")
    print("")
    print("")


def pip_install(venv_dir: Path, *args: str):
    """
    Install dependencies from requirements.txt if it exists.

    :param venv_dir: The path to the venv directory.
    :param venv_dir: The path to the requirements.txt file.
    """
    subprocess.run(
        [os.path.join(venv_dir, "bin", "python"), "-m", "pip", "install", *args],
        check=True,
    )


def main():
    """Start execution of the script."""
    # Create the parser
    parser = argparse.ArgumentParser(description="Create virtual environments.")
    # Add the 'recreate' optional argument
    parser.add_argument(
        "--recreate", action="store_true", help="Recreate the venv if it exists"
    )

    # Parse the arguments
    args = parser.parse_args()

    verify_python_version()
    project_dirs = [
        Path("."),
        *Path("./images").glob("airflow/*"),
    ]  # Include main project dir and each image dir
    for dir_path in project_dirs:
        if dir_path.is_dir() and (dir_path / "requirements.txt").exists():
            create_venv(dir_path, recreate=args.recreate)


if __name__ == "__main__":
    main()
