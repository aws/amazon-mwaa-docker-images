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
import os
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


def create_venv(path: Path):
    """
    Create a venv in the given directory and install requirements if present.

    :param dir_path: The path to create the venv in.
    """
    venv_path = path / ".venv"

    if not venv_path.exists():
        print(f"Creating virtualenv in directory: {path}")
        venv.create(venv_path, with_pip=True)
    else:
        print(f"Virtualenv already exists in {venv_path}")

    requirements_path = path / "requirements.txt"
    pip_install(venv_path, requirements_path)


def pip_install(venv_dir: Path, requirements_file: Path):
    """
    Install dependencies from requirements.txt if it exists.

    :param venv_dir: The path to the venv directory.
    :param venv_dir: The path to the requirements.txt file.
    """
    if os.path.exists(requirements_file):
        print(f"Installing dependencies from {requirements_file}...")
        subprocess.run(
            [
                os.path.join(venv_dir, "bin", "python"),
                "-m",
                "pip",
                "install",
                "-U",
                "-r",
                str(requirements_file),
                "pip",  # Upgrade pip as well.
            ],
            check=True,
        )


def main():
    """Start execution of the script."""
    verify_python_version()
    project_dirs = [
        Path("."),
        Path("./images/mockwatch-logs"),
        *Path("./images").glob("airflow/*"),
    ]  # Include main project dir and each image dir
    for dir_path in project_dirs:
        if dir_path.is_dir() and (dir_path / "requirements.txt").exists():
            create_venv(dir_path)


if __name__ == "__main__":
    main()
