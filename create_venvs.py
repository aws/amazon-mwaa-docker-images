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
import re
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


def create_venv(path: Path, development_build: bool, recreate: bool = False):
    """
    Create a venv in the given directory and optionally recreate it if it already exists.

    :param path: The path to create the venv in.
    :param development_build: Is this a development build.
    :param recreate: Whether to recreate the venv if it already exists.
    """
    venv_path = path / ".venv"
    print(f">>> Creating a virtual environment under the path {venv_path}...")

    if recreate and venv_path.exists():
        print(f"> Deleting existing virtualenv in {venv_path}")
        shutil.rmtree(venv_path)  # Delete the existing environment

    if not venv_path.exists():
        print(f"> Creating virtualenv in directory: {venv_path}")
        venv.create(venv_path, with_pip=True, symlinks=True)
    else:
        print(f"> Virtualenv already exists in {venv_path}")

    print("> Upgrade pip...")
    pip_install(venv_path, "-U", "pip")
    print("")

    requirements_path = generate_requirements(path, development_build)
    print(f"> Install dependencies from {requirements_path}...")
    pip_install(venv_path, "-r", str(requirements_path))
    print("")

    dev_tools = ["pydocstyle", "pyright", "ruff"]
    print(f"> Install/Upgrade development tools: {dev_tools}...")
    pip_install(venv_path, "-U", *dev_tools)
    print("")

    print(f">>> Finished creating a virtual environment under the path {venv_path}.")
    print("")
    print("")

def generate_requirements(path: Path, development_build: bool) -> Path:
    """
    If the requirements.txt file at the path needs to be updated for local development, generate
    a new requirements file.

    Return the path to the requirements file to be used.

    :param path: The path to the directory containing the requirements.txt file.
    :param development_build: Is this a development build.
    """
    requirements_path = path.joinpath("requirements.txt")

    if not development_build:
        print("> Production build selected. Using default requirements.")
        return requirements_path

    if not re.search(r"images\/airflow\/[2-3]\.[0-9]+\.[0-9]+$", str(path.resolve())):
        print(f"> No need to create dev requirements for {path.resolve()}.  Using default.")
        return requirements_path

    with open(requirements_path.resolve(), 'r') as file:
        # psycopg2-binary is meant for development and removes the requirement to install pg_config
        filedata = re.sub(r"\bpsycopg2\b", "psycopg2-binary", file.read())

    dev_requirements_path = path.joinpath('requirements-dev.txt')
    print(f"> Creating {dev_requirements_path} from {requirements_path}")
    with open(dev_requirements_path.resolve(), 'w') as file:
        file.write(filedata)

    return dev_requirements_path

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

    development_target_choice = "development"
    build_targets = [development_target_choice, "production"]
    parser.add_argument(
        "--target", choices=build_targets, required=True, help="Sets the build target"
    )
    
    # Add version filter argument
    parser.add_argument(
        "--version", type=str, help="Only create venv for specific Airflow version (e.g., 3.0.6)"
    )

    # Parse the arguments
    args = parser.parse_args()

    verify_python_version()
    
    # Filter directories based on version argument
    if args.version:
        # Validate that the version exists
        version_path = Path(f"./images/airflow/{args.version}")
        if not version_path.exists() or not version_path.is_dir():
            # Get available versions
            available_versions = sorted([
                d.name for d in Path("./images/airflow").iterdir() 
                if d.is_dir() and not d.name.startswith('.')
            ])
            print(f"ERROR: Version '{args.version}' not found in images/airflow/")
            print("\nAvailable versions:")
            for v in available_versions:
                print(f"  - {v}")
            sys.exit(1)
        
        project_dirs = [
            Path("."),
            version_path,
        ]
    else:
        project_dirs = [
            Path("."),
            *Path("./images").glob("airflow/*"),
        ]  # Include main project dir and each image dir
    
    for dir_path in project_dirs:
        if dir_path.is_dir() and (dir_path / "requirements.txt").exists():
            create_venv(
                dir_path,
                development_build=args.target == development_target_choice,
                recreate=args.recreate
            )


if __name__ == "__main__":
    main()
