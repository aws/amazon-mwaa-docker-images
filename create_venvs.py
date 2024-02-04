import os
import subprocess
import sys
import venv
from pathlib import Path


def verify_python_version():
    """Check if the current Python version is at least 3.9."""
    _major, minor, *_ = sys.version_info
    if minor < 9:
        print("Python 3.9 or higher is required.")
        sys.exit(1)


def create_venv(path: Path):
    """Create a virtual environment in the given directory and install
    requirements if `requirements.txt` is present.

    :param dir_path: The path to create the venv in."""
    venv_path = path / ".venv"

    if not venv_path.exists():
        print(f"Creating virtualenv in directory: {path}")
        venv.create(venv_path, with_pip=True)
    else:
        print(f"Virtualenv already exists in {venv_path}")

    requirements_path = path / "requirements.txt"
    pip_install(venv_path, requirements_path)


def pip_install(venv_dir: Path, requirements_file: Path):
    """Install dependencies from requirements.txt if it exists.

    :param venv_dir: The path to the venv directory.
    :param venv_dir: The path to the requirements.txt file."""
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
    """Main entrypoint of the script."""
    verify_python_version()
    project_dirs = [
        Path("."),
        *Path("./images").glob("airflow/*"),
    ]  # Include main project dir and each image dir
    for dir_path in project_dirs:
        if dir_path.is_dir() and (dir_path / "requirements.txt").exists():
            create_venv(dir_path)


if __name__ == "__main__":
    main()
