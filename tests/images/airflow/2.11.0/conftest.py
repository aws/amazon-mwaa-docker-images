import os
import subprocess
import sys


def pytest_configure(config):
    airflow_version = "2.11.0"
    requirements_path = os.path.join(
        os.path.dirname(__file__),
        "requirements.txt"
    )
    airflow_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "..",
            "..",
            "images",
            "airflow",
            airflow_version,
            "python"
        )
    )
    # Add to Python path
    sys.path.insert(0, airflow_path)

    os.environ["MWAA__CORE__TESTING_MODE"] = "true"
    os.environ["MWAA__CORE__STARTUP_SCRIPT_PATH"] = "../../startup/startup.sh"

    # Set required database environment variables for module imports
    os.environ["MWAA__DB__POSTGRES_HOST"] = "localhost"
    os.environ["MWAA__DB__POSTGRES_PORT"] = "5432"
    os.environ["MWAA__DB__POSTGRES_DB"] = "airflow"
    os.environ["MWAA__DB__POSTGRES_USER"] = "airflow"
    os.environ["MWAA__DB__POSTGRES_PASSWORD"] = "airflow"
    os.environ["MWAA__DB__POSTGRES_SSLMODE"] = "disable"

    if os.path.exists(requirements_path):
        try:
            print(f"Installing requirements from: {requirements_path}")
            subprocess.check_call([
                "pip",
                "install",
                "--no-cache-dir",
                "-r",
                requirements_path
            ])
        except subprocess.CalledProcessError as e:
            print(f"Error installing requirements: {e}")
            raise
    else:
        print(f"Requirements file not found at: {requirements_path}")
