"""
This is the entrypoint of the Docker image when running Airflow components.

The script gets called with the Airflow component name, e.g. scheduler, as the
first and only argument. It accordingly runs the requested Airlfow component
after setting up the necessary configurations.
"""

import sys


def main() -> None:
    """Entrypoint of the script."""
    print("Warming the Docker container.")
    print(sys.argv)
    # TODO Not yet implemented


if __name__ == '__main__':
    main()
else:
    print('This module cannot be imported.')
    sys.exit(1)
