#!/usr/bin/env python3
"""Run all quality check scripts under the quality-checks/ folder."""

import os
import subprocess
import sys
from typing import List


# NOTE Ideally, we should be specifying the typing annotation for 'process' to
# `subprocess.Popne[bytes]`. However, this requires Python 3.9+ and Amazon Linux 2
# is still on 3.7/3.8.
# TODO Remove support of Amazon Linux 2 from this package as soon as possible, as we
# shouldn't be relying on an EOLed Python version.
def prefix_output(file: str, process: subprocess.Popen) -> None:  # type: ignore
    """Prefix each line of output with the filename."""
    if not process.stdout:  # type: ignore
        raise RuntimeError("Process doesn't have an stdout stream.")
    for line in process.stdout:  # type: ignore
        print(f"[{file}] {line.decode().strip()}")  # type: ignore


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

    quality_checks_dir = "./quality-checks/"
    failed_scripts: List[str] = []

    # Iterate over every file in the quality-checks directory
    for file in os.listdir(quality_checks_dir):
        filepath = os.path.join(quality_checks_dir, file)

        # Exclude README.md and run_all.sh
        if file in ["README.md", "run_all.py"]:
            continue

        # Check if the file is executable
        if os.access(filepath, os.X_OK):
            print(f"Executing: {file}")
            with subprocess.Popen(
                filepath, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            ) as process:
                prefix_output(file, process)
                process.wait()  # Wait for the process to complete

                # Check the exit status of the script
                if process.returncode != 0:
                    print(
                        f"Script {file} failed with exit status "
                        f"{process.returncode}."
                    )
                    failed_scripts.append(file)
            print()

    # Exit with a non-zero status if any script failed
    if failed_scripts:
        print("The following scripts failed:")
        for fs in failed_scripts:
            print(f"- {fs}")
        sys.exit(1)


if __name__ == "__main__":
    main()
