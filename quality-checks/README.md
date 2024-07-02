# Quality Checks

## Overview

This `quality-checks` folder contains a collection of scripts designed to ensure
the quality and integrity of the repository code. These scripts automate various
checks and audits, helping maintain high standards in code development and
repository maintenance.

## Contents

- `lint_bash.sh`: A script for linting bash scripts in the repository. It helps
  in identifying and fixing potential issues, ensuring that the bash scripts
  adhere to best coding practices.

- `lint_python.sh`: This script is used for linting Python code. It checks
  Python scripts for stylistic errors and coding standards, ensuring consistency
  and quality in the Python codebase.

- `pip_install_check.py`: This script searches through bash scripts in a
  specified directory to find instances of `pip install`. Its purpose is to
  enforce the use of a special command we provide called `safe-pip-install` for
  installing pip packages, which meets certain criteria for compatibility.

- `trufflehog_scan.sh`: This script scans the repository for sensitive 
  information or secrets using TruffleHog. It helps in identifying potential
  security vulnerabilities by detecting secrets, such as API keys, passwords,
  or other sensitive data, that may have been accidentally committed.

## Usage

The easiest way to run all checks is to run the `run_all.py` script. If you would
like to execute a specific check, you can manually execute the corresponding script,
e.g. `./lint_bash.sh`.
