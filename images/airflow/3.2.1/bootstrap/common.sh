#!/bin/bash

# verify_env_vars_exist
#
# This function checks if the specified environment variables are set.
# It takes a list of environment variable names as arguments and verifies
# each one. If any of the variables is not set, the function prints an
# error message and exits with a non-zero status.
#
# Usage:
#   verify_env_vars_exist VAR1 VAR2 VAR3
#
# Arguments:
#   VAR1, VAR2, VAR3, ... : Names of the environment variables to check.
#
# Returns:
#   None if all variables are set.
#   Exits with status 1 and prints an error message if any variable is not set.
#
# Example:
#   verify_env_vars_exist DB_USER DB_PASS DB_HOST
#
verify_env_vars_exist() {
    for var in "$@"; do
        if [ -z "${!var}" ]; then
            echo "Error: Environment variable '$var' is not set."
            exit 1
        fi
    done
}
