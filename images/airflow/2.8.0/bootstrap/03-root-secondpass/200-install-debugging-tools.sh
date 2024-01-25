#!/bin/bash

# This is a conditional bootstrapping step to install tools that help with
# debugging, but shouldn't be installed in production.
# TODO Currently, we are always executing this step. In the near future, we
# will update the build process to make this step conditional on a user flag.

dnf install -y vim