#!/bin/bash

# This is a conditional bootstrapping step to install tools that help with
# debugging, but shouldn't be installed in production.

dnf install -y vim