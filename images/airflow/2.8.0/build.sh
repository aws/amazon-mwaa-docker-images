#!/bin/bash
set -e

python3 generate-dockerfile.py

docker build ./