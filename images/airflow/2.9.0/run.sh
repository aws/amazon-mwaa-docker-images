#!/bin/bash
set -e

# Build the Docker image
./build.sh

docker compose up 
