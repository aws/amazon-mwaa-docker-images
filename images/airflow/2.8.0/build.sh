#!/bin/bash
set -e

python3 generate-dockerfile.py

docker build -t "amazon-mwaa/airflow:2.8.0" ./
