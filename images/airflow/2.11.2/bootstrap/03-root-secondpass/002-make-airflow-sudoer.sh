#!/bin/bash
set -e

dnf install -y sudo
echo 'airflow ALL=(ALL)NOPASSWD:ALL' | sudo EDITOR='tee -a' visudo
