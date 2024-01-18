#!/bin/bash
set -e

# shellcheck source=images/airflow/2.8.0/bootstrap/common.sh
source /bootstrap/common.sh

verify_env_vars_exist \
    MARIADB_DOWNLOAD_BASE_URL \
    MARIADB_RPM_COMMON \
    MARIADB_RPM_COMMON_CHECKSUM \
    MARIADB_RPM_DEVEL \
    MARIADB_RPM_DEVEL_CHECKSUM \
    MARIADB_RPM_SHARED \
    MARIADB_RPM_SHARED_CHECKSUM

dnf install -y wget

# Download the necessary RPMs.
mkdir /mariadb_rpm
wget "${MARIADB_DOWNLOAD_BASE_URL}/${MARIADB_RPM_COMMON}" -P /mariadb_rpm
wget "${MARIADB_DOWNLOAD_BASE_URL}/${MARIADB_RPM_SHARED}" -P /mariadb_rpm
wget "${MARIADB_DOWNLOAD_BASE_URL}/${MARIADB_RPM_DEVEL}" -P /mariadb_rpm

# Verify their checkums
echo "$MARIADB_RPM_COMMON_CHECKSUM /mariadb_rpm/$MARIADB_RPM_COMMON" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_COMMON: OK$"
echo "$MARIADB_RPM_SHARED_CHECKSUM /mariadb_rpm/$MARIADB_RPM_SHARED" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_SHARED: OK$"
echo "$MARIADB_RPM_DEVEL_CHECKSUM /mariadb_rpm/$MARIADB_RPM_DEVEL" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_DEVEL: OK$"

# Install the RPMs.
rpm -ivh /mariadb_rpm/*

rm -rf /mariadb_rpm

dnf remove -y wget
