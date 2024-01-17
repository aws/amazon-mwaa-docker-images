#!/bin/bash
set -e

dnf install -y wget

MARIADB_RPM_COMMON_CHECKSUM=e87371d558efa97724f3728fb214cf19
MARIADB_RPM_SHARED_CHECKSUM=ed82ad5bc5b35cb2719a9471a71c6cdb
MARIADB_RPM_DEVEL_CHECKSUM=cfce6e9b53f4e4fb1cb14f1ed720c92c

# Installing mariadb-devel dependency for apache-airflow-providers-mysql.
MARIADB_RPM_COMMON=MariaDB-common-11.1.2-1.fc38.x86_64.rpm
MARIADB_RPM_SHARED=MariaDB-shared-11.1.2-1.fc38.x86_64.rpm
MARIADB_RPM_DEVEL=MariaDB-devel-11.1.2-1.fc38.x86_64.rpm

# Download the necessary RPMs.
mkdir /mariadb_rpm
wget  https://mirror.mariadb.org/yum/11.1/fedora38-amd64/rpms/$MARIADB_RPM_COMMON -P /mariadb_rpm
wget  https://mirror.mariadb.org/yum/11.1/fedora38-amd64/rpms/$MARIADB_RPM_SHARED -P /mariadb_rpm
wget  https://mirror.mariadb.org/yum/11.1/fedora38-amd64/rpms/$MARIADB_RPM_DEVEL -P /mariadb_rpm

# Verify their checkums
echo "$MARIADB_RPM_COMMON_CHECKSUM /mariadb_rpm/$MARIADB_RPM_COMMON" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_COMMON: OK$"
echo "$MARIADB_RPM_SHARED_CHECKSUM /mariadb_rpm/$MARIADB_RPM_SHARED" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_SHARED: OK$"
echo "$MARIADB_RPM_DEVEL_CHECKSUM /mariadb_rpm/$MARIADB_RPM_DEVEL" | md5sum --check - | grep --basic-regex "^/mariadb_rpm/$MARIADB_RPM_DEVEL: OK$"

# Install the RPMs.
rpm -ivh /mariadb_rpm/*

rm -rf /mariadb_rpm

dnf remove -y wget
