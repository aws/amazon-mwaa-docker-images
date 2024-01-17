#!/bin/bash
set -e

dnf install -y wget xz tar

PYTHON_VERSION=3.11.7
PYTHON_MD5_CHECKSUM=d96c7e134c35a8c46236f8a0e566b69c

mkdir python_install
python_file=Python-$PYTHON_VERSION
python_tar=$python_file.tar
python_tar_xz=$python_tar.xz

# Download Python's source code archive.
mkdir python_source
wget "https://www.python.org/ftp/python/$PYTHON_VERSION/$python_tar_xz" -P /python_source

# Verify the checksum
echo "$PYTHON_MD5_CHECKSUM /python_source/$python_tar_xz" | md5sum --check - | grep --basic-regex "^/python_source/${python_tar_xz}: OK$"

cp /python_source/$python_tar_xz /python_install/$python_tar_xz
unxz ./python_install/$python_tar_xz
tar -xf ./python_install/$python_tar -C ./python_install

dnf install -y dnf-plugins-core
dnf builddep -y python3

pushd /python_install/$python_file 
./configure 
make install -s -j "$(nproc)" # use -j to set the cores for the build
popd

# Upgrade pip
pip3 install --upgrade pip

rm -rf /python_source /python_install

dnf remove -y wget xz tar