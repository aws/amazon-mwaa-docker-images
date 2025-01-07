#!/bin/bash
set -e

dnf update -y

# shellcheck source=images/airflow/2.10.3/bootstrap/common.sh
source /bootstrap/common.sh

verify_env_vars_exist \
    BUILDARCH \
    TINI_DOWNLOAD_BASE_URL \
    TINI_ARM_SUFFIX \
    TINI_X86_SUFFIX \
    TINI_ARM_SHA_CHECKSUM \
    TINI_X86_SHA_CHECKSUM

dnf install -y wget

# Create directory for tini
mkdir -p /usr/local/bin

if [ "${BUILDARCH}" == "amd64" ]; then
    TINI_BINARY="${TINI_X86_SUFFIX}"
    TINI_SHA_CHECKSUM="${TINI_X86_SHA_CHECKSUM}"
elif [ "${BUILDARCH}" == "arm64" ]; then
    TINI_BINARY="${TINI_ARM_SUFFIX}"
    TINI_SHA_CHECKSUM="${TINI_ARM_SHA_CHECKSUM}"
else
    echo "Unsupported architecture: ${ARCH}"
    exit 1
fi

# Download tini binary
mkdir tini_source
wget "${TINI_DOWNLOAD_BASE_URL}${TINI_BINARY}" -P /tini_source

# Verify checksum
echo "${TINI_SHA_CHECKSUM} /tini_source/${TINI_BINARY}" | sha256sum --check - | grep --basic-regex "^/tini_source/${TINI_BINARY}: OK$"
cp "/tini_source/${TINI_BINARY}" "/usr/local/bin/tini"

# Make tini executable
chmod +x /usr/local/bin/tini

# Clean up
rm -rf /tini_source

dnf remove -y wget
