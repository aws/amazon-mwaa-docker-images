#!/bin/bash
set -e

# Temporarily disable SSL verification for dnf to fix certificate issues
# This will remain disabled throughout the bootstrap process
echo "sslverify=0" >> /etc/dnf/dnf.conf

# Install/update ca-certificates to fix SSL certificate issues
dnf install -y ca-certificates

# Update the CA trust store
update-ca-trust

# Now proceed with normal system update
dnf update -y
