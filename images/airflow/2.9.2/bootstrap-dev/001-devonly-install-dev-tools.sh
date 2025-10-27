#!/bin/bash
set -e

# This is a conditional bootstrapping step to install tools that help with
# debugging, but shouldn't be installed in production.

# Temporarily disable SSL verification for dnf to avoid certificate issues
echo "sslverify=0" >> /etc/dnf/dnf.conf

dnf install -y vim

# Re-enable SSL verification
sed -i '/sslverify=0/d' /etc/dnf/dnf.conf

echo "Dev tools installed successfully with SSL verification re-enabled"