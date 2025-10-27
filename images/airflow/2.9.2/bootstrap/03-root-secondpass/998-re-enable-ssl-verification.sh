#!/bin/bash
set -e

# Re-enable SSL verification now that all packages are installed
# and certificates are properly configured
sed -i '/sslverify=0/d' /etc/dnf/dnf.conf

echo "SSL verification re-enabled for dnf"

