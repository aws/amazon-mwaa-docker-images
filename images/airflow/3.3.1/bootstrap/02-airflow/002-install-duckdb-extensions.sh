#!/bin/bash
set -e

# shellcheck source=images/airflow/3.3.1/bootstrap/common.sh
source /bootstrap/common.sh

verify_env_vars_exist AIRFLOW_USER_HOME

# Pre-install the httpfs and aws extensions DuckDB needs for S3, so environments without internet
# access can use them. Extensions are built per DuckDB version, so these only serve the default
# duckdb. A user who installs another version gets its extensions downloaded on first use.
python3 - <<'PYTHON'
import duckdb

extensions = ("httpfs", "aws")

conn = duckdb.connect()
try:
    for extension in extensions:
        conn.execute(f"INSTALL {extension};")
finally:
    conn.close()

# Load with downloads disabled, so a bad install fails the build.
conn = duckdb.connect(config={"autoinstall_known_extensions": False})
try:
    for extension in extensions:
        conn.execute(f"LOAD {extension};")
    for name, path in conn.execute(
        "SELECT extension_name, install_path FROM duckdb_extensions() "
        "WHERE extension_name IN ('httpfs', 'aws') AND loaded"
    ).fetchall():
        print(f"DuckDB {duckdb.__version__} extension {name}: {path}")
finally:
    conn.close()
PYTHON
