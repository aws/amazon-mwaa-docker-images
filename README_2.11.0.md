Part 1: The "Golden" Requirements
This requirements.txt is the result of resolving conflicts between Airflow 2.11’s strict core (Protobuf 4.x/Isodate 0.7.x) and modern dbt/Snowflake libraries.

File Location: requirements/requirements.txt

Plaintext
# 1. Force the official MWAA 2.11 / Python 3.11 Constraints
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.0/constraints-3.11.txt"

# 2. Modern dbt Stack (The only combination that satisfies Isodate/Protobuf for 2.11)
dbt-core==1.9.0
dbt-snowflake==1.9.0
dbt-adapters>=1.16.3
dbt-common>=1.25.0

# 3. Base Providers (Let constraints pick the exact 2.11-compatible version)
apache-airflow-providers-snowflake
apache-airflow-providers-cncf-kubernetes
snowflake-connector-python
pandas
numpy
Part 2: The Simulation Setup (PowerShell)
The official run.ps1 in the repo often fails due to syntax errors or environment mismatch. Use this "clean-slate" script to build and launch local-runner-1.

PowerShell
# --- CONFIGURATION ---
$Version = "2.11.0"
$ImageName = "amazon-mwaa-docker-images/airflow"

Write-Host "🚀 Starting MWAA $Version Simulation..." -ForegroundColor Cyan

# 1. Allow script execution
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 2. Create the Virtual Environment (Mandatory for repo internal checks)
python create_venvs.py --target development --version $Version

# 3. Navigate to version folder
cd "images\airflow\$Version"

# 4. Nuclear Rebuild (Force Python 3.11)
Write-Host "🛠️ Building Base Image (Python 3.11)..." -ForegroundColor Yellow
docker build --no-cache --build-arg PYTHON_VERSION=3.11 -t "$($ImageName):$($Version)-base" -f Dockerfiles/Dockerfile.base .

Write-Host "🛠️ Building Local Image..." -ForegroundColor Yellow
docker build --no-cache -t "$($ImageName):$($Version)-local" -f Dockerfiles/Dockerfile .

# 5. Launch with Manual Compose (Fixes entrypoint & DB errors)
Write-Host "🚀 Launching local-runner-1..." -ForegroundColor Green
docker-compose -f docker-compose-manual.yaml up -d --force-recreate
Part 3: The Manual Orchestration File
This file bypasses the buggy entrypoint.py which often fails with "unpacking" errors in PowerShell.

File Location: images/airflow/2.11.0/docker-compose-manual.yaml

YAML
version: '3.8'
services:
  postgres:
    image: postgres:15-alpine
    container_name: postgres
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      retries: 5

  local-runner:
    image: amazon-mwaa-docker-images/airflow:2.11.0-local
    container_name: local-runner-1
    # Direct execution bypasses buggy entrypoint parsing
    entrypoint: "/bin/bash -c 'airflow db migrate && airflow standalone'"
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/usr/local/airflow/dags
      - ./requirements:/usr/local/airflow/requirements
    environment:
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      - MWAA__DB__POSTGRES_HOST=postgres
      - MWAA__DB__POSTGRES_PORT=5432
      - MWAA__DB__POSTGRES_DB=airflow
      - MWAA__DB__POSTGRES_USER=airflow
      - MWAA__DB__POSTGRES_PASSWORD=airflow
      - MWAA__DB__POSTGRES_SSLMODE=disable
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    depends_on:
      postgres:
        condition: service_healthy
Part 4: The Verification Scripts
Once the container is up, use these scripts to verify the environment is ready for production.

PowerShell
Write-Host "`n--- FINAL VERIFICATION ---" -ForegroundColor Cyan

# A. Test Python and Airflow Versions
Write-Host "🐍 Python: " -NoNewline; docker exec local-runner-1 python --version
Write-Host "💨 Airflow: " -NoNewline; docker exec local-runner-1 airflow version

# B. Test Migration Requirements (The Conflict Test)
Write-Host "🧪 Running Conflict Check..." -ForegroundColor Yellow
docker exec -it local-runner-1 pip install -r /usr/local/airflow/requirements/requirements.txt --dry-run

# C. Get Login Password
Write-Host "🔑 Admin Password: " -NoNewline
docker exec local-runner-1 cat /usr/local/airflow/standalone_admin_password.tx


# Manual Execution to force the environment to use python 3.11
# 1. Rebuild the base specifically for 3.11

``` bash
docker build --no-cache --build-arg PYTHON_VERSION=3.11 -t amazon-mwaa-docker-images/airflow:2.11.0-base -f Dockerfiles/Dockerfile.base .

```
# 2. Rebuild your local image

``` bash
docker build --no-cache -t amazon-mwaa-docker-images/airflow:2.11.0-local -f Dockerfiles/Dockerfile .
```

# 3. Restart the container

```bash
docker-compose -f docker-compose-manual.yaml up -d --force-recreate
```

# 4. Local requirement test
```bash
docker exec -it local-runner-1 pip install -r /usr/local/airflow/requirements/requirements.txt --dry-run               
```

# 5. Check the airflow pw
```bash
ocker exec local-runner-1 cat /usr/local/airflow/standalone_admin_password.txt
```
