#Requires -Version 5.1
<#
.SYNOPSIS
    Builds and runs the Amazon MWAA local Docker environment on Windows.

.DESCRIPTION
    PowerShell equivalent of run.sh for Windows (PowerShell 5.1+).
    Detects the available container runtime (finch, podman, or docker),
    generates and caches a Fernet encryption key, builds the Docker image,
    and starts the full Airflow stack via Docker Compose.

    PREREQUISITES
    -------------
    - Docker Desktop for Windows with Linux containers mode enabled
    - Python 3.11+ in PATH
    - AWS CLI in PATH (optional — required only for CloudWatch log groups)
    - PowerShell execution policy allowing local scripts:
        Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

.PARAMETER Command
    Optional subcommand to run instead of the default stack:
      test-requirements   — validates requirements.txt installation
      test-startup-script — validates the user startup script

.EXAMPLE
    .\run.ps1
    .\run.ps1 -Command test-requirements
    .\run.ps1 -Command test-startup-script
#>

param(
    [string]$Command = ""
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

# ---------------------------------------------------------------------------
# Prerequisite validation
# ---------------------------------------------------------------------------
function Test-Prerequisites {
    # Docker installed
    if (-not (Get-Command -Name docker -ErrorAction SilentlyContinue)) {
        throw "Docker is not installed or not in PATH. Please install Docker Desktop for Windows."
    }

    # Docker daemon running
    docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker is not running. Please start Docker Desktop and try again."
    }

    # Linux containers mode
    $osType = docker info --format '{{.OSType}}' 2>&1
    if ($LASTEXITCODE -ne 0 -or $osType -ne 'linux') {
        throw "Docker is not in Linux containers mode. Right-click the Docker Desktop tray icon and choose 'Switch to Linux containers'."
    }

    # Python 3
    $script:PythonCmd = $null
    foreach ($candidate in @('python3', 'python')) {
        $cmd = Get-Command -Name $candidate -ErrorAction SilentlyContinue
        if ($cmd) {
            # Skip the Windows Store stub (zero-byte redirect)
            if ($cmd.Source -notmatch 'WindowsApps') {
                $script:PythonCmd = $candidate
                break
            }
        }
    }
    if (-not $script:PythonCmd) {
        throw "Python 3 is not installed or not in PATH. Please install Python 3.11+ from https://www.python.org/downloads/."
    }

    # pip
    if (-not (Get-Command -Name pip -ErrorAction SilentlyContinue)) {
        throw "pip is not installed or not in PATH. Ensure Python was installed with pip."
    }

    # AWS CLI (optional — only needed for CloudWatch log groups)
    if (-not (Get-Command -Name aws -ErrorAction SilentlyContinue)) {
        Write-Warning "AWS CLI not found. CloudWatch log group creation will be skipped."
    }
}

Test-Prerequisites

# ---------------------------------------------------------------------------
# Container runtime detection
# ---------------------------------------------------------------------------
if (Get-Command -Name finch -ErrorAction SilentlyContinue) {
    $ContainerRuntime = "finch"
} elseif (Get-Command -Name podman -ErrorAction SilentlyContinue) {
    $ContainerRuntime = "podman"
} else {
    $ContainerRuntime = "docker"
}

# ---------------------------------------------------------------------------
# Fernet key — generate once, cache for reuse
# ---------------------------------------------------------------------------
function Invoke-FernetKeyGeneration {
    # Install cryptography temporarily (not a production dependency)
    pip install cryptography 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Failed to install the cryptography package." }

    $key = & $script:PythonCmd generate_fernet_key.py
    if ($LASTEXITCODE -ne 0) { throw "Failed to generate Fernet key." }

    # Remove the temporary package
    & $script:PythonCmd -m pip uninstall -y cryptography cryptography-vectors 2>&1 | Out-Null

    return $key
}

$CacheDir      = "$env:USERPROFILE\.cache\mwaa-local"
$FernetKeyFile = "$CacheDir\fernet.key"

if (-not (Test-Path -Path $CacheDir)) {
    New-Item -ItemType Directory -Path $CacheDir -Force | Out-Null
}

if (-not (Test-Path -Path $FernetKeyFile)) {
    $fernetKey = Invoke-FernetKeyGeneration
    # Use WriteAllText to avoid BOM and trailing newline that would corrupt the key
    [System.IO.File]::WriteAllText($FernetKeyFile, $fernetKey)
}

$env:FERNET_KEY = [System.IO.File]::ReadAllText($FernetKeyFile).Trim()

# ---------------------------------------------------------------------------
# Configuration — fill in your values below
# ---------------------------------------------------------------------------
$AccountId = "" # Put your account ID here.
$EnvName   = "" # Choose an environment name here.
$Region    = "us-west-2" # Keeping the region us-west-2 as default.

# Write .env before the build so Docker Compose always picks up REGION on Windows.
# PowerShell $env: variables are not reliably inherited by Docker Compose on Windows.
[System.IO.File]::WriteAllText("$PSScriptRoot\.env", "REGION=$Region`n")

# ---------------------------------------------------------------------------
# Build the Docker image
# ---------------------------------------------------------------------------
& "$PSScriptRoot\build.ps1" -ContainerRuntime $ContainerRuntime

# AWS Credentials
# For local running without a real AWS account, dummy values are sufficient —
# ElasticMQ (local SQS) does not validate credentials, but boto3 requires
# non-empty values to sign requests. Replace with real credentials if you
# want CloudWatch logging or other real AWS service access.
$env:AWS_ACCESS_KEY_ID     = "local" # Put your credentials here, or leave as "local" for offline use.
$env:AWS_SECRET_ACCESS_KEY = "local" # Put your credentials here, or leave as "local" for offline use.
$env:AWS_SESSION_TOKEN     = ""      # Put your credentials here (leave empty for offline use).

# Set to http://host_name:8080
$env:MWAA__CORE__API_SERVER_URL = "http://mwaa-320-webserver:8080"

# BOM Generation
$env:GENERATE_BILL_OF_MATERIALS = "False"

# MWAA Configuration
$env:MWAA__CORE__REQUIREMENTS_PATH                     = "/usr/local/airflow/requirements/requirements.txt"
$env:MWAA__CORE__STARTUP_SCRIPT_PATH                   = "/usr/local/airflow/startup/startup.sh"
$env:MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED  = "true"
$env:MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_GROUP_ARN = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-DAGProcessing"
$env:MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOG_LEVEL     = "INFO"
$env:MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED     = "true"
$env:MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_GROUP_ARN    = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-Scheduler"
$env:MWAA__LOGGING__AIRFLOW_SCHEDULER_LOG_LEVEL        = "INFO"
$env:MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED          = "true"
$env:MWAA__LOGGING__AIRFLOW_TASK_LOG_GROUP_ARN         = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-Task"
$env:MWAA__LOGGING__AIRFLOW_TASK_LOG_LEVEL             = "INFO"
$env:MWAA__LOGGING__AIRFLOW_TRIGGERER_LOGS_ENABLED     = "true"
$env:MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_GROUP_ARN    = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-Scheduler"
$env:MWAA__LOGGING__AIRFLOW_TRIGGERER_LOG_LEVEL        = "INFO"
$env:MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED     = "true"
$env:MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_GROUP_ARN    = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-WebServer"
$env:MWAA__LOGGING__AIRFLOW_WEBSERVER_LOG_LEVEL        = "INFO"
$env:MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED        = "true"
$env:MWAA__LOGGING__AIRFLOW_WORKER_LOG_GROUP_ARN       = "arn:aws:logs:$($Region):$($AccountId):log-group:$($EnvName)-Worker"
$env:MWAA__LOGGING__AIRFLOW_WORKER_LOG_LEVEL           = "INFO"
$env:MWAA__CORE__TASK_MONITORING_ENABLED               = "false"
$env:MWAA__CORE__TERMINATE_IF_IDLE                     = "false"
$env:MWAA__CORE__MWAA_SIGNAL_HANDLING_ENABLED          = "false"

# ---------------------------------------------------------------------------
# CloudWatch log group creation
# ---------------------------------------------------------------------------
function New-LogGroupIfNotExists {
    param(
        [string]$Component,
        [string]$LogEnabled
    )

    if (-not $EnvName) {
        Write-Host "Not creating log group for $Component as EnvName is not set."
        return
    }

    if ($LogEnabled -ne "true") {
        Write-Host "Skipping log group creation as logging is not enabled for $Component."
        return
    }

    if (-not (Get-Command -Name aws -ErrorAction SilentlyContinue)) {
        Write-Host "Skipping log group creation for $Component — AWS CLI not available."
        return
    }

    $logGroupName = "$EnvName-$Component"
    Write-Host "Verifying existence of log group: '$logGroupName' in region '$Region'..."

    $output = (aws logs describe-log-groups --log-group-name-prefix $logGroupName --region $Region 2>&1) | Out-String
    if ($output.Contains($logGroupName)) {
        Write-Host "Log group '$logGroupName' already exists in region '$Region'."
    } else {
        Write-Host "Creating log group '$logGroupName' in region '$Region'..."
        aws logs create-log-group --log-group-name $logGroupName --region $Region 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Log group '$logGroupName' created successfully in region '$Region'."
        } else {
            Write-Host "Error creating log group '$logGroupName' in region '$Region'."
        }
    }
}

New-LogGroupIfNotExists -Component "DAGProcessing" -LogEnabled $env:MWAA__LOGGING__AIRFLOW_DAGPROCESSOR_LOGS_ENABLED
New-LogGroupIfNotExists -Component "Scheduler"     -LogEnabled $env:MWAA__LOGGING__AIRFLOW_SCHEDULER_LOGS_ENABLED
New-LogGroupIfNotExists -Component "Task"          -LogEnabled $env:MWAA__LOGGING__AIRFLOW_TASK_LOGS_ENABLED
New-LogGroupIfNotExists -Component "WebServer"     -LogEnabled $env:MWAA__LOGGING__AIRFLOW_WEBSERVER_LOGS_ENABLED
New-LogGroupIfNotExists -Component "Worker"        -LogEnabled $env:MWAA__LOGGING__AIRFLOW_WORKER_LOGS_ENABLED

# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------
if ($Command -eq "test-requirements" -or $Command -eq "test-startup-script") {
    & $ContainerRuntime compose -f docker-compose-test-commands.yaml up $Command --abort-on-container-exit
} else {
    & $ContainerRuntime compose up
}
if ($LASTEXITCODE -ne 0) { throw "Docker Compose exited with code $LASTEXITCODE." }
