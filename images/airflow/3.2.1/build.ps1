#Requires -Version 5.1
<#
.SYNOPSIS
    Builds the Amazon MWAA Airflow 3.2.1 Docker images on Windows.

.DESCRIPTION
    PowerShell equivalent of build.sh for Windows (PowerShell 5.1+).
    Activates the repo virtual environment, generates Dockerfiles from
    Jinja2 templates, and builds the base image plus all derivative images.
    Optionally extracts the Bill of Materials from each built image.

    PREREQUISITES
    -------------
    - Docker Desktop for Windows with Linux containers mode enabled
    - Repo virtual environment created at <repo-root>/.venv:
        python create_venvs.py --target development
    - PowerShell execution policy allowing local scripts:
        Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

.PARAMETER ContainerRuntime
    Container runtime to use: docker (default), podman, or finch.

.EXAMPLE
    .\build.ps1
    .\build.ps1 -ContainerRuntime podman
#>

param(
    [string]$ContainerRuntime = ""
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

# ---------------------------------------------------------------------------
# Container runtime — detect if not provided
# ---------------------------------------------------------------------------
if (-not $ContainerRuntime) {
    if (Get-Command -Name finch -ErrorAction SilentlyContinue) {
        $ContainerRuntime = "finch"
    } elseif (Get-Command -Name podman -ErrorAction SilentlyContinue) {
        $ContainerRuntime = "podman"
    } else {
        $ContainerRuntime = "docker"
    }
}

Write-Host "Using $ContainerRuntime runtime in build.ps1"

# ---------------------------------------------------------------------------
# Prerequisite validation
# ---------------------------------------------------------------------------
if (-not (Get-Command -Name $ContainerRuntime -ErrorAction SilentlyContinue)) {
    throw "$ContainerRuntime is not installed or not in PATH."
}

& $ContainerRuntime info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "$ContainerRuntime is not running. Please start Docker Desktop and try again."
}

$osType = & $ContainerRuntime info --format '{{.OSType}}' 2>&1
if ($LASTEXITCODE -ne 0 -or $osType -ne 'linux') {
    throw "$ContainerRuntime is not in Linux containers mode. Right-click the Docker Desktop tray icon and choose 'Switch to Linux containers'."
}

# ---------------------------------------------------------------------------
# Virtual environment activation
# ---------------------------------------------------------------------------
$venvActivate = [System.IO.Path]::GetFullPath(
    [System.IO.Path]::Combine($PSScriptRoot, "..\..\..\.venv\Scripts\Activate.ps1")
)
if (-not (Test-Path -Path $venvActivate)) {
    throw "Virtual environment not found at '$venvActivate'. Run from the repo root: python create_venvs.py --target development"
}
. $venvActivate

# ---------------------------------------------------------------------------
# Generate Dockerfiles from Jinja2 templates
# ---------------------------------------------------------------------------
python ..\generate-dockerfiles.py
if ($LASTEXITCODE -ne 0) { throw "generate-dockerfiles.py failed." }

deactivate

# ---------------------------------------------------------------------------
# BOM configuration
# ---------------------------------------------------------------------------
$GenerateBom   = if ($env:GENERATE_BILL_OF_MATERIALS) { $env:GENERATE_BILL_OF_MATERIALS } else { "False" }
$BomDockerPath = "/BillOfMaterials"
$BomLocalPath  = ".\BillOfMaterials"

Write-Host "GENERATE_BILL_OF_MATERIALS is set to $GenerateBom"

if ($GenerateBom -eq "True") {
    if (Test-Path -Path $BomLocalPath) {
        Remove-Item -Path $BomLocalPath -Recurse -Force
    }
    New-Item -ItemType Directory -Path $BomLocalPath -Force | Out-Null
}

# ---------------------------------------------------------------------------
# Build base image
# ---------------------------------------------------------------------------
& $ContainerRuntime build -f .\Dockerfiles\Dockerfile.base -t amazon-mwaa-docker-images/airflow:3.2.1-base .\
if ($LASTEXITCODE -ne 0) { throw "Failed to build base image." }

# ---------------------------------------------------------------------------
# Build derivative images
# ---------------------------------------------------------------------------
foreach ($dev in @("True", "False")) {
    foreach ($buildType in @("standard", "explorer", "explorer-privileged")) {
        $dockerfileName = "Dockerfile"
        $tagName        = "3.2.1"

        if ($buildType -ne "standard") {
            $dockerfileName = "$dockerfileName-$buildType"
            $tagName        = "$tagName-$buildType"
        }

        if ($dev -eq "True") {
            $dockerfileName = "$dockerfileName-dev"
            $tagName        = "$tagName-dev"
        }

        $imageName = "amazon-mwaa-docker-images/airflow:$tagName"

        & $ContainerRuntime build -f ".\Dockerfiles\$dockerfileName" -t $imageName .\
        if ($LASTEXITCODE -ne 0) { throw "Failed to build image '$imageName'." }

        if ($GenerateBom -eq "True") {
            & $ContainerRuntime container rm bom_temp_container 2>&1 | Out-Null

            & $ContainerRuntime container create --name bom_temp_container $imageName
            if ($LASTEXITCODE -ne 0) { throw "Failed to create temp container for BOM extraction." }

            & $ContainerRuntime cp "bom_temp_container:$BomDockerPath" $BomLocalPath
            if ($LASTEXITCODE -ne 0) { throw "Failed to copy BOM from container." }

            Rename-Item -Path "$BomLocalPath\BillOfMaterials" -NewName $tagName

            & $ContainerRuntime rm bom_temp_container
            if ($LASTEXITCODE -ne 0) { throw "Failed to remove temp container." }
        }
    }
}
