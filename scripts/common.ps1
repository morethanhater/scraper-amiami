Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Set-LocalUvCache {
    param(
        [string]$RepoRoot
    )

    $env:UV_CACHE_DIR = Join-Path $RepoRoot ".uv-cache"
    $env:UV_PYTHON_INSTALL_DIR = Join-Path $RepoRoot ".uv-python"
}

function Get-UvExe {
    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $candidates = @(
        (Join-Path $env:USERPROFILE ".local\bin\uv.exe"),
        (Join-Path $env:APPDATA "Python\Python314\Scripts\uv.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python314\Scripts\uv.exe")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    throw "uv.exe was not found. Install uv first, then rerun this script."
}

function Install-UvIfMissing {
    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    Write-Host "uv.exe was not found. Installing uv..."
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression

    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    return Get-UvExe
}

function Assert-EnvFile {
    param(
        [string]$RepoRoot
    )

    $envPath = Join-Path $RepoRoot ".env"
    if (-not (Test-Path $envPath)) {
        Copy-Item (Join-Path $RepoRoot ".env.default") $envPath
    }
}
