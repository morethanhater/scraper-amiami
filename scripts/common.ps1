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

function Add-UvLocationToPath {
    param(
        [string]$UvExePath
    )

    $uvDirectory = Split-Path -Parent $UvExePath
    if (-not $uvDirectory) {
        return
    }

    $pathEntries = $env:PATH -split ';' | Where-Object { $_ -ne "" }
    if ($pathEntries -notcontains $uvDirectory) {
        $env:PATH = "$uvDirectory;$env:PATH"
    }
}

function Install-UvIfMissing {
    try {
        $uvExe = Get-UvExe
        Add-UvLocationToPath -UvExePath $uvExe
        return $uvExe
    }
    catch {
    }

    Write-Host "uv.exe was not found. Installing uv..."
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression

    $uvExe = Get-UvExe
    Add-UvLocationToPath -UvExePath $uvExe
    return $uvExe
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

function Get-EnvValue {
    param(
        [string]$EnvPath,
        [string]$Key,
        [string]$DefaultValue = ""
    )

    if (-not (Test-Path $EnvPath)) {
        return $DefaultValue
    }

    foreach ($line in Get-Content $EnvPath) {
        if ($line -match "^\s*$([regex]::Escape($Key))\s*=\s*""(.*)""\s*$") {
            return $matches[1]
        }
    }

    return $DefaultValue
}

function Set-EnvValue {
    param(
        [string]$EnvPath,
        [string]$Key,
        [string]$Value
    )

    $lines = @()
    if (Test-Path $EnvPath) {
        $lines = Get-Content $EnvPath
    }

    $escapedKey = [regex]::Escape($Key)
    $updated = $false
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match "^\s*$escapedKey\s*=") {
            $lines[$index] = "$Key = ""$Value"""
            $updated = $true
            break
        }
    }

    if (-not $updated) {
        $lines += "$Key = ""$Value"""
    }

    Set-Content -Path $EnvPath -Value $lines
}
