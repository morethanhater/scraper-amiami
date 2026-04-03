Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$repoRoot = Get-RepoRoot
$uvExe = Get-UvExe

Assert-EnvFile -RepoRoot $repoRoot
Set-LocalUvCache -RepoRoot $repoRoot

Push-Location $repoRoot
try {
    $venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"

    if (-not (Test-Path $venvPython)) {
        & $uvExe python install 3.10
        & $uvExe venv --python 3.10 .venv
    }

    & $uvExe sync --python $venvPython
}
finally {
    Pop-Location
}

Write-Host "Local Python 3.10 environment is ready in .venv"
