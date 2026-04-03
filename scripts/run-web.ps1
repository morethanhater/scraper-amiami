param(
    [int]$Port = 8000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$repoRoot = Get-RepoRoot
$uvExe = Get-UvExe
Set-LocalUvCache -RepoRoot $repoRoot

Push-Location $repoRoot
try {
    & (Join-Path $PSScriptRoot "setup.ps1")
    & $uvExe run --python 3.10 python -m http.server $Port
}
finally {
    Pop-Location
}
