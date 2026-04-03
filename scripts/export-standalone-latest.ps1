Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$repoRoot = Get-RepoRoot
$uvExe = Get-UvExe
Set-LocalUvCache -RepoRoot $repoRoot

Push-Location $repoRoot
try {
    & (Join-Path $PSScriptRoot "setup.ps1")

    $latestMappedFile = Get-ChildItem -Path "web\data" -Filter "*-mapped_items.json" -File |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $latestMappedFile) {
        throw "No mapped JSON files were found in web\data."
    }

    $outputDir = Join-Path $repoRoot "exports"
    $outputHtml = Join-Path $outputDir ($latestMappedFile.BaseName + ".standalone.html")

    & $uvExe run --python 3.10 --env-file .env python `
        (Join-Path $PSScriptRoot "build-standalone-html.py") `
        $latestMappedFile.FullName `
        $outputHtml
}
finally {
    Pop-Location
}
