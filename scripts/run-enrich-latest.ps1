Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$repoRoot = Get-RepoRoot
$uvExe = Get-UvExe
Set-LocalUvCache -RepoRoot $repoRoot

Push-Location $repoRoot
try {
    & (Join-Path $PSScriptRoot "setup.ps1")

    $latestRawFile = Get-ChildItem -Path "output" -Filter "*.json" -File |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if (-not $latestRawFile) {
        throw "No raw JSON files were found in output."
    }

    $timestamp = $latestRawFile.BaseName.Split('-')[0]
    $pythonScriptPath = Join-Path $repoRoot ".codex-run-enrich-latest.py"
    $pythonScript = @"
from scrapers.amiami import AmiAmiScraper

timestamp = $(ConvertTo-Json $timestamp -Compress)
filename = $(ConvertTo-Json $latestRawFile.Name -Compress)

with AmiAmiScraper(always_scrap_details=False) as amiami:
    amiami.run_enrich(timestamp, filename)

print(f"Enriched: {filename}")
"@

    Set-Content -Path $pythonScriptPath -Value $pythonScript -Encoding UTF8
    try {
        & $uvExe run --python 3.10 --env-file .env python $pythonScriptPath
    }
    finally {
        Remove-Item -LiteralPath $pythonScriptPath -ErrorAction SilentlyContinue
    }
}
finally {
    Pop-Location
}
