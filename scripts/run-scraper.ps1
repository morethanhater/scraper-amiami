Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

function Read-OptionalScrapeValue {
    param(
        [string]$Prompt,
        [string]$CurrentValue
    )

    $displayValue = if ($CurrentValue -eq "") { "<empty>" } else { $CurrentValue }
    $inputValue = Read-Host "$Prompt [$displayValue]"
    if ([string]::IsNullOrWhiteSpace($inputValue)) {
        return $CurrentValue
    }

    if ($inputValue.Trim() -eq "-") {
        return ""
    }

    return $inputValue.Trim()
}

function Read-ValidatedScrapeValue {
    param(
        [string]$Prompt,
        [string]$CurrentValue,
        [string[]]$AllowedValues,
        [bool]$AllowEmpty = $true
    )

    while ($true) {
        $value = Read-OptionalScrapeValue -Prompt $Prompt -CurrentValue $CurrentValue
        if ($AllowEmpty -and [string]::IsNullOrWhiteSpace($value)) {
            return ""
        }

        if ($AllowedValues -contains $value.ToUpperInvariant()) {
            return $value.ToUpperInvariant()
        }

        Write-Host "Invalid value. Allowed values: $($AllowedValues -join ', ')" -ForegroundColor Yellow
    }
}

function Read-ValidatedPositiveIntOrBlank {
    param(
        [string]$Prompt,
        [string]$CurrentValue
    )

    while ($true) {
        $value = Read-OptionalScrapeValue -Prompt $Prompt -CurrentValue $CurrentValue
        if ([string]::IsNullOrWhiteSpace($value)) {
            return ""
        }

        $parsed = 0
        if ([int]::TryParse($value, [ref]$parsed) -and $parsed -gt 0) {
            return $parsed.ToString()
        }

        Write-Host "Enter a positive integer or leave blank for unlimited pages." -ForegroundColor Yellow
    }
}

function Show-ScraperSettings {
    param(
        [string]$EnvPath
    )

    Write-Host "Current scraper settings:" -ForegroundColor Cyan
    Write-Host "  keyword: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_KEYWORD')"
    Write-Host "  num pages: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_NUM_PAGES')"
    Write-Host "  types: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_TYPES' -DefaultValue 'BACK_ORDER,NEW,PRE_ORDER,PRE_OWNED')"
    Write-Host "  category1: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_CATEGORY1')"
    Write-Host "  category2: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_CATEGORY2' -DefaultValue 'BISHOUJO_FIGURES')"
    Write-Host "  category3: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_CATEGORY3')"
    Write-Host "  sort key: $(Get-EnvValue -EnvPath $EnvPath -Key 'AMIAMI_SCRAPE_SORT_KEY' -DefaultValue 'RECENT_UPDATE')"
}

function Get-AllowedScrapeValues {
    param(
        [string]$EnvPath,
        [string]$Key,
        [string[]]$Fallback
    )

    $raw = Get-EnvValue -EnvPath $EnvPath -Key $Key
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $Fallback
    }

    $values = @($raw.Split(",") | ForEach-Object { $_.Trim().ToUpperInvariant() } | Where-Object { $_ })
    if ($values.Count -eq 0) {
        return $Fallback
    }

    return $values
}

function Show-IndexedOptions {
    param(
        [string[]]$Options
    )

    for ($index = 0; $index -lt $Options.Count; $index++) {
        Write-Host ("  [{0}] {1}" -f ($index + 1), $Options[$index])
    }
}

function Read-SelectedOption {
    param(
        [string]$Prompt,
        [string]$CurrentValue,
        [string[]]$AllowedValues,
        [bool]$AllowEmpty = $true
    )

    while ($true) {
        $displayValue = if ([string]::IsNullOrWhiteSpace($CurrentValue)) { "<empty>" } else { $CurrentValue }
        Write-Host ""
        Write-Host "$Prompt [$displayValue]" -ForegroundColor Cyan
        Show-IndexedOptions -Options $AllowedValues
        $selection = Read-Host "Choose one number, Enter to keep current, '-' to clear"

        if ([string]::IsNullOrWhiteSpace($selection)) {
            return $CurrentValue
        }

        if ($selection.Trim() -eq "-") {
            if ($AllowEmpty) {
                return ""
            }
            Write-Host "This field cannot be empty." -ForegroundColor Yellow
            continue
        }

        $parsed = 0
        if ([int]::TryParse($selection.Trim(), [ref]$parsed) -and $parsed -ge 1 -and $parsed -le $AllowedValues.Count) {
            return $AllowedValues[$parsed - 1]
        }

        Write-Host "Choose a valid number from the list." -ForegroundColor Yellow
    }
}

function Read-SelectedOptions {
    param(
        [string]$Prompt,
        [string]$CurrentValue,
        [string[]]$AllowedValues
    )

    while ($true) {
        $displayValue = if ([string]::IsNullOrWhiteSpace($CurrentValue)) { "<empty>" } else { $CurrentValue }
        Write-Host ""
        Write-Host "$Prompt [$displayValue]" -ForegroundColor Cyan
        Show-IndexedOptions -Options $AllowedValues
        $selection = Read-Host "Choose one or more numbers separated by commas, Enter to keep current"

        if ([string]::IsNullOrWhiteSpace($selection)) {
            return $CurrentValue
        }

        $parts = @($selection.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ })
        if ($parts.Count -eq 0) {
            Write-Host "At least one choice is required." -ForegroundColor Yellow
            continue
        }

        $selectedValues = New-Object System.Collections.Generic.List[string]
        $invalidSelections = New-Object System.Collections.Generic.List[string]
        foreach ($part in $parts) {
            $parsed = 0
            if ([int]::TryParse($part, [ref]$parsed) -and $parsed -ge 1 -and $parsed -le $AllowedValues.Count) {
                $selectedValue = $AllowedValues[$parsed - 1]
                if (-not $selectedValues.Contains($selectedValue)) {
                    $selectedValues.Add($selectedValue)
                }
            }
            else {
                $invalidSelections.Add($part)
            }
        }

        if ($invalidSelections.Count -gt 0) {
            Write-Host "Invalid selections: $($invalidSelections -join ', ')" -ForegroundColor Yellow
            continue
        }

        if ($selectedValues.Count -eq 0) {
            Write-Host "At least one choice is required." -ForegroundColor Yellow
            continue
        }

        return ($selectedValues -join ",")
    }
}

function Update-ScraperSettingsInteractively {
    param(
        [string]$EnvPath
    )

    $typeOptions = Get-AllowedScrapeValues -EnvPath $EnvPath -Key "AMIAMI_AVAILABLE_SCRAPE_TYPES" -Fallback @("PRE_ORDER", "BACK_ORDER", "NEW", "PRE_OWNED")
    $sortOptions = Get-AllowedScrapeValues -EnvPath $EnvPath -Key "AMIAMI_AVAILABLE_SCRAPE_SORT_KEY" -Fallback @("RECENT_UPDATE", "RECOMMENDATION", "RELEASE_DATE", "PREOWNED")
    $category1Options = Get-AllowedScrapeValues -EnvPath $EnvPath -Key "AMIAMI_AVAILABLE_SCRAPE_CATEGORY1" -Fallback @("TRADING_FIGURES", "CHARACTER_GOODS", "FASHION", "CARD_GAMES", "TRADING_CARDS", "HOUSEHOLD_GOODS", "AGE_RESTRICTED_PRODUCTS")
    $category2Options = Get-AllowedScrapeValues -EnvPath $EnvPath -Key "AMIAMI_AVAILABLE_SCRAPE_CATEGORY2" -Fallback @("BISHOUJO_FIGURES", "CHARACTER_FIGURES", "FOREIGN_FIGURES", "DOLLS", "SCALE_MILITARY", "CAR_MODELS", "TRAIN_MODELS", "TOOLS_PAINTS_MATERIAL", "CAR_PLASTIC_MODEL_KITS", "BOOKS_MANGAS", "VIDEO_GAMES", "BLURAY_DISCS", "DVDS", "CDS", "CARD_SUPPLIES", "KIDS_TOYS", "STATIONERY", "JIGSAW_PUZZLES", "CALENDARS")
    $category3Options = Get-AllowedScrapeValues -EnvPath $EnvPath -Key "AMIAMI_AVAILABLE_SCRAPE_CATEGORY3" -Fallback @("GUNDAM_TOYS", "ROBOTS", "TOKUSATSU_TOYS", "PLUSH_DOLLS")

    Write-Host "Press Enter to keep the current value." -ForegroundColor DarkGray
    Write-Host "Enter '-' to clear an optional value." -ForegroundColor DarkGray

    $keyword = Read-OptionalScrapeValue -Prompt "Keyword" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_KEYWORD")
    $numPages = Read-ValidatedPositiveIntOrBlank -Prompt "Max pages (blank = all pages)" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_NUM_PAGES")
    $types = Read-SelectedOptions -Prompt "Types" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_TYPES" -DefaultValue "BACK_ORDER,NEW,PRE_ORDER,PRE_OWNED") -AllowedValues $typeOptions
    $category1 = Read-SelectedOption -Prompt "Category1" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY1") -AllowedValues $category1Options
    $category2 = Read-SelectedOption -Prompt "Category2" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY2" -DefaultValue "BISHOUJO_FIGURES") -AllowedValues $category2Options
    $category3 = Read-SelectedOption -Prompt "Category3" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY3") -AllowedValues $category3Options
    $sortKey = Read-SelectedOption -Prompt "Sort key" -CurrentValue (Get-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_SORT_KEY" -DefaultValue "RECENT_UPDATE") -AllowedValues $sortOptions -AllowEmpty $false

    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_KEYWORD" -Value $keyword
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_NUM_PAGES" -Value $numPages
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_TYPES" -Value $types
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY1" -Value $category1
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY2" -Value $category2
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_CATEGORY3" -Value $category3
    Set-EnvValue -EnvPath $EnvPath -Key "AMIAMI_SCRAPE_SORT_KEY" -Value $sortKey

    Write-Host ""
    Write-Host "Updated .env scraper settings." -ForegroundColor Green
    Show-ScraperSettings -EnvPath $EnvPath
}

$repoRoot = Get-RepoRoot
$uvExe = Get-UvExe
Set-LocalUvCache -RepoRoot $repoRoot

Push-Location $repoRoot
try {
    & (Join-Path $PSScriptRoot "setup.ps1")
    $envPath = Join-Path $repoRoot ".env"

    Write-Host ""
    Show-ScraperSettings -EnvPath $envPath
    $choice = Read-Host "Use the current scraper settings from .env? [Y/n]"
    if ($choice.Trim().ToLowerInvariant() -in @("n", "no")) {
        Write-Host ""
        Update-ScraperSettingsInteractively -EnvPath $envPath
        Write-Host ""
    }

    & $uvExe run --python 3.10 --env-file .env core/main.py
}
finally {
    Pop-Location
}
