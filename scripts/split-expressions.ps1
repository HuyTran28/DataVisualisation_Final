param(
    [string]$RepoRoot
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (git rev-parse --show-toplevel)
}

$semanticModelPath = Join-Path $RepoRoot 'Dashboard\Report.SemanticModel\definition'
$expressionsFile = Join-Path $semanticModelPath 'expressions.tmdl'
$expressionsFolder = Join-Path $semanticModelPath 'expressions'
$modelFile = Join-Path $semanticModelPath 'model.tmdl'

if (-not (Test-Path $expressionsFile)) {
    Write-Host 'No consolidated expressions.tmdl found. Nothing to split.'
    exit 0
}

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$utf8 = [System.Text.Encoding]::UTF8

$raw = [System.IO.File]::ReadAllText($expressionsFile, $utf8)
$blocks = [regex]::Split($raw, '(?m)(?=^expression\s+)') | Where-Object { $_.Trim().Length -gt 0 }

if (-not (Test-Path $expressionsFolder)) {
    New-Item -ItemType Directory -Path $expressionsFolder | Out-Null
}

$names = New-Object System.Collections.Generic.List[string]

foreach ($block in $blocks) {
    $trimmed = $block.TrimEnd()
    if ($trimmed -match '(?m)^expression\s+([^\s=]+)\s*=') {
        $name = $Matches[1]
        $names.Add($name)
        $target = Join-Path $expressionsFolder ("$name.tmdl")
        [System.IO.File]::WriteAllText($target, ($trimmed + [Environment]::NewLine), $utf8NoBom)
        $legacyTemplate = Join-Path $expressionsFolder ("$name.tmdl.template")
        if (Test-Path $legacyTemplate) {
            Remove-Item $legacyTemplate -Force
        }
        Write-Host ("Split expression: {0}.tmdl" -f $name)
    }
}

if ($names.Count -eq 0) {
    throw 'Unable to parse any expression blocks from expressions.tmdl. Aborting to avoid corrupting model.tmdl.'
}

Remove-Item $expressionsFile -Force

$modelContent = [System.IO.File]::ReadAllText($modelFile, $utf8)
$modelContent = [regex]::Replace($modelContent, '(?m)^ref expression [^\r\n]*\r?\n', '')
$modelContent = [regex]::Replace($modelContent, '(\r?\n){3,}', "`r`n`r`n")

$refLines = ($names | ForEach-Object { "ref expression $_" }) -join "`r`n"
if ($modelContent.TrimEnd().Length -gt 0) {
    $modelContent = $modelContent.TrimEnd("`r", "`n") + "`r`n`r`n" + $refLines + "`r`n"
} else {
    $modelContent = $refLines + "`r`n"
}

[System.IO.File]::WriteAllText($modelFile, $modelContent, $utf8NoBom)
Write-Host 'Updated model.tmdl expression refs.'
