# Power BI Project - Hook Setup Script
# This script installs the Power BI TMDL pre-commit hook into your local .git folder.

$hookName = "pre-commit"
$repoRoot = Get-Location
$hookPath = Join-Path $repoRoot ".git\hooks\$hookName"
$hookFolder = Join-Path $repoRoot ".git\hooks"

# The pre-commit hook content (Shell script for Git Bash compatibility)
$hookContent = @'
#!/bin/sh
set -eu

REPO_ROOT="$(git rev-parse --show-toplevel)"
SPLITTER="$REPO_ROOT/scripts/split-expressions.ps1"

if [ ! -f "$SPLITTER" ]; then
    echo "Power BI Hook failed: missing scripts/split-expressions.ps1"
    exit 1
fi

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$SPLITTER" -RepoRoot "$REPO_ROOT"

# Stage any resulting changes from split/remove/update operations.
git add -A "Dashboard/Report.SemanticModel/definition/expressions" "Dashboard/Report.SemanticModel/definition/model.tmdl" "Dashboard/Report.SemanticModel/definition/expressions.tmdl" 2>/dev/null || true

exit 0
'@

Write-Host "--- Power BI Project: Hook Setup ---" -ForegroundColor Cyan

# 1. Verify Git repository
if (!(Test-Path ".git")) {
    Write-Error "This script must be run from the root of a Git repository."
    exit
}

# 2. Ensure hooks directory exists
if (!(Test-Path $hookFolder)) {
    New-Item -ItemType Directory -Path $hookFolder | Out-Null
    Write-Host "Created .git/hooks directory." -ForegroundColor Yellow
}

# 3. Create/Overwrite the pre-commit hook
try {
    [System.IO.File]::WriteAllText($hookPath, $hookContent)
    Write-Host "Successfully installed pre-commit hook to $hookPath" -ForegroundColor Green
    Write-Host "Expressions will now be split safely during commits (UTF-8 safe)." -ForegroundColor White
}
catch {
    Write-Error "Failed to write hook file: $($_.Exception.Message)"
}

Write-Host "`nSetup complete!" -ForegroundColor Cyan
