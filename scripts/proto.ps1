param(
    [switch]$Check,
    [string]$Against
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "Running buf lint..."
buf lint
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running buf generate..."
buf generate
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($Check) {
    Write-Host "Verifying generated code in gen/ is up to date..."
    $diff = git diff --name-only gen/
    $untracked = git ls-files --others --exclude-standard gen/
    if ($diff -or $untracked) {
        Write-Error "gen/ is out of date. Run scripts/proto.ps1 and commit changes.`n$diff`n$untracked"
        exit 1
    }
}

if ($Against) {
    Write-Host "Running buf breaking against $Against..."
    buf breaking --against $Against
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "Proto verification passed."
