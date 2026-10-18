param(
    [switch]$Write
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if ($Write) {
    Write-Host "Formatting Go files with gofmt -w..."
    gofmt -w cmd internal
    Write-Host "gofmt: formatted ok"
    exit 0
}

$unformatted = gofmt -l cmd internal
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($unformatted) {
    Write-Error "gofmt: files need formatting (run: scripts\format.ps1 -Write):`n$unformatted"
    exit 1
}

Write-Host "gofmt: ok"
