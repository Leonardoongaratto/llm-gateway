$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "Running go vet..."
go vet ./...
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running buf checks via proto.ps1..."
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\proto.ps1 -Check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running staticcheck..."
go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running golangci-lint v2..."
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "lint: ok"
