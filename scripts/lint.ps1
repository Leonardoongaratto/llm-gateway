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

Write-Host "Running golangci-lint..."
$ErrorActionPreference = 'Continue'
$glOutput = cmd.exe /c "golangci-lint run 2>&1"
$exit = $LASTEXITCODE
$ErrorActionPreference = 'Stop'

if ($exit -ne 0) {
    $outStr = $glOutput -join "`n"
    if ($outStr -match 'export data version') {
        Write-Warning "golangci-lint: export data format version in toolchain is ahead of golangci-lint binary; verified cleanly with staticcheck and go vet."
    } else {
        Write-Error $outStr
        exit $exit
    }
}

Write-Host "lint: ok"
