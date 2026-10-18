param(
    [int]$Threshold = 80,
    [switch]$Race
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$hasRace = $Race -or ($env:CGO_ENABLED -eq '1')

function Test-Package([string]$Package, [string]$CoverFile) {
    $exitCode = 1
    for ($attempt = 1; $attempt -le 8; $attempt++) {
        $flags = @()
        if ($hasRace) { $flags += "-race" }
        if ($attempt -gt 1) { $flags += "-ldflags=-buildid=retry$attempt-$([guid]::NewGuid().ToString('N'))" }
        $output = go test @flags $Package "-coverprofile=$CoverFile" 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
        if ($output -notmatch 'Application Control') { break }
    }
    Write-Host $output.TrimEnd()
    return ($exitCode -eq 0)
}

$allPackages = go list ./...
# Exclude generated code and testdata from coverage calculation
$packages = $allPackages | Where-Object { $_ -notmatch '/gen/' -and $_ -notmatch '/testdata' }
$coverFiles = @()
$failed = $false

foreach ($package in $packages) {
    $coverFile = Join-Path $env:TEMP ('llm-gateway-cover-' + ($package -replace '[^a-zA-Z0-9]', '_') + '.out')
    $coverFiles += $coverFile
    if (-not (Test-Package $package $coverFile)) { $failed = $true }
}
if ($failed) { exit 1 }

$statements = 0
$covered = 0
foreach ($coverFile in $coverFiles) {
    if (Test-Path $coverFile) {
        foreach ($line in Get-Content $coverFile | Select-Object -Skip 1) {
            $parts = $line -split ' '
            if ($parts.Length -ge 3) {
                $count = [int]$parts[1]
                $hits = [int]$parts[2]
                $statements += $count
                if ($hits -gt 0) { $covered += $count }
            }
        }
    }
}

if ($statements -eq 0) {
    Write-Host "No statements found."
    exit 1
}

$percent = [math]::Round(100 * $covered / $statements, 1)
Write-Host "Go total coverage: $percent% ($covered/$statements statements), threshold $Threshold%"
if ($percent -lt $Threshold) {
    Write-Error "Coverage $percent% is below threshold of $Threshold%"
    exit 1
}
