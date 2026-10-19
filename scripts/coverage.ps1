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
$packages = $allPackages | Where-Object { $_ -notmatch '/gen/' -and $_ -notmatch '/testdata' }
$coverFiles = @()
$failed = $false
$tempDir = [System.IO.Path]::GetTempPath()

foreach ($package in $packages) {
    $coverFile = Join-Path $tempDir ('llm-gateway-cover-' + ($package -replace '[^a-zA-Z0-9]', '_') + '.out')
    $coverFiles += $coverFile
    if (-not (Test-Package $package $coverFile)) { $failed = $true }
}
if ($failed) { exit 1 }

$statements = 0
$covered = 0
$packageResults = @()
for ($i = 0; $i -lt $packages.Count; $i++) {
    $pkg = $packages[$i]
    $coverFile = $coverFiles[$i]
    $pkgStatements = 0
    $pkgCovered = 0
    if (Test-Path $coverFile) {
        foreach ($line in Get-Content $coverFile | Select-Object -Skip 1) {
            $parts = $line -split ' '
            if ($parts.Length -ge 3) {
                $count = [int]$parts[1]
                $hits = [int]$parts[2]
                $pkgStatements += $count
                if ($hits -gt 0) { $pkgCovered += $count }
            }
        }
    }
    $statements += $pkgStatements
    $covered += $pkgCovered
    if ($pkgStatements -gt 0) {
        $pkgPercent = [math]::Round(100 * $pkgCovered / $pkgStatements, 1)
        $packageResults += [PSCustomObject]@{
            Package = $pkg -replace '^github.com/Leonardoongaratto/llm-gateway/', ''
            Coverage = "$pkgPercent%"
            Statements = "$pkgCovered/$pkgStatements"
        }
    }
}

if ($statements -eq 0) {
    Write-Host "No statements found."
    exit 1
}

$percent = [math]::Round(100 * $covered / $statements, 1)
$packageResults | Format-Table -AutoSize | Out-String | Write-Host
Write-Host "Go total coverage: $percent% ($covered/$statements statements), threshold $Threshold%"
if ($percent -lt $Threshold) {
    Write-Error "Coverage $percent% is below threshold of $Threshold%"
    exit 1
}
