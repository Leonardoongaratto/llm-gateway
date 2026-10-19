param(
    [int]$TimeoutSeconds = 15
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$binDir = Join-Path $root "bin"
if (-not (Test-Path $binDir)) {
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null
}

Get-Process edge, gateway, control, budget, usage -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "$binDir*" } | Stop-Process -Force

$services = @(
    @{ Name = "edge"; Cmd = "./cmd/edge"; Exe = Join-Path $binDir "edge.exe"; Port = 8787 },
    @{ Name = "gateway"; Cmd = "./cmd/gateway"; Exe = Join-Path $binDir "gateway.exe"; Port = 8788 },
    @{ Name = "control"; Cmd = "./cmd/control"; Exe = Join-Path $binDir "control.exe"; Port = 8790 },
    @{ Name = "budget"; Cmd = "./cmd/budget"; Exe = Join-Path $binDir "budget.exe"; Port = 8792 },
    @{ Name = "usage"; Cmd = "./cmd/usage"; Exe = Join-Path $binDir "usage.exe"; Port = 8791 }
)

Write-Host "==> Compiling service binaries..."
foreach ($svc in $services) {
    Write-Host "Building $($svc.Name)..."
    & go build -o $svc.Exe $svc.Cmd
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to build $($svc.Name)"
        exit 1
    }
}

$env:LGW_ADMIN_TOKEN = "runtime-admin-token"
$env:LGW_KEY_PEPPER = "runtime-key-pepper"

$processes = @()

try {
    Write-Host "==> Starting 5 services in background..."
    foreach ($svc in $services) {
        $p = Start-Process -FilePath $svc.Exe -PassThru -NoNewWindow
        $processes += @{ Service = $svc; Process = $p }
        Write-Host "Started $($svc.Name) (PID $($p.Id)) on HTTP port $($svc.Port)"
    }

    Write-Host "==> Validating endpoints via curl.exe..."
    $endpoints = @("/livez", "/readyz", "/metrics")

    foreach ($entry in $processes) {
        $svc = $entry.Service
        $p = $entry.Process
        $base = "http://127.0.0.1:$($svc.Port)"

        $ready = $false
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        while ($sw.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
            if ($p.HasExited) {
                Write-Error "Service $($svc.Name) exited prematurely with code $($p.ExitCode)"
                exit 1
            }
            $curlOut = & curl.exe -s -o /dev/null -w "%{http_code}" "$base/livez" 2>$null
            if ($curlOut -eq "200") {
                $ready = $true
                break
            }
            Start-Sleep -Milliseconds 250
        }

        if (-not $ready) {
            Write-Error "Service $($svc.Name) did not respond 200 on $base/livez within $TimeoutSeconds s"
            exit 1
        }

        foreach ($ep in $endpoints) {
            $url = "$base$ep"
            $code = & curl.exe -s -o /dev/null -w "%{http_code}" $url
            if ($code -ne "200") {
                Write-Error "Service $($svc.Name) $url returned HTTP $code (expected 200)"
                exit 1
            }
            Write-Host "  [OK] $($svc.Name): $url -> HTTP $code"
        }
    }

    Write-Host "==> Validating control admin routes..."
    $adminBase = "http://127.0.0.1:8790"
    $unauthCode = & curl.exe -s -o /dev/null -w "%{http_code}" "$adminBase/admin/keys"
    if ($unauthCode -ne "401") {
        Write-Error "control GET /admin/keys without token returned $unauthCode (expected 401)"
        exit 1
    }
    Write-Host "  [OK] control: unauthorized request correctly rejected with 401"

    $createPayload = '{"name":"runtime-key","rpm":60}'
    $createResp = $createPayload | & curl.exe -s -X POST -H "Authorization: Bearer runtime-admin-token" -H "Content-Type: application/json" -d '@-' "$adminBase/admin/keys"
    $createdKey = $createResp | ConvertFrom-Json
    if (-not $createdKey.id -or -not $createdKey.key) {
        Write-Error "control POST /admin/keys failed: $createResp"
        exit 1
    }
    Write-Host "  [OK] control: created key $($createdKey.id) (key prefix: $($createdKey.key.Substring(0, 4)))"

    $delCode = & curl.exe -s -o /dev/null -w "%{http_code}" -X DELETE -H "Authorization: Bearer runtime-admin-token" "$adminBase/admin/keys/$($createdKey.id)"
    if ($delCode -ne "204") {
        Write-Error "control DELETE /admin/keys returned $delCode (expected 204)"
        exit 1
    }
    Write-Host "  [OK] control: revoked key $($createdKey.id) -> HTTP 204"

    Write-Host "==> All 5 services passed runtime verification successfully!"
}
finally {
    Write-Host "==> Cleaning up service processes..."
    foreach ($entry in $processes) {
        $p = $entry.Process
        if ($p -and -not $p.HasExited) {
            try {
                Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
                Write-Host "Stopped $($entry.Service.Name) (PID $($p.Id))"
            } catch {
            }
        }
    }
    Start-Sleep -Milliseconds 500
    if (Test-Path $binDir) {
        Remove-Item -Path $binDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
