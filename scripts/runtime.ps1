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

Get-Process edge, gateway, control, budget, usage, fakeupstream -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "$binDir*" } | Stop-Process -Force

$upstreamService = @{ Name = "fakeupstream"; Cmd = "./scripts/fakeupstream"; Exe = Join-Path $binDir "fakeupstream.exe"; Port = 8999 }
$coreServices = @(
    @{ Name = "control"; Cmd = "./cmd/control"; Exe = Join-Path $binDir "control.exe"; Port = 8790 },
    @{ Name = "budget"; Cmd = "./cmd/budget"; Exe = Join-Path $binDir "budget.exe"; Port = 8792 },
    @{ Name = "usage"; Cmd = "./cmd/usage"; Exe = Join-Path $binDir "usage.exe"; Port = 8791 },
    @{ Name = "edge"; Cmd = "./cmd/edge"; Exe = Join-Path $binDir "edge.exe"; Port = 8787 }
)
$gwService = @{ Name = "gateway"; Cmd = "./cmd/gateway"; Exe = Join-Path $binDir "gateway.exe"; Port = 8788 }

Write-Host "==> Compiling service binaries..."
$allServices = @($upstreamService) + $coreServices + @($gwService)
foreach ($svc in $allServices) {
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
    Write-Host "==> Starting fake upstream and backend services..."
    $pUpstream = Start-Process -FilePath $upstreamService.Exe -PassThru -NoNewWindow
    $processes += @{ Service = $upstreamService; Process = $pUpstream }
    Write-Host "Started fake upstream (PID $($pUpstream.Id)) on port 8999"

    foreach ($svc in $coreServices) {
        $p = Start-Process -FilePath $svc.Exe -PassThru -NoNewWindow
        $processes += @{ Service = $svc; Process = $p }
        Write-Host "Started $($svc.Name) (PID $($p.Id)) on HTTP port $($svc.Port)"
    }

    Write-Host "==> Validating core backend endpoints..."
    $endpoints = @("/livez", "/readyz", "/metrics")
    foreach ($entry in $processes) {
        if ($entry.Service.Name -eq "fakeupstream") { continue }
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

    Write-Host "==> Seeding control service with pricing and key..."
    $adminBase = "http://127.0.0.1:8790"

    $pricingPayload = '[{"model":"gpt-4o","provider":"openai","input_per_mtok_micros":5000000,"output_per_mtok_micros":15000000}]'
    $pricingCode = $pricingPayload | & curl.exe -s -o /dev/null -w "%{http_code}" -X PUT -H "Authorization: Bearer runtime-admin-token" -H "Content-Type: application/json" -d '@-' "$adminBase/admin/pricing"
    if ($pricingCode -ne "200") {
        Write-Error "control PUT /admin/pricing failed with HTTP $pricingCode"
        exit 1
    }
    Write-Host "  [OK] control: seeded pricing for model gpt-4o"

    $createPayload = '{"name":"runtime-key","rpm":60}'
    $createResp = $createPayload | & curl.exe -s -X POST -H "Authorization: Bearer runtime-admin-token" -H "Content-Type: application/json" -d '@-' "$adminBase/admin/keys"
    $createdKey = $createResp | ConvertFrom-Json
    if (-not $createdKey.id -or -not $createdKey.key) {
        Write-Error "control POST /admin/keys failed: $createResp"
        exit 1
    }
    $rawVirtualKey = $createdKey.key
    Write-Host "  [OK] control: created virtual key $($createdKey.id) (key prefix: $($rawVirtualKey.Substring(0, 4)))"

    $limitsPayload = '{"key_id":"' + $createdKey.id + '","rpm":60,"daily_limit_micros":1000000000,"monthly_limit_micros":10000000000}'
    $limitsCode = $limitsPayload | & curl.exe -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '@-' "http://127.0.0.1:8792/admin/limits"
    if ($limitsCode -ne "200") {
        Write-Error "budget POST /admin/limits failed with HTTP $limitsCode"
        exit 1
    }
    Write-Host "  [OK] budget: configured limits for virtual key $($createdKey.id)"

    Write-Host "==> Starting gateway with control, budget, and fake upstream..."
    $env:LGW_CONTROL_ADDR = "127.0.0.1:9102"
    $env:LGW_BUDGET_ADDR = "127.0.0.1:9101"
    $env:LGW_OPENAI_BASE_URL = "http://127.0.0.1:8999"
    $env:LGW_OPENAI_API_KEY = "test-upstream-key"

    $pGw = Start-Process -FilePath $gwService.Exe -PassThru -NoNewWindow
    $processes += @{ Service = $gwService; Process = $pGw }
    Write-Host "Started gateway (PID $($pGw.Id)) on HTTP port $($gwService.Port)"

    $gwBase = "http://127.0.0.1:$($gwService.Port)"
    $gwReady = $false
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if ($pGw.HasExited) {
            Write-Error "Gateway exited prematurely with code $($pGw.ExitCode)"
            exit 1
        }
        $curlOut = & curl.exe -s -o /dev/null -w "%{http_code}" "$gwBase/readyz" 2>$null
        if ($curlOut -eq "200") {
            $gwReady = $true
            break
        }
        Start-Sleep -Milliseconds 250
    }

    if (-not $gwReady) {
        Write-Error "Gateway did not become ready on $gwBase/readyz within $TimeoutSeconds s"
        exit 1
    }
    Write-Host "  [OK] gateway: /livez, /readyz, /metrics -> HTTP 200"

    Write-Host "==> Validating gateway routes end-to-end..."
    $gwUnauth = & curl.exe -s -o /dev/null -w "%{http_code}" "$gwBase/v1/models"
    if ($gwUnauth -ne "401") {
        Write-Error "gateway GET /v1/models without key returned $gwUnauth (expected 401)"
        exit 1
    }
    Write-Host "  [OK] gateway: GET /v1/models without key -> 401"

    $gwGarbage = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer garbage-key" "$gwBase/v1/models"
    if ($gwGarbage -ne "401") {
        Write-Error "gateway GET /v1/models with garbage key returned $gwGarbage (expected 401)"
        exit 1
    }
    Write-Host "  [OK] gateway: GET /v1/models with garbage key -> 401"

    $modelsResp = & curl.exe -s -H "Authorization: Bearer $rawVirtualKey" "$gwBase/v1/models"
    if ($modelsResp -notmatch "gpt-4o") {
        Write-Error "gateway GET /v1/models with valid key did not return gpt-4o: $modelsResp"
        exit 1
    }
    Write-Host "  [OK] gateway: GET /v1/models with valid key -> 200 with model gpt-4o"

    Write-Host "==> Validating POST /v1/chat/completions (unary)..."
    $chatPayload = '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello gateway"}]}'
    $headerFile = Join-Path $binDir "headers.txt"
    $chatResp = $chatPayload | & curl.exe -s -D $headerFile -H "Authorization: Bearer $rawVirtualKey" -H "Content-Type: application/json" -d '@-' "$gwBase/v1/chat/completions"
    $headers = Get-Content $headerFile -Raw
    if ($headers -notmatch "HTTP/1.1 200") {
        Write-Error "gateway POST /v1/chat/completions failed: headers=$headers body=$chatResp"
        exit 1
    }
    if ($headers -notmatch "X-LGW-Cost-Micros:\s*\d+") {
        Write-Error "missing X-LGW-Cost-Micros header in gateway response: $headers"
        exit 1
    }
    if ($headers -notmatch "X-LGW-Provider:\s*openai") {
        Write-Error "missing or invalid X-LGW-Provider header: $headers"
        exit 1
    }
    if ($headers -notmatch "X-Request-Id:") {
        Write-Error "missing X-Request-Id header: $headers"
        exit 1
    }
    Write-Host "  [OK] gateway: POST /v1/chat/completions -> 200 OK with billing and tracing headers"

    Write-Host "==> Validating POST /v1/chat/completions (streaming)..."
    $streamPayload = '{"model":"gpt-4o","messages":[{"role":"user","content":"Stream gateway"}],"stream":true}'
    $streamResp = $streamPayload | & curl.exe -s -N -H "Authorization: Bearer $rawVirtualKey" -H "Content-Type: application/json" -d '@-' "$gwBase/v1/chat/completions" | Out-String
    if (-not $streamResp.Contains("data: [DONE]")) {
        Write-Error "gateway streaming did not return data: [DONE]: $streamResp"
        exit 1
    }
    if (-not $streamResp.Contains("Hello from fake upstream!")) {
        Write-Error "gateway streaming did not return expected chunk text: $streamResp"
        exit 1
    }
    Write-Host "  [OK] gateway: POST /v1/chat/completions (streaming) -> 200 SSE stream with [DONE]"

    $delCode = & curl.exe -s -o /dev/null -w "%{http_code}" -X DELETE -H "Authorization: Bearer runtime-admin-token" "$adminBase/admin/keys/$($createdKey.id)"
    if ($delCode -ne "204") {
        Write-Error "control DELETE /admin/keys returned $delCode (expected 204)"
        exit 1
    }
    Write-Host "  [OK] control: revoked key $($createdKey.id) -> HTTP 204"

    Write-Host "==> All 5 services and complete pipeline passed runtime verification successfully!"
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
