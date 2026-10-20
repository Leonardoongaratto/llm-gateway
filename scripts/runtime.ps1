param(
    [int]$TimeoutSeconds = 15
)

$ErrorActionPreference = 'Stop'
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$binDir = Join-Path $root "bin"
if (-not (Test-Path $binDir)) {
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null
}

$webDir = Join-Path $env:TEMP ("lgw_web_" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $webDir -Force | Out-Null
Set-Content -Path (Join-Path $webDir "index.html") -Value "<html><body>LLM Gateway Dashboard</body></html>" -Encoding UTF8
Set-Content -Path (Join-Path $webDir "app.js") -Value "console.log('dashboard');" -Encoding UTF8

function Invoke-CurlBinary {
    param(
        [string[]]$CurlArgs,
        [string]$Payload
    )
    if ($Payload) {
        $tmpFile = Join-Path $env:TEMP ("lgw_rt_" + [guid]::NewGuid().ToString("N") + ".json")
        try {
            [System.IO.File]::WriteAllText($tmpFile, $Payload, [System.Text.UTF8Encoding]::new($false))
            & curl.exe @CurlArgs "--data-binary" "@$tmpFile"
        } finally {
            if (Test-Path $tmpFile) { Remove-Item $tmpFile -Force -ErrorAction SilentlyContinue }
        }
    } else {
        & curl.exe @CurlArgs
    }
}

Get-Process edge, gateway, control, budget, usage, fakeupstream, lgw-dev -ErrorAction SilentlyContinue | Where-Object { $_.Path -like "$binDir*" } | Stop-Process -Force

$upstreamService = @{ Name = "fakeupstream"; Cmd = "./scripts/fakeupstream"; Exe = Join-Path $binDir "fakeupstream.exe"; Port = 8999 }
$coreServices = @(
    @{ Name = "control"; Cmd = "./cmd/control"; Exe = Join-Path $binDir "control.exe"; Port = 8790 },
    @{ Name = "budget"; Cmd = "./cmd/budget"; Exe = Join-Path $binDir "budget.exe"; Port = 8792 },
    @{ Name = "usage"; Cmd = "./cmd/usage"; Exe = Join-Path $binDir "usage.exe"; Port = 8791 },
    @{ Name = "edge"; Cmd = "./cmd/edge"; Exe = Join-Path $binDir "edge.exe"; Port = 8787 }
)
$gwService = @{ Name = "gateway"; Cmd = "./cmd/gateway"; Exe = Join-Path $binDir "gateway.exe"; Port = 8788 }
$devService = @{ Name = "lgw-dev"; Cmd = "./cmd/lgw-dev"; Exe = Join-Path $binDir "lgw-dev.exe"; Port = 8787 }

Write-Host "==> Compiling service binaries..."
$allServices = @($upstreamService) + $coreServices + @($gwService) + @($devService)
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
$env:LGW_WEB_DIR = $webDir

$processes = @()

try {
    Write-Host "==> Starting Phase 1: Microservices Topology..."
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
    $pricingCode = Invoke-CurlBinary -CurlArgs @("-s", "-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", "-H", "Authorization: Bearer runtime-admin-token", "-H", "Content-Type: application/json", "$adminBase/admin/pricing") -Payload $pricingPayload
    if ($pricingCode -ne "200") {
        Write-Error "control PUT /admin/pricing failed with HTTP $pricingCode"
        exit 1
    }
    Write-Host "  [OK] control: seeded pricing for model gpt-4o"

    $createPayload = '{"name":"runtime-key","rpm":60}'
    $createResp = Invoke-CurlBinary -CurlArgs @("-s", "-X", "POST", "-H", "Authorization: Bearer runtime-admin-token", "-H", "Content-Type: application/json", "$adminBase/admin/keys") -Payload $createPayload | Out-String
    $createdKey = $createResp | ConvertFrom-Json
    if (-not $createdKey.id -or -not $createdKey.key) {
        Write-Error "control POST /admin/keys failed: $createResp"
        exit 1
    }
    $rawVirtualKey = $createdKey.key
    Write-Host "  [OK] control: created virtual key $($createdKey.id) (key prefix: $($rawVirtualKey.Substring(0, 4)))"

    $limitsPayload = '{"key_id":"' + $createdKey.id + '","rpm":60,"daily_limit_micros":1000000000,"monthly_limit_micros":10000000000}'
    $limitsCode = Invoke-CurlBinary -CurlArgs @("-s", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", "-H", "Authorization: Bearer runtime-admin-token", "-H", "Content-Type: application/json", "http://127.0.0.1:8792/admin/limits") -Payload $limitsPayload
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

    Write-Host "==> Validating complete pipeline through Edge proxy (port 8787)..."
    $edgeBase = "http://127.0.0.1:8787"

    $healthResp = & curl.exe -s "$edgeBase/health" | Out-String
    if ($healthResp -notmatch '"status":\s*"ok"') {
        Write-Error "edge /health did not report status ok: $healthResp"
        exit 1
    }
    if ($healthResp -notmatch '"gateway":\s*"ok"' -or $healthResp -notmatch '"control":\s*"ok"' -or $healthResp -notmatch '"budget":\s*"ok"' -or $healthResp -notmatch '"usage":\s*"ok"') {
        Write-Error "edge /health missing one or more healthy services: $healthResp"
        exit 1
    }
    Write-Host "  [OK] edge: /health -> 200 OK with all 4 services healthy"

    $edgeLimitsCode = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer runtime-admin-token" "$edgeBase/admin/limits/$($createdKey.id)"
    if ($edgeLimitsCode -ne "200") {
        Write-Error "edge GET /admin/limits/$($createdKey.id) returned $edgeLimitsCode (expected 200)"
        exit 1
    }
    Write-Host "  [OK] edge: GET /admin/limits/$($createdKey.id) -> HTTP 200"

    $edgeSummaryCode = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer runtime-admin-token" "$edgeBase/admin/summary"
    if ($edgeSummaryCode -ne "200") {
        Write-Error "edge GET /admin/summary returned $edgeSummaryCode (expected 200)"
        exit 1
    }
    Write-Host "  [OK] edge: GET /admin/summary -> HTTP 200"

    $dashResp = & curl.exe -s "$edgeBase/dashboard" | Out-String
    if ($dashResp -notmatch "LLM Gateway Dashboard") {
        Write-Error "edge GET /dashboard failed to serve index.html: $dashResp"
        exit 1
    }
    $dashSpaResp = & curl.exe -s "$edgeBase/dashboard/projetos" | Out-String
    if ($dashSpaResp -notmatch "LLM Gateway Dashboard") {
        Write-Error "edge GET /dashboard/projetos failed SPA fallback: $dashSpaResp"
        exit 1
    }
    $dashJsResp = & curl.exe -s "$edgeBase/dashboard/app.js" | Out-String
    if ($dashJsResp -notmatch "dashboard") {
        Write-Error "edge GET /dashboard/app.js failed to serve static script: $dashJsResp"
        exit 1
    }
    Write-Host "  [OK] edge: /dashboard (root, SPA fallback, static script) -> HTTP 200"

    $code413 = & curl.exe -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Length: 11000000" "$edgeBase/v1/chat/completions"
    if ($code413 -ne "413") {
        Write-Error "edge request > 10MB returned HTTP $code413 (expected 413)"
        exit 1
    }
    Write-Host "  [OK] edge: POST > 10MB payload -> HTTP 413 Payload Too Large"

    $gwUnauth = & curl.exe -s -o /dev/null -w "%{http_code}" "$edgeBase/v1/models"
    if ($gwUnauth -ne "401") {
        Write-Error "edge GET /v1/models without key returned $gwUnauth (expected 401)"
        exit 1
    }
    Write-Host "  [OK] edge: GET /v1/models without key -> 401"

    $gwGarbage = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer garbage-key" "$edgeBase/v1/models"
    if ($gwGarbage -ne "401") {
        Write-Error "edge GET /v1/models with garbage key returned $gwGarbage (expected 401)"
        exit 1
    }
    Write-Host "  [OK] edge: GET /v1/models with garbage key -> 401"

    $modelsResp = & curl.exe -s -H "Authorization: Bearer $rawVirtualKey" "$edgeBase/v1/models"
    if ($modelsResp -notmatch "gpt-4o") {
        Write-Error "edge GET /v1/models with valid key did not return gpt-4o: $modelsResp"
        exit 1
    }
    Write-Host "  [OK] edge: GET /v1/models with valid key -> 200 with model gpt-4o"

    Write-Host "==> Validating POST /v1/chat/completions via Edge (unary)..."
    $chatPayload = '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello gateway"}]}'
    $headerFile = Join-Path $binDir "headers.txt"
    $chatResp = Invoke-CurlBinary -CurlArgs @("-s", "-D", $headerFile, "-H", "Authorization: Bearer $rawVirtualKey", "-H", "Content-Type: application/json", "$edgeBase/v1/chat/completions") -Payload $chatPayload | Out-String
    $headers = Get-Content $headerFile -Raw
    if ($headers -notmatch "HTTP/1.1 200") {
        Write-Error "edge POST /v1/chat/completions failed: headers=$headers body=$chatResp"
        exit 1
    }
    if ($headers -notmatch "X-LGW-Cost-Micros:\s*\d+") {
        Write-Error "missing X-LGW-Cost-Micros header in edge response: $headers"
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
    Write-Host "  [OK] edge: POST /v1/chat/completions -> 200 OK with billing and tracing headers"

    Write-Host "==> Validating POST /v1/chat/completions via Edge (streaming)..."
    $streamPayload = '{"model":"gpt-4o","messages":[{"role":"user","content":"Stream gateway"}],"stream":true}'
    $streamResp = Invoke-CurlBinary -CurlArgs @("-s", "-N", "-H", "Authorization: Bearer $rawVirtualKey", "-H", "Content-Type: application/json", "$edgeBase/v1/chat/completions") -Payload $streamPayload | Out-String
    if (-not $streamResp.Contains("data: [DONE]")) {
        Write-Error "edge streaming did not return data: [DONE]: $streamResp"
        exit 1
    }
    if (-not $streamResp.Contains("Hello from fake upstream!")) {
        Write-Error "edge streaming did not return expected chunk text: $streamResp"
        exit 1
    }
    Write-Host "  [OK] edge: POST /v1/chat/completions (streaming) -> 200 SSE stream with [DONE]"

    Write-Host "==> Validating POST /v1/messages via Edge (Anthropic)..."
    $msgPayload = '{"model":"gpt-4o","max_tokens":100,"messages":[{"role":"user","content":"Hello anthropic"}]}'
    $msgResp = Invoke-CurlBinary -CurlArgs @("-s", "-H", "Authorization: Bearer $rawVirtualKey", "-H", "Content-Type: application/json", "$edgeBase/v1/messages") -Payload $msgPayload | Out-String
    if (-not $msgResp.Contains('"type":"message"')) {
        Write-Error "edge POST /v1/messages failed: $msgResp"
        exit 1
    }
    Write-Host "  [OK] edge: POST /v1/messages -> 200 OK with Anthropic format"

    $delCode = & curl.exe -s -o /dev/null -w "%{http_code}" -X DELETE -H "Authorization: Bearer runtime-admin-token" "$edgeBase/admin/keys/$($createdKey.id)"
    if ($delCode -ne "204") {
        Write-Error "edge DELETE /admin/keys returned $delCode (expected 204)"
        exit 1
    }
    Write-Host "  [OK] edge: revoked key $($createdKey.id) -> HTTP 204"

    $listResp = & curl.exe -s -H "Authorization: Bearer runtime-admin-token" "$edgeBase/admin/keys" | Out-String
    $keys = $listResp | ConvertFrom-Json
    $revokedKeyObj = $keys | Where-Object { $_.id -eq $createdKey.id }
    if (-not $revokedKeyObj -or $revokedKeyObj.active -ne $false) {
        Write-Error "revoked key $($createdKey.id) not marked active:false: $listResp"
        exit 1
    }
    Write-Host "  [OK] edge: verified key marked inactive in GET /admin/keys"

    Write-Host "==> Stopping Phase 1 backend services..."
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
    $processes = @()
    Start-Sleep -Milliseconds 1500

    Write-Host "==> Starting Phase 2: Standalone lgw-dev all-in-one cluster..."
    $pDev = Start-Process -FilePath $devService.Exe -ArgumentList "--mock-provider=true", "--seed-demo=true", "--edge-port=8787", "--admin-token=dev-admin-token", "--web-dir=$webDir" -PassThru -NoNewWindow
    $processes += @{ Service = $devService; Process = $pDev }
    Write-Host "Started lgw-dev (PID $($pDev.Id))"

    $devReady = $false
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if ($pDev.HasExited) {
            Write-Error "lgw-dev exited prematurely with code $($pDev.ExitCode)"
            exit 1
        }
        $curlOut = & curl.exe -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8787/health" 2>$null
        if ($curlOut -eq "200") {
            $devReady = $true
            break
        }
        Start-Sleep -Milliseconds 300
    }

    if (-not $devReady) {
        Write-Error "lgw-dev did not become ready on http://127.0.0.1:8787/health within $TimeoutSeconds s"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: /health -> HTTP 200 OK"

    $devHealthResp = & curl.exe -s "http://127.0.0.1:8787/health" | Out-String
    if ($devHealthResp -notmatch '"status":\s*"ok"') {
        Write-Error "lgw-dev /health did not report status ok: $devHealthResp"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: aggregated health verified"

    $devDashResp = & curl.exe -s "http://127.0.0.1:8787/dashboard" | Out-String
    if ($devDashResp -notmatch "LLM Gateway Dashboard") {
        Write-Error "lgw-dev /dashboard failed to serve HTML: $devDashResp"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: /dashboard -> HTTP 200 with web UI"

    $devSummaryCode = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer dev-admin-token" "http://127.0.0.1:8787/admin/summary"
    if ($devSummaryCode -ne "200") {
        Write-Error "lgw-dev GET /admin/summary returned $devSummaryCode (expected 200)"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: GET /admin/summary -> HTTP 200"

    $devLimitsCode = & curl.exe -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer dev-admin-token" "http://127.0.0.1:8787/admin/limits/key-dev-default"
    if ($devLimitsCode -ne "200") {
        Write-Error "lgw-dev GET /admin/limits/key-dev-default returned $devLimitsCode (expected 200)"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: GET /admin/limits/key-dev-default -> HTTP 200"

    $devKey = "lgw_0123456789abcdef0123456789abcdef"
    $devChatPayload = '{"model":"gpt-4o","messages":[{"role":"user","content":"Hello lgw-dev"}]}'
    $devHeaderFile = Join-Path $binDir "dev_headers.txt"
    $devChatResp = Invoke-CurlBinary -CurlArgs @("-s", "-D", $devHeaderFile, "-H", "Authorization: Bearer $devKey", "-H", "Content-Type: application/json", "http://127.0.0.1:8787/v1/chat/completions") -Payload $devChatPayload | Out-String
    $devHeaders = Get-Content $devHeaderFile -Raw
    if ($devHeaders -notmatch "HTTP/1.1 200") {
        Write-Error "lgw-dev POST /v1/chat/completions failed: headers=$devHeaders body=$devChatResp"
        exit 1
    }
    if ($devHeaders -notmatch "X-LGW-Cost-Micros:") {
        Write-Error "missing X-LGW-Cost-Micros header in lgw-dev response: $devHeaders"
        exit 1
    }
    if (-not $devChatResp.Contains("Fake OpenAI response from dev server")) {
        Write-Error "lgw-dev response did not contain expected mock content: $devChatResp"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: POST /v1/chat/completions -> 200 OK with mock response and cost header"

    Write-Host "==> Gracefully shutting down lgw-dev..."
    Stop-Process -Id $pDev.Id -Force -ErrorAction SilentlyContinue
    $pDev.WaitForExit(3000)
    Start-Sleep -Milliseconds 500

    $shutCheck = & curl.exe -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:8787/health" 2>$null
    if ($shutCheck -eq "200") {
        Write-Error "lgw-dev port 8787 still responsive after shutdown"
        exit 1
    }
    Write-Host "  [OK] lgw-dev: clean shutdown verified (port closed)"

    Write-Host "==> All runtime verifications (Microservices pipeline + lgw-dev cluster) passed successfully!"
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
    if ($webDir -and (Test-Path $webDir)) {
        Remove-Item -Path $webDir -Recurse -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path $binDir) {
        Remove-Item -Path $binDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
