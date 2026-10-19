package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	budgetcore "github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	controlcore "github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/devserver"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	defer func() { _ = lis.Close() }()
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("failed to cast addr to net.TCPAddr")
	}
	return tcpAddr.Port
}

func waitForHealthy(t *testing.T, healthURL string) {
	t.Helper()
	client := &http.Client{
		Timeout:   1 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(healthURL)
		if err == nil {
			status := resp.StatusCode
			_ = resp.Body.Close()
			if status == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to return 200", healthURL)
}

func startTestCluster(t *testing.T, cfgMod func(*devserver.Config)) (*devserver.Cluster, *http.Client) {
	t.Helper()

	cfg := devserver.Config{
		EdgePort:         getFreePort(t),
		GatewayPort:      getFreePort(t),
		Gateway2Port:     getFreePort(t),
		ControlPort:      getFreePort(t),
		UsagePort:        getFreePort(t),
		BudgetPort:       getFreePort(t),
		BudgetGRPCPort:   getFreePort(t),
		ControlGRPCPort:  getFreePort(t),
		MockProvider:     true,
		SeedDemo:         false,
		ImportClaudeCode: false,
		AdminToken:       "dev-admin-token",
		KeyPepper:        "dev-pepper-secret-32-chars-long!",
		LogLevel:         "debug",
	}

	if cfgMod != nil {
		cfgMod(&cfg)
	}

	cluster, err := devserver.NewCluster(cfg, devserver.WithLogWriter(io.Discard))
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		cluster.Close()
	})

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	waitForHealthy(t, cluster.EdgeURL()+"/health")

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	t.Cleanup(client.CloseIdleConnections)

	return cluster, client
}

func TestE2E_HappyPath_Unary(t *testing.T) {
	cluster, client := startTestCluster(t, nil)

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Hello E2E Unary"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unary chat completion request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	reqID := resp.Header.Get("X-Request-Id")
	if reqID == "" {
		t.Error("expected X-Request-Id header in response, got empty")
	}

	var resMap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&resMap); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	choices, ok := resMap["choices"].([]any)
	if !ok || len(choices) == 0 {
		t.Fatalf("expected non-empty choices array, got %v", resMap["choices"])
	}
	choice0, ok := choices[0].(map[string]any)
	if !ok {
		t.Fatalf("expected choice 0 map, got %T", choices[0])
	}
	msg, ok := choice0["message"].(map[string]any)
	if !ok {
		t.Fatalf("expected message map, got %T", choice0["message"])
	}
	content, _ := msg["content"].(string)

	if !strings.Contains(content, "Fake OpenAI response from dev server") {
		t.Errorf("unexpected content in response: %s", content)
	}
}

func TestE2E_HappyPath_StreamingSSE(t *testing.T) {
	cluster, client := startTestCluster(t, nil)

	payload := map[string]any{
		"model":  "gpt-4o",
		"stream": true,
		"messages": []map[string]string{
			{"role": "user", "content": "Hello E2E Stream"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("streaming request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("expected Content-Type text/event-stream, got %s", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	var eventsReceived []string
	var foundDone bool

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			dataPayload := strings.TrimPrefix(line, "data: ")
			if dataPayload == "[DONE]" {
				foundDone = true
			} else {
				eventsReceived = append(eventsReceived, dataPayload)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("error reading SSE stream: %v", err)
	}

	if len(eventsReceived) == 0 {
		t.Error("expected at least one SSE data chunk, got 0")
	}
	if !foundDone {
		t.Error("expected [DONE] event in SSE stream")
	}
}

func TestE2E_BudgetExceeded_HTTP402(t *testing.T) {
	cluster, client := startTestCluster(t, nil)

	ctx := context.Background()
	brokeKey := "lgw_99999999999999999999999999999999"
	keyHash, err := controlcore.HashKey(brokeKey, "dev-pepper-secret-32-chars-long!")
	if err != nil {
		t.Fatalf("failed to hash key: %v", err)
	}

	vk := controlcore.VirtualKey{
		ID:        "key-broke-e2e",
		Name:      "Broke Test Key",
		KeyHash:   keyHash,
		CreatedAt: time.Now(),
	}
	if _, err := cluster.ControlStore().CreateKey(ctx, vk); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	limits := budgetcore.KeyLimits{
		KeyID:              "key-broke-e2e",
		DailyLimitMicros:   1, // 1 micro, far below minimum request cost
		MonthlyLimitMicros: 1,
		RPM:                1000,
	}
	if err := cluster.BudgetStore().SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	if err := cluster.ReloadSnapshots(ctx); err != nil {
		t.Fatalf("failed to reload snapshots: %v", err)
	}

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "I have no money"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+brokeKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPaymentRequired {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 402 Payment Required, got %d. Body: %s", resp.StatusCode, string(body))
	}

	var errResp map[string]map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error body: %v", err)
	}

	errObj := errResp["error"]
	if errObj["code"] != "budget_exceeded" {
		t.Errorf("expected error code budget_exceeded, got %v", errObj["code"])
	}
}

func TestE2E_ProviderFallback(t *testing.T) {
	primaryFail := fake.New(
		fake.WithName("failing-primary"),
		fake.WithError(&provider.UpstreamError{
			StatusCode: http.StatusServiceUnavailable,
			Message:    "upstream temporary outage",
			Retryable:  true,
		}),
	)

	cluster, client := startTestCluster(t, func(c *devserver.Config) {
		c.Providers = map[string]provider.Provider{
			"failing-primary": primaryFail,
		}
	})

	ctx := context.Background()

	models := []controlcore.LogicalModel{
		{
			Name:  "resilient-model",
			Chain: []string{"failing-primary/fail-target", "openai/gpt-4o"},
		},
	}
	if err := cluster.ControlStore().UpsertLogicalModels(ctx, models); err != nil {
		t.Fatalf("failed to upsert logical model: %v", err)
	}

	prices := []controlcore.Pricing{
		{
			Model:         "resilient-model",
			Provider:      "failing-primary",
			InputPerMTok:  5000000,
			OutputPerMTok: 15000000,
		},
		{
			Model:         "fail-target",
			Provider:      "failing-primary",
			InputPerMTok:  5000000,
			OutputPerMTok: 15000000,
		},
	}
	if err := cluster.ControlStore().UpsertPricing(ctx, prices); err != nil {
		t.Fatalf("failed to upsert pricing: %v", err)
	}

	if err := cluster.ReloadSnapshots(ctx); err != nil {
		t.Fatalf("failed to reload snapshots: %v", err)
	}

	payload := map[string]any{
		"model": "resilient-model",
		"messages": []map[string]string{
			{"role": "user", "content": "Test automatic fallback"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, _ := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK via fallback, got %d: %s", resp.StatusCode, string(body))
	}

	var resMap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&resMap); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	choices, ok := resMap["choices"].([]any)
	if !ok || len(choices) == 0 {
		t.Fatalf("expected non-empty choices, got %v", resMap["choices"])
	}
	choice0, ok := choices[0].(map[string]any)
	if !ok {
		t.Fatalf("expected choice 0 map, got %T", choices[0])
	}
	msg, ok := choice0["message"].(map[string]any)
	if !ok {
		t.Fatalf("expected message map, got %T", choice0["message"])
	}
	content, _ := msg["content"].(string)

	if !strings.Contains(content, "Fake OpenAI response from dev server") {
		t.Errorf("expected response from fallback openai provider, got: %s", content)
	}

	if primaryFail.CallCount() == 0 {
		t.Error("expected failing-primary to have been called before fallback")
	}
}

func sendConcurrentRequest(edgeURL, budgetKey string, idx int) int {
	httpClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer httpClient.CloseIdleConnections()

	payload := map[string]any{
		"model":      "gpt-4o",
		"max_tokens": 10,
		"messages": []map[string]string{
			{"role": "user", "content": fmt.Sprintf("012345678901234567890123456789%02d", idx)},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, edgeURL+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return 0
	}
	req.Header.Set("Authorization", "Bearer "+budgetKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestE2E_AC1F05_AC05_ConcurrentBudgetEnforcement(t *testing.T) {
	cluster, _ := startTestCluster(t, nil)

	ctx := context.Background()
	budgetKey := "lgw_11112222333344445555666677778888"
	keyHash, err := controlcore.HashKey(budgetKey, "dev-pepper-secret-32-chars-long!")
	if err != nil {
		t.Fatalf("failed to hash key: %v", err)
	}

	keyID := "key-budget-ac05"
	vk := controlcore.VirtualKey{
		ID:        keyID,
		Name:      "AC-05 Concurrent Budget Key",
		KeyHash:   keyHash,
		CreatedAt: time.Now(),
	}
	if _, err := cluster.ControlStore().CreateKey(ctx, vk); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	// Cost per call:
	// Prompt: "user" (4) + 32 chars + 4 = 40 chars => 10 prompt tokens * 5 micros = 50 micros
	// Completion (max_tokens=10): 10 completion tokens * 15 micros = 150 micros
	// Total estimated max cost = 200 micros. Fake provider also returns 10+10 tokens = 200 micros.
	// Budget for exactly 10 calls: 10 * 200 = 2000 micros.
	costPerCall := int64(200)
	limits := budgetcore.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   money.Micros(10 * costPerCall),
		MonthlyLimitMicros: money.Micros(10 * costPerCall),
		RPM:                1000,
	}
	if err := cluster.BudgetStore().SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	if err := cluster.ReloadSnapshots(ctx); err != nil {
		t.Fatalf("failed to reload snapshots: %v", err)
	}

	totalReqs := 50
	var successCount atomic.Int64
	var budgetExceededCount atomic.Int64
	var otherCount atomic.Int64

	var wg sync.WaitGroup
	wg.Add(totalReqs)

	for i := 0; i < totalReqs; i++ {
		go func(idx int) {
			defer wg.Done()
			status := sendConcurrentRequest(cluster.EdgeURL(), budgetKey, idx)
			switch status {
			case http.StatusOK:
				successCount.Add(1)
			case http.StatusPaymentRequired:
				budgetExceededCount.Add(1)
			default:
				otherCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	succ := successCount.Load()
	exceeded := budgetExceededCount.Load()
	other := otherCount.Load()

	t.Logf("AC-05 Results: Total=%d, Success(200)=%d, BudgetExceeded(402)=%d, Other=%d",
		totalReqs, succ, exceeded, other)

	if succ > 10 {
		t.Fatalf("AC-05 violation: expected at most 10 successful requests, but got %d", succ)
	}
	if other != 0 {
		t.Errorf("expected 0 unexpected errors, got %d", other)
	}
	if succ+exceeded != int64(totalReqs) {
		t.Errorf("expected all requests to either succeed or exceed budget, got succ=%d, exceeded=%d", succ, exceeded)
	}
}

func sendRateLimitRequest(client *http.Client, baseURL, rateKey string) (int, string, error) {
	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Rate limit check"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+rateKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	retryAfter := resp.Header.Get("Retry-After")
	if resp.StatusCode == http.StatusTooManyRequests {
		var errResp map[string]map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		if code, ok := errResp["error"]["code"].(string); ok && code != "rate_limited" {
			return resp.StatusCode, retryAfter, fmt.Errorf("unexpected code: %s", code)
		}
	}
	return resp.StatusCode, retryAfter, nil
}

func TestE2E_AC1F06_AC13_SharedRateLimiting(t *testing.T) {
	cluster, _ := startTestCluster(t, nil)

	ctx := context.Background()
	rateKey := "lgw_55556666777788889999000011112222"
	keyHash, err := controlcore.HashKey(rateKey, "dev-pepper-secret-32-chars-long!")
	if err != nil {
		t.Fatalf("failed to hash key: %v", err)
	}

	keyID := "key-rate-ac13"
	vk := controlcore.VirtualKey{
		ID:        keyID,
		Name:      "AC-13 Rate Limited Key",
		KeyHash:   keyHash,
		CreatedAt: time.Now(),
	}
	if _, err := cluster.ControlStore().CreateKey(ctx, vk); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	// 5 requests per minute allowed
	limits := budgetcore.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   1000000000,
		MonthlyLimitMicros: 5000000000,
		RPM:                5,
	}
	if err := cluster.BudgetStore().SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	if err := cluster.ReloadSnapshots(ctx); err != nil {
		t.Fatalf("failed to reload snapshots: %v", err)
	}

	gw1URL := cluster.Gateway1URL()
	gw2URL := cluster.Gateway2URL()

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	sendSuccessfulRequests(t, client, gw1URL, rateKey, 3)
	sendSuccessfulRequests(t, client, gw2URL, rateKey, 2)

	assertRateLimited(t, client, gw1URL, rateKey)
	assertRateLimited(t, client, gw2URL, rateKey)
}

func sendSuccessfulRequests(t *testing.T, client *http.Client, url, key string, count int) {
	t.Helper()
	for i := 1; i <= count; i++ {
		st, _, err := sendRateLimitRequest(client, url, key)
		if err != nil || st != http.StatusOK {
			t.Fatalf("call %d to %s failed: status=%d, err=%v", i, url, st, err)
		}
	}
}

func assertRateLimited(t *testing.T, client *http.Client, url, key string) {
	t.Helper()
	st, retryAfter, err := sendRateLimitRequest(client, url, key)
	if err != nil || st != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for %s, got %d (err=%v)", url, st, err)
	}
	if retryAfter == "" {
		t.Errorf("expected Retry-After header on 429 response, got empty")
	}
}
