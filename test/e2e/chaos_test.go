package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	budgetcore "github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	budgetstore "github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	budgetsweeper "github.com/Leonardoongaratto/llm-gateway/internal/budget/sweeper"
	controlcore "github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/devserver"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	usagestore "github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func TestChaos_AC1F07_AC31_BudgetUnavailable_FailClosed(t *testing.T) {
	fakeProv := fake.New(fake.WithName("openai"), fake.WithContent("Should not be called"))
	cluster, client := startTestCluster(t, func(c *devserver.Config) {
		c.Providers = map[string]provider.Provider{
			"openai": fakeProv,
		}
	})

	// Stop budget service
	cluster.StopBudget()

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Hello budget down"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("request failed unexpectedly: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// AC-31: Fail fast within 200ms
	if elapsed > 200*time.Millisecond {
		t.Errorf("fail-closed exceeded threshold: took %v (expected <= 200ms)", elapsed)
	}

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d. Body: %s", resp.StatusCode, string(body))
	}

	var errResp map[string]map[string]any
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("failed to parse error body: %v", err)
	}

	if errResp["error"]["code"] != "budget_unavailable" {
		t.Errorf("expected error code budget_unavailable, got %v", errResp["error"]["code"])
	}

	// Verify upstream was never called
	if fakeProv.CallCount() != 0 {
		t.Errorf("expected 0 calls to provider when budget is down, got %d", fakeProv.CallCount())
	}
}

func TestChaos_AC1F08_AC32_SweeperRestoresBalance(t *testing.T) {
	cluster, client := startTestCluster(t, nil)

	ctx := context.Background()
	sweepKey := "lgw_33334444555566667777888899990000"
	keyHash, err := controlcore.HashKey(sweepKey, "dev-pepper-secret-32-chars-long!")
	if err != nil {
		t.Fatalf("failed to hash key: %v", err)
	}

	keyID := "key-sweep-chaos"
	vk := controlcore.VirtualKey{
		ID:                 keyID,
		Name:               "Chaos Sweeper Key",
		KeyHash:            keyHash,
		DailyLimitMicros:   200,
		MonthlyLimitMicros: 200,
		RPM:                1000,
		CreatedAt:          time.Now(),
	}
	if _, err := cluster.ControlStore().CreateKey(ctx, vk); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	// Set exact limits of 200 micros (1 call)
	limits := budgetcore.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   200,
		MonthlyLimitMicros: 200,
		RPM:                1000,
	}
	if err := cluster.BudgetStore().SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	if err := cluster.ReloadSnapshots(ctx); err != nil {
		t.Fatalf("failed to reload snapshots: %v", err)
	}

	now := time.Now().UTC()
	// Simulate gateway crashing after reserving all budget with an expired reservation
	expiredTime := now.Add(-100 * time.Millisecond)
	resParams := budgetstore.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 200,
		RequestID: "req-crashed-gateway",
		Now:       now,
		TTL:       50 * time.Millisecond,
	}
	_, err = cluster.BudgetStore().Reserve(ctx, resParams)
	if err != nil {
		t.Fatalf("initial reservation failed: %v", err)
	}

	// Verify that with all 200 micros reserved, immediate new call gets 402 budget_exceeded
	sendCheckCall(t, client, cluster.EdgeURL(), sweepKey, http.StatusPaymentRequired)

	// Run sweeper with time past expiration
	sw := budgetsweeper.New(cluster.BudgetStore())
	expiredCount, err := sw.SweepOnce(ctx, expiredTime.Add(1*time.Second))
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if expiredCount == 0 {
		t.Errorf("expected at least 1 expired reservation swept, got 0")
	}

	// After sweep, balance must be restored, so next call succeeds with 200 OK!
	sendCheckCall(t, client, cluster.EdgeURL(), sweepKey, http.StatusOK)
}

func sendCheckCall(t *testing.T, client *http.Client, edgeURL, key string, wantStatus int) {
	t.Helper()
	payload := map[string]any{
		"model":      "gpt-4o",
		"max_tokens": 10,
		"messages": []map[string]string{
			{"role": "user", "content": "01234567890123456789012345678901"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, edgeURL+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("check call failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != wantStatus {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status %d, got %d. Body: %s", wantStatus, resp.StatusCode, string(body))
	}
}

func sendSingleUsageRequest(client *http.Client, edgeURL string, idx int) error {
	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": fmt.Sprintf("Usage downtime msg %d", idx)},
		},
	}
	bodyBytes, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, edgeURL+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func waitForUsageCount(ctx context.Context, store *usagestore.Store, wantCount int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, total, err := store.QueryUsage(ctx, usagestore.UsageFilter{Limit: 200})
		if err == nil && total >= wantCount {
			return total, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, finalTotal, err := store.QueryUsage(ctx, usagestore.UsageFilter{Limit: 200})
	return finalTotal, err
}

func TestChaos_AC1F09_AC35_UsageDowntimeAndCatchup(t *testing.T) {
	cluster, client := startTestCluster(t, nil)

	ctx := context.Background()

	// 1. Stop the usage service
	cluster.StopUsage()

	// 2. Send 100 requests through edge while usage is down
	targetCount := 100
	for i := 0; i < targetCount; i++ {
		if err := sendSingleUsageRequest(client, cluster.EdgeURL(), i); err != nil {
			t.Fatalf("request %d failed while usage was down: %v", i, err)
		}
	}

	// Allow gateway publisher goroutines to flush into NATS
	time.Sleep(300 * time.Millisecond)

	// 3. Verify that with usage service stopped, no records are written yet
	_, countWhileDown, err := cluster.UsageStore().QueryUsage(ctx, usagestore.UsageFilter{Limit: 200})
	if err != nil {
		t.Fatalf("failed to query usage while down: %v", err)
	}
	if countWhileDown != 0 {
		t.Errorf("expected 0 usage records while service stopped, got %d", countWhileDown)
	}

	// 4. Start usage service back up
	cluster.StartUsage(ctx)

	// 5. Wait for consumer to catch up and consume all 100 queued events from JetStream
	finalCount, err := waitForUsageCount(ctx, cluster.UsageStore(), targetCount, 15*time.Second)
	if err != nil {
		t.Fatalf("failed querying usage after restart: %v", err)
	}
	if finalCount != targetCount {
		t.Fatalf("AC-35 failure: expected all %d events consumed without loss, got %d", targetCount, finalCount)
	}
}
