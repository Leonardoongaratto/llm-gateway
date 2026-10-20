package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/devserver"
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
	client := &http.Client{Timeout: 1 * time.Second}
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
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to return 200", healthURL)
}

func calculatePercentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	idx := int(float64(len(durations)) * (p / 100.0))
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	return durations[idx]
}

func setupBenchmarkCluster(t *testing.T) (*devserver.Cluster, *http.Client, string) {
	t.Helper()

	cfg := devserver.Config{
		EdgePort:         getFreePort(t),
		GatewayPort:      getFreePort(t),
		Gateway2Port:     0,
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
		LogLevel:         "error",
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

	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
	t.Cleanup(client.CloseIdleConnections)

	endpoint := cluster.EdgeURL() + "/v1/chat/completions"
	return cluster, client, endpoint
}

func sendBenchmarkRequest(client *http.Client, endpoint string, body []byte) (time.Duration, error) {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		return elapsed, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return elapsed, fmt.Errorf("expected 200, got %d: %s", resp.StatusCode, string(respBody))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return elapsed, nil
}

func TestBenchmark_Overhead(t *testing.T) {
	_, client, endpoint := setupBenchmarkCluster(t)

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Benchmark ping"},
		},
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	// Warm-up phase (50 requests)
	for i := 0; i < 50; i++ {
		if _, err := sendBenchmarkRequest(client, endpoint, bodyBytes); err != nil {
			t.Fatalf("warm-up request failed at %d: %v", i, err)
		}
	}

	// Benchmark phase (1000 sequential requests)
	const totalRequests = 1000
	durations := make([]time.Duration, totalRequests)

	for i := 0; i < totalRequests; i++ {
		elapsed, err := sendBenchmarkRequest(client, endpoint, bodyBytes)
		if err != nil {
			t.Fatalf("benchmark request %d failed: %v", i, err)
		}
		durations[i] = elapsed
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	p50 := calculatePercentile(durations, 50.0)
	p90 := calculatePercentile(durations, 90.0)
	p99 := calculatePercentile(durations, 99.0)
	p999 := calculatePercentile(durations, 99.9)

	targetP50 := 5 * time.Millisecond
	targetP99 := 20 * time.Millisecond
	if raceDetectorEnabled {
		targetP50 = 15 * time.Millisecond
		targetP99 = 40 * time.Millisecond
		t.Logf("Running under race detector: adjusted targets (p50 <= %v, p99 <= %v)", targetP50, targetP99)
	}

	t.Logf("Overhead Benchmark Results (N=%d):", totalRequests)
	t.Logf("  p50 : %v (target <= %v)", p50, targetP50)
	t.Logf("  p90 : %v", p90)
	t.Logf("  p99 : %v (target <= %v)", p99, targetP99)
	t.Logf("  p999: %v", p999)

	// AC-1F-11 / AC-39 assertions
	if p50 > targetP50 {
		t.Errorf("expected p50 <= %v, got %v", targetP50, p50)
	}
	if p99 > targetP99 {
		t.Errorf("expected p99 <= %v, got %v", targetP99, p99)
	}
}
