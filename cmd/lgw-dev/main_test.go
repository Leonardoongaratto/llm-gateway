package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("failed to cast addr to net.TCPAddr")
	}
	port := tcpAddr.Port
	_ = lis.Close()
	return port
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

func checkHTTP(t *testing.T, client *http.Client, req *http.Request, wantStatus int, wantSubstring string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request %s %s failed: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("expected status %d for %s, got %d. Body: %s", wantStatus, req.URL, resp.StatusCode, string(body))
	}

	if wantSubstring != "" && !strings.Contains(string(body), wantSubstring) {
		t.Fatalf("expected response containing %q, got: %s", wantSubstring, string(body))
	}
}

func TestDevConfig_Defaults(t *testing.T) {
	cfg := defaultDevConfig()
	if cfg.EdgePort != 8787 {
		t.Errorf("expected EdgePort 8787, got %d", cfg.EdgePort)
	}
	if !cfg.MockProvider {
		t.Errorf("expected MockProvider to be true by default")
	}
}

func TestDevServer_LifecycleAndEndpoints_AC1F04(t *testing.T) {
	tempDir := t.TempDir()
	webDir := filepath.Join(tempDir, "web")
	if err := os.MkdirAll(webDir, 0755); err != nil {
		t.Fatalf("failed to create web dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("<h1>Lgw Dev Dashboard</h1>"), 0644); err != nil {
		t.Fatalf("failed to write index.html: %v", err)
	}

	cfg := DevConfig{
		EdgePort:         getFreePort(t),
		GatewayPort:      getFreePort(t),
		Gateway2Port:     getFreePort(t),
		ControlPort:      getFreePort(t),
		UsagePort:        getFreePort(t),
		BudgetPort:       getFreePort(t),
		BudgetGRPCPort:   getFreePort(t),
		ControlGRPCPort:  getFreePort(t),
		MockProvider:     true,
		SeedDemo:         true,
		ImportClaudeCode: false,
		WebDir:           webDir,
		AdminToken:       "dev-admin-token",
		KeyPepper:        "dev-pepper-secret-32-chars-long!",
		LogLevel:         "debug",
		TempDir:          filepath.Join(tempDir, "data"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	var logBuf bytes.Buffer
	defer func() {
		if t.Failed() {
			t.Logf("Dev Server Logs:\n%s", logBuf.String())
		}
	}()
	go func() {
		errCh <- runDevServer(ctx, cfg, WithLogWriter(&logBuf))
	}()

	edgeURL := fmt.Sprintf("http://127.0.0.1:%d", cfg.EdgePort)
	waitForHealthy(t, edgeURL+"/health")

	client := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	// 1. Health check
	reqHealth, _ := http.NewRequest(http.MethodGet, edgeURL+"/health", nil)
	checkHTTP(t, client, reqHealth, http.StatusOK, `"status":"ok"`)

	// 2. Gateway routes (/v1/models)
	reqModels, _ := http.NewRequest(http.MethodGet, edgeURL+"/v1/models", nil)
	reqModels.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	checkHTTP(t, client, reqModels, http.StatusOK, "data")

	// 3. Unary Chat Completion with default dev key
	chatPayload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Hello dev server"},
		},
	}
	chatBytes, _ := json.Marshal(chatPayload)
	reqChat, _ := http.NewRequest(http.MethodPost, edgeURL+"/v1/chat/completions", bytes.NewReader(chatBytes))
	reqChat.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	reqChat.Header.Set("Content-Type", "application/json")
	checkHTTP(t, client, reqChat, http.StatusOK, "Fake OpenAI response from dev server")

	// 4. Admin Keys route
	reqKeys, _ := http.NewRequest(http.MethodGet, edgeURL+"/admin/keys", nil)
	reqKeys.Header.Set("Authorization", "Bearer dev-admin-token")
	checkHTTP(t, client, reqKeys, http.StatusOK, "key-dev-default")

	// 5. Admin Usage route
	reqUsage, _ := http.NewRequest(http.MethodGet, edgeURL+"/admin/usage", nil)
	reqUsage.Header.Set("Authorization", "Bearer dev-admin-token")
	checkHTTP(t, client, reqUsage, http.StatusOK, "items")

	// 6. SPA Dashboard
	reqDash, _ := http.NewRequest(http.MethodGet, edgeURL+"/dashboard", nil)
	checkHTTP(t, client, reqDash, http.StatusOK, "Lgw Dev Dashboard")

	client.CloseIdleConnections()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("dev server stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("dev server failed to shutdown within timeout")
	}
}

func TestParseFlags_CustomAndInvalid(t *testing.T) {
	cfg, err := parseFlags([]string{
		"--edge-port=9090",
		"--mock-provider=false",
		"--seed-demo=true",
		"--admin-token=custom-token",
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.EdgePort != 9090 {
		t.Errorf("expected EdgePort 9090, got %d", cfg.EdgePort)
	}
	if cfg.MockProvider {
		t.Errorf("expected MockProvider to be false")
	}
	if !cfg.SeedDemo {
		t.Errorf("expected SeedDemo to be true")
	}
	if cfg.AdminToken != "custom-token" {
		t.Errorf("expected custom-token, got %s", cfg.AdminToken)
	}

	_, errInvalid := parseFlags([]string{"--unsupported-flag"})
	if errInvalid == nil {
		t.Errorf("expected error for unsupported flag, got nil")
	}
}

func TestRealMain_InvalidFlagReturnsOne(t *testing.T) {
	code := realMain([]string{"--bad-flag"})
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid flag, got %d", code)
	}
}

func TestDevServer_SingleGatewayInstance(t *testing.T) {
	tempDir := t.TempDir()
	cfg := DevConfig{
		EdgePort:        getFreePort(t),
		GatewayPort:     getFreePort(t),
		Gateway2Port:    0, // disable second gateway
		ControlPort:     getFreePort(t),
		UsagePort:       getFreePort(t),
		BudgetPort:      getFreePort(t),
		BudgetGRPCPort:  getFreePort(t),
		ControlGRPCPort: getFreePort(t),
		MockProvider:    true,
		SeedDemo:        false,
		AdminToken:      "dev-admin-token",
		KeyPepper:       "dev-pepper-secret-32-chars-long!",
		LogLevel:        "debug",
		TempDir:         filepath.Join(tempDir, "data"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- runDevServer(ctx, cfg, WithLogWriter(io.Discard))
	}()

	edgeURL := fmt.Sprintf("http://127.0.0.1:%d", cfg.EdgePort)
	waitForHealthy(t, edgeURL+"/health")

	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	reqHealth, _ := http.NewRequest(http.MethodGet, edgeURL+"/health", nil)
	checkHTTP(t, client, reqHealth, http.StatusOK, `"status":"ok"`)

	client.CloseIdleConnections()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("dev server stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("dev server failed to shutdown within timeout")
	}
}
