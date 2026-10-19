package devserver

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.EdgePort != 8787 {
		t.Errorf("expected EdgePort 8787, got %d", cfg.EdgePort)
	}
	if !cfg.MockProvider {
		t.Errorf("expected MockProvider to be true by default")
	}
}

func TestCluster_LifecycleAndEndpoints_AC1F04(t *testing.T) {
	tempDir := t.TempDir()
	webDir := filepath.Join(tempDir, "web")
	if err := os.MkdirAll(webDir, 0755); err != nil {
		t.Fatalf("failed to create web dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("<h1>Lgw Dev Dashboard</h1>"), 0644); err != nil {
		t.Fatalf("failed to write index.html: %v", err)
	}

	cfg := Config{
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

	var logBuf bytes.Buffer
	cluster, err := NewCluster(cfg, WithLogWriter(&logBuf))
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	edgeURL := cluster.EdgeURL()
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

	// 7. Verify Getters
	if cluster.BudgetStore() == nil {
		t.Error("expected BudgetStore to not be nil")
	}
	if cluster.ControlStore() == nil {
		t.Error("expected ControlStore to not be nil")
	}
	if cluster.UsageStore() == nil {
		t.Error("expected UsageStore to not be nil")
	}
	if cluster.NATSConn() == nil {
		t.Error("expected NATSConn to not be nil")
	}
	if cluster.JS() == nil {
		t.Error("expected JS to not be nil")
	}
	if cluster.Gateway1URL() == "" || cluster.Gateway2URL() == "" {
		t.Errorf("expected gateway URLs, got gw1=%s, gw2=%s", cluster.Gateway1URL(), cluster.Gateway2URL())
	}
	if cluster.ControlURL() == "" || cluster.UsageURL() == "" || cluster.BudgetURL() == "" {
		t.Errorf("expected service URLs, got ctrl=%s, usg=%s, bdg=%s", cluster.ControlURL(), cluster.UsageURL(), cluster.BudgetURL())
	}

	cluster.PrintBanner()
}

func TestCluster_SingleGateway(t *testing.T) {
	tempDir := t.TempDir()
	cfg := Config{
		EdgePort:        getFreePort(t),
		GatewayPort:     getFreePort(t),
		Gateway2Port:    0,
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

	cluster, err := NewCluster(cfg, WithLogWriter(io.Discard))
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	if cluster.Gateway2URL() != "" {
		t.Errorf("expected empty Gateway2URL, got %s", cluster.Gateway2URL())
	}

	waitForHealthy(t, cluster.EdgeURL()+"/health")
}

func TestCluster_StopAndRestartServices(t *testing.T) {
	tempDir := t.TempDir()
	cfg := Config{
		EdgePort:        getFreePort(t),
		GatewayPort:     getFreePort(t),
		Gateway2Port:    0,
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

	cluster, err := NewCluster(cfg, WithLogWriter(io.Discard))
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	waitForHealthy(t, cluster.EdgeURL()+"/health")

	// Test Stop/Start Budget
	cluster.StopBudget()
	cluster.StartBudget(ctx)

	// Test Stop/Start Usage
	cluster.StopUsage()
	cluster.StartUsage(ctx)
}
