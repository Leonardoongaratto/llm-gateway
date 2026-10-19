package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
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

func waitForEndpoints(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	defer client.CloseIdleConnections()

	for _, ep := range []string{"/livez", "/readyz", "/metrics"} {
		var ok bool
		for attempt := 0; attempt < 20; attempt++ {
			time.Sleep(50 * time.Millisecond)
			resp, err := client.Get(baseURL + ep)
			if err == nil && resp.StatusCode == http.StatusOK {
				ok = true
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				break
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}
		if !ok {
			t.Fatalf("endpoint %s%s failed to return 200 within timeout", baseURL, ep)
		}
	}
}

func waitForStatus(t *testing.T, url string, expectedCode int) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	defer client.CloseIdleConnections()

	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(50 * time.Millisecond)
		resp, err := client.Get(url)
		if err == nil {
			code := resp.StatusCode
			_ = resp.Body.Close()
			if code == expectedCode {
				return
			}
		}
	}
	t.Fatalf("url %s failed to return expected code %d", url, expectedCode)
}

func TestEdge_DefaultConfigContract(t *testing.T) {
	cfg, err := config.LoadFromEnv[Config]("LGW_NONEXISTENT_")
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if cfg.HTTPPort != 8787 {
		t.Errorf("expected default HTTPPort 8787, got %d", cfg.HTTPPort)
	}
}

func TestEdge_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestEdge_RunAndEndpoints_AC1A11(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "edge",
		HTTPPort:    port,
		LogLevel:    "debug",
	}

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, url)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown exceeded 5s timeout")
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "shutting down edge service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "shutdown completed successfully") {
		t.Errorf("expected shutdown completed log in buffer, got: %s", logOutput)
	}
}

func TestEdge_Readyz503WithFailingCheck(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "edge",
		HTTPPort:    port,
		LogLevel:    "debug",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("upstream gateway unreachable")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("gateway", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func newMockServiceServer(customPath, customBody string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == customPath {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(customBody))
			return
		}
		http.NotFound(w, r)
	}))
}

func assertEndpoint(t *testing.T, client *http.Client, url string, expectedCode int, containsText string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expectedCode {
		t.Fatalf("expected %s status %d, got %d", url, expectedCode, resp.StatusCode)
	}
	if containsText != "" {
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), containsText) {
			t.Fatalf("expected %s body containing %q, got: %s", url, containsText, string(body))
		}
	}
}

func TestEdge_IntegratedRoutingAndHealth(t *testing.T) {
	gwSrv := newMockServiceServer("/v1/models", `{"data":[{"id":"gpt-4o"}]}`)
	defer gwSrv.Close()

	ctrlSrv := newMockServiceServer("/admin/keys", `{"keys":[]}`)
	defer ctrlSrv.Close()

	usgSrv := newMockServiceServer("/admin/usage", `{"records":[]}`)
	defer usgSrv.Close()

	bdgSrv := newMockServiceServer("/admin/limits", `{"limits":[]}`)
	defer bdgSrv.Close()

	tmpDir := t.TempDir()
	indexFile := filepath.Join(tmpDir, "index.html")
	if err := os.WriteFile(indexFile, []byte("<h1>Edge Dashboard</h1>"), 0600); err != nil {
		t.Fatalf("failed to write test index.html: %v", err)
	}

	port := getFreePort(t)
	cfg := Config{
		ServiceName:  "edge",
		HTTPPort:     port,
		LogLevel:     "debug",
		GatewayURL:   gwSrv.URL,
		ControlURL:   ctrlSrv.URL,
		UsageURL:     usgSrv.URL,
		BudgetURL:    bdgSrv.URL,
		WebDir:       tmpDir,
		MaxBodyBytes: 10485760,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
	}()

	edgeURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, edgeURL)

	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	defer client.CloseIdleConnections()

	assertEndpoint(t, client, edgeURL+"/health", http.StatusOK, "")
	assertEndpoint(t, client, edgeURL+"/v1/models", http.StatusOK, "gpt-4o")
	assertEndpoint(t, client, edgeURL+"/admin/keys", http.StatusOK, "")
	assertEndpoint(t, client, edgeURL+"/admin/usage", http.StatusOK, "")
	assertEndpoint(t, client, edgeURL+"/admin/limits", http.StatusOK, "")
	assertEndpoint(t, client, edgeURL+"/dashboard", http.StatusOK, "Edge Dashboard")

	client.CloseIdleConnections()
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestEdge_InvalidTargetURL_FailsRun(t *testing.T) {
	cfg := Config{
		ServiceName: "edge",
		HTTPPort:    getFreePort(t),
		GatewayURL:  "://invalid-url",
	}

	ctx := context.Background()
	err := run(ctx, cfg, WithLogWriter(io.Discard))
	if err == nil {
		t.Fatal("expected error with invalid target URL, got nil")
	}
}
