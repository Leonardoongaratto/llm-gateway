package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
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

func TestGateway_DefaultConfigContract(t *testing.T) {
	t.Setenv("LGW_NONEXISTENT_KEY_PEPPER", "test-pepper-1234567890")
	cfg, err := config.LoadFromEnv[Config]("LGW_NONEXISTENT_")
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if cfg.HTTPPort != 8788 {
		t.Errorf("expected default HTTPPort 8788, got %d", cfg.HTTPPort)
	}
}

func TestGateway_RealMain_MissingRequiredPepperReturnsOne(t *testing.T) {
	t.Setenv("LGW_KEY_PEPPER", "")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for missing key pepper, got %d", code)
	}
}

func TestGateway_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_KEY_PEPPER", "test-pepper-1234567890")
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestGateway_RunAndEndpoints_AC1A11(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "gateway",
		HTTPPort:    port,
		LogLevel:    "debug",
		KeyPepper:   "test-pepper-1234567890",
	}

	authMgr, _ := auth.NewManager("test-pepper-1234567890", nil)
	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		Pricing: map[string]*auth.PricingInfo{
			"model-test": {Model: "model-test"},
		},
	})
	svc := core.NewService(core.ServiceConfig{
		AuthManager: authMgr,
	})

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf), WithCoreService(svc, authMgr))
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
	if !strings.Contains(logOutput, "shutting down gateway service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "shutdown completed successfully") {
		t.Errorf("expected shutdown completed log in buffer, got: %s", logOutput)
	}
}

func TestGateway_Readyz503WithoutBudgetAndSnapshot(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "gateway",
		HTTPPort:    port,
		LogLevel:    "debug",
		KeyPepper:   "test-pepper-1234567890",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
	}()

	urlLivez := fmt.Sprintf("http://127.0.0.1:%d/livez", port)
	waitForStatus(t, urlLivez, http.StatusOK)

	urlReadyz := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	waitForStatus(t, urlReadyz, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestGateway_Readyz503WithFailingCheck(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "gateway",
		HTTPPort:    port,
		LogLevel:    "debug",
		KeyPepper:   "test-pepper-1234567890",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("upstream provider failure")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("upstream", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestGateway_WithCustomCoreService(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "gateway",
		HTTPPort:    port,
		LogLevel:    "debug",
		KeyPepper:   "test-pepper-1234567890",
	}

	authMgr, _ := auth.NewManager("test-pepper", nil)
	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		Pricing: map[string]*auth.PricingInfo{
			"model-test": {Model: "model-test"},
		},
	})
	svc := core.NewService(core.ServiceConfig{
		AuthManager: authMgr,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCoreService(svc, authMgr))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/livez", port)
	waitForStatus(t, url, http.StatusOK)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestGateway_SetupRouter(t *testing.T) {
	cfg := Config{
		AnthropicAPIKey: "ant-key",
		OpenAIAPIKey:    "oai-key",
		GeminiAPIKey:    "gem-key",
	}
	r := setupRouter(cfg, nil)
	if r == nil {
		t.Fatal("expected non-nil router")
	}
}
