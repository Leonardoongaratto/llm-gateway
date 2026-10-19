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

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"google.golang.org/grpc/health/grpc_health_v1"
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

func TestBudget_DefaultConfigContract(t *testing.T) {
	cfg, err := config.LoadFromEnv[Config]("LGW_NONEXISTENT_")
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if cfg.HTTPPort != 8792 {
		t.Errorf("expected default HTTPPort 8792, got %d", cfg.HTTPPort)
	}
	if cfg.GRPCPort != 9101 {
		t.Errorf("expected default GRPCPort 9101, got %d", cfg.GRPCPort)
	}
}

func TestBudget_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestBudget_RunAndEndpoints_AC1A11(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "budget",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
	}

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	seeder := func(s *store.Store) error {
		return s.SetKeyLimits(context.Background(), core.KeyLimits{
			KeyID:            "k-1",
			DailyLimitMicros: 5000,
		})
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf), WithStoreSeeder(seeder))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitForEndpoints(t, url)

	grpcConn, err := grpcx.Dial(context.Background(), fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		t.Fatalf("failed to dial budget grpc: %v", err)
	}

	healthClient := grpc_health_v1.NewHealthClient(grpcConn)
	healthResp, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || healthResp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		_ = grpcConn.Close()
		t.Fatalf("grpc health check failed: %v, resp: %v", err, healthResp)
	}

	budgetClient := budgetv1.NewBudgetServiceClient(grpcConn)
	remResp, err := budgetClient.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "k-1"})
	if err != nil {
		_ = grpcConn.Close()
		t.Fatalf("expected Remaining success on configured key, got error: %v", err)
	}
	if remResp == nil {
		_ = grpcConn.Close()
		t.Fatal("expected non-nil remaining response")
	}

	_ = grpcConn.Close()
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
	if !strings.Contains(logOutput, "shutting down budget service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "shutdown completed successfully") {
		t.Errorf("expected shutdown completed log in buffer, got: %s", logOutput)
	}
}

func TestBudget_Readyz503WithFailingCheck(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "budget",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("budget database offline")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("budget_db", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", httpPort)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestBudget_RunErrors(t *testing.T) {
	t.Run("invalid database driver", func(t *testing.T) {
		cfg := Config{
			DatabaseDriver: "unsupported-driver",
		}
		err := run(context.Background(), cfg, WithLogWriter(io.Discard))
		if err == nil || !strings.Contains(err.Error(), "unsupported database driver") {
			t.Fatalf("expected unsupported driver error, got: %v", err)
		}
	})

	t.Run("failing seeder", func(t *testing.T) {
		cfg := Config{
			DatabaseDriver: "sqlite",
		}
		failingSeeder := func(_ *store.Store) error {
			return errors.New("seeder failure")
		}
		err := run(context.Background(), cfg, WithLogWriter(io.Discard), WithStoreSeeder(failingSeeder))
		if err == nil || !strings.Contains(err.Error(), "seeder failure") {
			t.Fatalf("expected seeder failure, got: %v", err)
		}
	})

	t.Run("nats connection failure handled gracefully", func(t *testing.T) {
		cfg := Config{
			ServiceName:    "budget",
			HTTPPort:       getFreePort(t),
			GRPCPort:       getFreePort(t),
			DatabaseDriver: "sqlite",
			NATSURL:        "nats://127.0.0.1:1",
		}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
		}()
		time.Sleep(100 * time.Millisecond)
		cancel()
		if err := <-errCh; err != nil {
			t.Fatalf("expected clean run even with nats error, got: %v", err)
		}
	})
}

func TestBudget_RealMain_ServiceErrorReturnsOne(t *testing.T) {
	t.Setenv("LGW_DATABASE_DRIVER", "invalid-driver")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for failing service run, got %d", code)
	}
}

func TestBudget_RunWithNATS(t *testing.T) {
	_, natsURL := natsx.StartEmbeddedServer(t)
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "budget",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		NATSURL:        natsURL,
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitForEndpoints(t, url)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestBudget_AdminLimitsHTTP(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "budget",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitForEndpoints(t, url)

	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()

	payload := `{"key_id":"test-key","daily_limit_micros":1000000,"monthly_limit_micros":5000000,"rpm":60}`
	resp, err := client.Post(url+"/admin/limits", "application/json", strings.NewReader(payload))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on valid limits, got err: %v, resp: %v", err, resp)
	}
	_ = resp.Body.Close()

	resp, err = client.Post(url+"/admin/limits", "application/json", strings.NewReader(`{"key_id":""}`))
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty key_id, got err: %v, resp: %v", err, resp)
	}
	_ = resp.Body.Close()

	resp, err = client.Post(url+"/admin/limits", "application/json", strings.NewReader(`invalid-json`))
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid json, got err: %v, resp: %v", err, resp)
	}
	_ = resp.Body.Close()

	client.CloseIdleConnections()
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}
