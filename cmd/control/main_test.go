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

	controlv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/store"
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

func TestControl_DefaultConfigContract(t *testing.T) {
	cfg, err := config.LoadFromEnv[Config]("LGW_NONEXISTENT_")
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if cfg.HTTPPort != 8790 {
		t.Errorf("expected default HTTPPort 8790, got %d", cfg.HTTPPort)
	}
	if cfg.GRPCPort != 9102 {
		t.Errorf("expected default GRPCPort 9102, got %d", cfg.GRPCPort)
	}
	if cfg.ServiceName != "control" {
		t.Errorf("expected default ServiceName control, got %s", cfg.ServiceName)
	}
}

func TestControl_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestControl_Readyz503WithFailingCheck(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "control",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("control database offline")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("control_db", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", httpPort)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestControl_RunErrors(t *testing.T) {
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
			ServiceName:    "control",
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

func TestControl_RealMain_ServiceErrorReturnsOne(t *testing.T) {
	t.Setenv("LGW_DATABASE_DRIVER", "invalid-driver")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for failing service run, got %d", code)
	}
}

func verifyGRPCControlService(ctx context.Context, t *testing.T, grpcPort int) {
	t.Helper()

	grpcConn, err := grpcx.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		t.Fatalf("failed to dial control grpc: %v", err)
	}
	defer grpcConn.Close()

	healthClient := grpc_health_v1.NewHealthClient(grpcConn)
	healthResp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil || healthResp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("grpc health check failed: %v, resp: %v", err, healthResp)
	}

	controlClient := controlv1.NewControlServiceClient(grpcConn)
	snapResp, err := controlClient.GetSnapshot(ctx, &controlv1.GetSnapshotRequest{})
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if snapResp.Version < 1 {
		t.Errorf("expected version >= 1, got %d", snapResp.Version)
	}
	if len(snapResp.Keys) != 1 || snapResp.Keys[0].Id != "seeded-key" {
		t.Errorf("unexpected keys in snapshot: %+v", snapResp.Keys)
	}
}

func verifyHTTPAdminRoutes(t *testing.T, httpPort int) {
	t.Helper()

	client := &http.Client{Timeout: 1 * time.Second}
	defer client.CloseIdleConnections()

	url := fmt.Sprintf("http://127.0.0.1:%d/admin/keys", httpPort)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer dev-admin-token")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("failed to call /admin/keys: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /admin/keys, got %d", resp.StatusCode)
	}
}

func TestControl_RunAndEndpoints_AC1C06_AC1C08(t *testing.T) {
	_, natsURL := natsx.StartEmbeddedServer(t)
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName:    "control",
		HTTPPort:       httpPort,
		GRPCPort:       grpcPort,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		NATSURL:        natsURL,
		AdminToken:     "dev-admin-token",
		KeyPepper:      "dev-pepper",
	}

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	seeder := func(s *store.Store) error {
		_, err := s.CreateKey(context.Background(), core.VirtualKey{
			ID:      "seeded-key",
			Name:    "Seeded Key",
			KeyHash: "seeded-hash",
		})
		return err
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf), WithStoreSeeder(seeder))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitForEndpoints(t, url)

	verifyGRPCControlService(ctx, t, grpcPort)
	verifyHTTPAdminRoutes(t, httpPort)

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
	if !strings.Contains(logOutput, "shutting down control service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
}
