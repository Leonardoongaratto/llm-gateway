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

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
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
}

func TestControl_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain()
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestControl_RunAndEndpoints_AC1A11(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName: "control",
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		LogLevel:    "debug",
	}

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	waitForEndpoints(t, url)

	grpcConn, err := grpcx.Dial(context.Background(), fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		t.Fatalf("failed to dial control grpc: %v", err)
	}

	healthClient := grpc_health_v1.NewHealthClient(grpcConn)
	healthResp, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || healthResp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		_ = grpcConn.Close()
		t.Fatalf("grpc health check failed: %v, resp: %v", err, healthResp)
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
	if !strings.Contains(logOutput, "shutting down control service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "shutdown completed successfully") {
		t.Errorf("expected shutdown completed log in buffer, got: %s", logOutput)
	}
}

func TestControl_Readyz503WithFailingCheck(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName: "control",
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		LogLevel:    "debug",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("database connection lost")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("database", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", httpPort)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}
