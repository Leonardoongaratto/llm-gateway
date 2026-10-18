package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/service"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	tcpAddr, ok := lis.Addr().(*net.TCPAddr)
	if !ok {
		_ = lis.Close()
		t.Fatal("failed to cast addr to net.TCPAddr")
	}
	port := tcpAddr.Port
	_ = lis.Close()
	return port
}

func TestService_Run_SuccessAndGracefulShutdown(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)

	cfg := service.Config{
		ServiceName: "testsvc",
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		LogLevel:    "debug",
	}

	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- service.Run(ctx, cfg, service.Options{
			LogWriter: &buf,
		})
	}()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	url := fmt.Sprintf("http://127.0.0.1:%d/livez", httpPort)

	var ok bool
	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(50 * time.Millisecond)
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			ok = true
			break
		}
	}
	if !ok {
		cancel()
		t.Fatalf("failed to reach service within timeout")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error from Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown exceeded timeout")
	}

	out := buf.String()
	if !strings.Contains(out, "shutting down testsvc service") {
		t.Errorf("expected shutdown log, got: %s", out)
	}
}

func TestService_Run_HTTPConflictStopsGRPC(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)

	blocker, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpPort))
	if err != nil {
		t.Fatalf("failed to occupy http port: %v", err)
	}
	defer blocker.Close()

	cfg := service.Config{
		ServiceName: "conflictsvc",
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		LogLevel:    "debug",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = service.Run(ctx, cfg, service.Options{
		Checker: health.New(),
	})
	if err == nil {
		t.Fatal("expected error due to port conflict, got nil")
	}

	// Verify grpc port was released by trying to bind to it
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", grpcPort))
	if err != nil {
		t.Fatalf("grpc port was not released: %v", err)
	}
	_ = lis.Close()
}
