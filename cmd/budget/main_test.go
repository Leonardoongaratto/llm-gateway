package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return port
}

func TestBudget_RunAndEndpoints_AC1A11(t *testing.T) {
	httpPort := getFreePort(t)
	grpcPort := getFreePort(t)
	cfg := Config{
		ServiceName: "budget",
		HTTPPort:    httpPort,
		GRPCPort:    grpcPort,
		LogLevel:    "debug",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg)
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	client := &http.Client{Timeout: 1 * time.Second}

	var livezOk, readyzOk, metricsOk bool
	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(50 * time.Millisecond)

		if !livezOk {
			resp, err := client.Get(url + "/livez")
			if err == nil && resp.StatusCode == http.StatusOK {
				livezOk = true
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
		if !readyzOk {
			resp, err := client.Get(url + "/readyz")
			if err == nil && resp.StatusCode == http.StatusOK {
				readyzOk = true
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}
		if !metricsOk {
			resp, err := client.Get(url + "/metrics")
			if err == nil && resp.StatusCode == http.StatusOK {
				metricsOk = true
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}

		if livezOk && readyzOk && metricsOk {
			break
		}
	}

	client.CloseIdleConnections()

	if !livezOk || !readyzOk || !metricsOk {
		t.Fatalf("endpoints check failed: livez=%v, readyz=%v, metrics=%v", livezOk, readyzOk, metricsOk)
	}

	// Verify gRPC Health
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

	// Verify BudgetService.Remaining
	budgetClient := budgetv1.NewBudgetServiceClient(grpcConn)
	rem, err := budgetClient.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "k-1"})
	if err != nil || rem.DailyMicros != 1000000 {
		_ = grpcConn.Close()
		t.Fatalf("remaining check failed: %v, rem: %v", err, rem)
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
}
