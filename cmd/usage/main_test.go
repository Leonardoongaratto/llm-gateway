package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
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

func TestUsage_RunAndEndpoints_AC1A11(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName: "usage",
		HTTPPort:    port,
		LogLevel:    "debug",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg)
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
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
