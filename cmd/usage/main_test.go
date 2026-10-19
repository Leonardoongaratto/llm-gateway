package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
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
		for attempt := 0; attempt < 25; attempt++ {
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

	for attempt := 0; attempt < 25; attempt++ {
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

func TestUsage_DefaultConfigContract(t *testing.T) {
	t.Setenv("LGW_TEST_ADMIN_TOKEN", "adm-secret")

	cfg, err := config.LoadFromEnv[Config]("LGW_TEST_")
	if err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if cfg.HTTPPort != 8791 {
		t.Errorf("expected default HTTPPort 8791, got %d", cfg.HTTPPort)
	}
	if cfg.ServiceName != "usage" {
		t.Errorf("expected default ServiceName usage, got %s", cfg.ServiceName)
	}
}

func TestUsage_RealMain_MissingAdminTokenReturnsOne(t *testing.T) {
	t.Setenv("LGW_ADMIN_TOKEN", "")
	code := realMain(nil)
	if code != 1 {
		t.Errorf("expected exit code 1 for missing admin token, got %d", code)
	}
}

func TestUsage_RealMain_InvalidEnvReturnsOne(t *testing.T) {
	t.Setenv("LGW_ADMIN_TOKEN", "adm-secret")
	t.Setenv("LGW_HTTP_PORT", "not-an-int")
	code := realMain(nil)
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid env, got %d", code)
	}
}

func TestUsage_RealMain_RunErrorReturnsOne(t *testing.T) {
	t.Setenv("LGW_ADMIN_TOKEN", "adm-secret")
	t.Setenv("LGW_HTTP_PORT", "8791")
	t.Setenv("LGW_DATABASE_DRIVER", "invalid-unsupported-driver")
	code := realMain(nil)
	if code != 1 {
		t.Errorf("expected exit code 1 when run fails, got %d", code)
	}
}

func TestUsage_RealMain_SeedDemoFlag(t *testing.T) {
	tempOut := filepath.Join(t.TempDir(), "summary.test.json")
	t.Setenv("LGW_SEED_OUTPUT_PATH", tempOut)
	t.Setenv("LGW_DATABASE_DSN", ":memory:")

	code := realMain([]string{"--seed-demo"})
	if code != 0 {
		t.Fatalf("expected realMain(--seed-demo) to return 0, got %d", code)
	}

	data, err := os.ReadFile(tempOut)
	if err != nil {
		t.Fatalf("failed to read generated summary: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "Claude Code Pro") {
		t.Errorf("expected subscription in summary.test.json, got: %s", content)
	}
	if !strings.Contains(content, `"aproveitamento": 4.2`) {
		t.Errorf("expected aproveitamento 4.2 in summary.test.json, got: %s", content)
	}

	targetPath := "docs/summary.example.json"
	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		targetPath = "../../docs/summary.example.json"
	}
	golden, err := os.ReadFile(targetPath)
	if err == nil {
		goldenNorm := bytes.ReplaceAll(golden, []byte("\r\n"), []byte("\n"))
		dataNorm := bytes.ReplaceAll(data, []byte("\r\n"), []byte("\n"))
		if !bytes.Equal(dataNorm, goldenNorm) {
			t.Errorf("generated output differs from golden docs/summary.example.json")
		}
	}
}

func TestUsage_RunSeedDemo_ErrorBranch(t *testing.T) {
	t.Setenv("LGW_DATABASE_DRIVER", "invalid-driver")
	code := runSeedDemo()
	if code != 1 {
		t.Errorf("expected runSeedDemo to return 1 with invalid driver, got %d", code)
	}
}

func TestUsage_InitDatabase_ErrorBranch(t *testing.T) {
	_, _, err := initDatabase(context.Background(), "invalid-driver", "")
	if err == nil {
		t.Error("expected initDatabase to fail with invalid driver")
	}
}

func TestUsage_StartNATSServices_ErrorBranch(_ *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cleanup := startNATSServices(context.Background(), "nats://invalid-host:1234", nil, logger)
	cleanup()
}

func TestUsage_RunAndEndpoints_AC1E14(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName:    "usage",
		HTTPPort:       port,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		DatabaseDSN:    ":memory:",
		AdminToken:     "test-token",
	}

	var logBuf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(&logBuf))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, url)

	// Test unauthenticated call to /admin/summary
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url + "/admin/summary")
	if err != nil {
		t.Fatalf("GET /admin/summary failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized without token, got %d", resp.StatusCode)
	}

	// Test authenticated call to /admin/summary
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/admin/summary", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET /admin/summary with token failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK with valid token, got %d", resp.StatusCode)
	}

	client.CloseIdleConnections()
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
	if !strings.Contains(logOutput, "shutting down usage service") {
		t.Errorf("expected shutdown log in buffer, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "shutdown completed successfully") {
		t.Errorf("expected shutdown completed log in buffer, got: %s", logOutput)
	}
}

func TestUsage_Readyz503WithFailingCheck(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName:    "usage",
		HTTPPort:       port,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		DatabaseDSN:    ":memory:",
		AdminToken:     "test-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	failingCheck := func(_ context.Context) error {
		return errors.New("database connection failed")
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithCheck("external_dep", failingCheck))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d/readyz", port)
	waitForStatus(t, url, http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestUsage_Readyz503WhenDBClosed(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName:    "usage",
		HTTPPort:       port,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		DatabaseDSN:    ":memory:",
		AdminToken:     "test-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	stCh := make(chan *store.Store, 1)
	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithStoreSeeder(func(st *store.Store) error {
			stCh <- st
			return nil
		}))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, url)

	select {
	case st := <-stCh:
		_ = st.DB().Close()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for store reference from seeder")
	}
	waitForStatus(t, url+"/readyz", http.StatusServiceUnavailable)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestUsage_WithStoreSeeder(t *testing.T) {
	port := getFreePort(t)
	cfg := Config{
		ServiceName:    "usage",
		HTTPPort:       port,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		DatabaseDSN:    ":memory:",
		AdminToken:     "test-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	var seeded bool
	seeder := func(_ *store.Store) error {
		seeded = true
		return nil
	}

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard), WithStoreSeeder(seeder))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, url)

	if !seeded {
		t.Error("expected store seeder to have been called")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestUsage_WithNATSServices(t *testing.T) {
	_, natsURL := natsx.StartEmbeddedServer(t)
	port := getFreePort(t)
	cfg := Config{
		ServiceName:    "usage",
		HTTPPort:       port,
		LogLevel:       "debug",
		DatabaseDriver: "sqlite",
		DatabaseDSN:    ":memory:",
		NATSURL:        natsURL,
		AdminToken:     "test-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, cfg, WithLogWriter(io.Discard))
	}()

	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForEndpoints(t, url)

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestUsage_RunPeriodicImports(_ *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runPeriodicImports(ctx, 0, nil, nil, logger)
}

func TestUsage_RunPeriodicImports_TickerExecutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	db, err := sqlx.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()
	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}
	st := store.New(db)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ccImp := claudecode.New(st, t.TempDir(), logger)
	orgImp := org.New(st, org.Config{
		AnthropicAdminKey: "anthropic-key",
		OpenAIAdminKey:    "openai-key",
	}, logger)

	done := make(chan struct{})
	go func() {
		runPeriodicImports(ctx, 10*time.Millisecond, ccImp, orgImp, logger)
		close(done)
	}()

	time.Sleep(35 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic imports did not stop after cancel")
	}
}
