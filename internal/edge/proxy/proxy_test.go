package proxy_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/edge/proxy"
)

type upstreamMocks struct {
	gateway *httptest.Server
	control *httptest.Server
	usage   *httptest.Server
	budget  *httptest.Server
}

func (u *upstreamMocks) Close() {
	u.gateway.Close()
	u.control.Close()
	u.usage.Close()
	u.budget.Close()
}

func setupUpstreamMocks(t *testing.T) *upstreamMocks {
	t.Helper()

	makeServer := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/readyz" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
				return
			}
			w.Header().Set("X-Upstream-Service", name)
			w.Header().Set("X-Received-Request-Id", r.Header.Get("X-Request-Id"))
			w.Header().Set("X-Received-Traceparent", r.Header.Get("traceparent"))

			if r.Header.Get("Accept") == "text/event-stream" {
				w.Header().Set("Content-Type", "text/event-stream")
				flusher, ok := w.(http.Flusher)
				if ok {
					_, _ = fmt.Fprintf(w, "data: chunk1\n\n")
					flusher.Flush()
					_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
					flusher.Flush()
				}
				return
			}

			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"service":"%s","path":"%s"}`, name, r.URL.Path)
		}))
	}

	mocks := &upstreamMocks{
		gateway: makeServer("gateway"),
		control: makeServer("control"),
		usage:   makeServer("usage"),
		budget:  makeServer("budget"),
	}
	t.Cleanup(mocks.Close)
	return mocks
}

func TestProxy_RoutingTable_AC1F01(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	tests := []struct {
		path            string
		expectedService string
	}{
		{"/v1/chat/completions", "gateway"},
		{"/v1/messages", "gateway"},
		{"/v1/models", "gateway"},
		{"/admin/keys", "control"},
		{"/admin/keys/key-123", "control"},
		{"/admin/pricing", "control"},
		{"/admin/models", "control"},
		{"/admin/usage", "usage"},
		{"/admin/summary", "usage"},
		{"/admin/subscriptions", "usage"},
		{"/admin/imports/claude-code", "usage"},
		{"/admin/limits", "budget"},
		{"/admin/limits/key-123", "budget"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected status 200, got %d", rec.Code)
			}
			upstream := rec.Header().Get("X-Upstream-Service")
			if upstream != tt.expectedService {
				t.Errorf("path %s routed to %s, expected %s", tt.path, upstream, tt.expectedService)
			}
		})
	}
}

func TestProxy_RequestID_GeneratedAndPreserved(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	t.Run("preserves client request ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set("X-Request-Id", "client-req-999")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Header().Get("X-Request-Id") != "client-req-999" {
			t.Errorf("expected response X-Request-Id 'client-req-999', got '%s'", rec.Header().Get("X-Request-Id"))
		}
		if rec.Header().Get("X-Received-Request-Id") != "client-req-999" {
			t.Errorf("expected upstream X-Request-Id 'client-req-999', got '%s'", rec.Header().Get("X-Received-Request-Id"))
		}
	})

	t.Run("generates request ID if omitted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		genID := rec.Header().Get("X-Request-Id")
		if genID == "" {
			t.Errorf("expected non-empty generated X-Request-Id")
		}
		if rec.Header().Get("X-Received-Request-Id") != genID {
			t.Errorf("expected upstream to receive generated ID '%s', got '%s'", genID, rec.Header().Get("X-Received-Request-Id"))
		}
	})
}

func TestProxy_TraceparentPropagation(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("traceparent", traceparent)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	received := rec.Header().Get("X-Received-Traceparent")
	if !strings.Contains(received, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Errorf("expected traceparent trace ID to be propagated, got '%s'", received)
	}
}

func TestProxy_StreamingSSE_AC1F01(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	req.Header.Set("Accept", "text/event-stream")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "data: chunk1") || !strings.Contains(body, "data: [DONE]") {
		t.Errorf("expected SSE chunks in response, got: %s", body)
	}
}

func TestProxy_MaxBodyBytes_AC1F01(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL:   mocks.gateway.URL,
		ControlURL:   mocks.control.URL,
		UsageURL:     mocks.usage.URL,
		BudgetURL:    mocks.budget.URL,
		MaxBodyBytes: 100, // Small limit for testing
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	largeBody := strings.Repeat("A", 200)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(largeBody))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected status 413, got %d", rec.Code)
	}
}

func TestProxy_Health_AllServicesHealthy_AC1F02(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Status   string            `json:"status"`
		Services map[string]string `json:"services"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse health response: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got '%s'", resp.Status)
	}
	for _, svc := range []string{"gateway", "control", "budget", "usage"} {
		if resp.Services[svc] != "ok" {
			t.Errorf("expected service '%s' to be 'ok', got '%s'", svc, resp.Services[svc])
		}
	}
}

func TestProxy_Health_PartialFailureReturns503_AC1F02(t *testing.T) {
	mocks := setupUpstreamMocks(t)

	// Close budget server to simulate outage
	mocks.budget.Close()

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Status   string            `json:"status"`
		Services map[string]string `json:"services"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse health response: %v", err)
	}

	if resp.Status != "unhealthy" {
		t.Errorf("expected status 'unhealthy', got '%s'", resp.Status)
	}
	if resp.Services["budget"] == "ok" {
		t.Errorf("expected budget service to report failure, got 'ok'")
	}
}

func assertResponseContains(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Errorf("path %s: expected status %d, got %d", path, wantStatus, rec.Code)
	}
	if wantBody != "" && !strings.Contains(rec.Body.String(), wantBody) {
		t.Errorf("path %s: expected body containing %q, got: %s", path, wantBody, rec.Body.String())
	}
}

func TestProxy_Dashboard_SPA_AC1F03(t *testing.T) {
	mocks := setupUpstreamMocks(t)
	tempDir := t.TempDir()

	indexContent := "<html><body>Dashboard Index</body></html>"
	if err := os.WriteFile(filepath.Join(tempDir, "index.html"), []byte(indexContent), 0644); err != nil {
		t.Fatalf("failed to write index.html: %v", err)
	}

	assetsDir := filepath.Join(tempDir, "assets")
	if err := os.MkdirAll(assetsDir, 0755); err != nil {
		t.Fatalf("failed to create assets dir: %v", err)
	}
	jsContent := "console.log('dashboard bundle');"
	if err := os.WriteFile(filepath.Join(assetsDir, "main.js"), []byte(jsContent), 0644); err != nil {
		t.Fatalf("failed to write main.js: %v", err)
	}

	handler, err := proxy.New(proxy.Config{
		GatewayURL: mocks.gateway.URL,
		ControlURL: mocks.control.URL,
		UsageURL:   mocks.usage.URL,
		BudgetURL:  mocks.budget.URL,
		WebDir:     tempDir,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	t.Run("serves index.html at /dashboard", func(t *testing.T) {
		assertResponseContains(t, handler, "/dashboard", http.StatusOK, "Dashboard Index")
	})

	t.Run("serves static assets at /dashboard/assets/main.js", func(t *testing.T) {
		assertResponseContains(t, handler, "/dashboard/assets/main.js", http.StatusOK, "dashboard bundle")
	})

	t.Run("falls back to index.html for SPA route /dashboard/projetos", func(t *testing.T) {
		assertResponseContains(t, handler, "/dashboard/projetos", http.StatusOK, "Dashboard Index")
	})

	t.Run("serves 404 when WebDir is empty", func(t *testing.T) {
		noWebHandler, err := proxy.New(proxy.Config{
			GatewayURL: mocks.gateway.URL,
			ControlURL: mocks.control.URL,
			UsageURL:   mocks.usage.URL,
			BudgetURL:  mocks.budget.URL,
			WebDir:     "",
		})
		if err != nil {
			t.Fatalf("failed to create proxy: %v", err)
		}
		assertResponseContains(t, noWebHandler, "/dashboard", http.StatusNotFound, "")
	})
}

func TestProxy_MultipleGateways_RoundRobin(t *testing.T) {
	var count1, count2 atomic.Int64

	gw1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count1.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"gw":1}`))
	}))
	defer gw1.Close()

	gw2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count2.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"gw":2}`))
	}))
	defer gw2.Close()

	handler, err := proxy.New(proxy.Config{
		GatewayURLs: []string{gw1.URL, gw2.URL},
		ControlURL:  gw1.URL,
		UsageURL:    gw1.URL,
		BudgetURL:   gw1.URL,
	})
	if err != nil {
		t.Fatalf("failed to create proxy: %v", err)
	}

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d failed with status %d", i, rec.Code)
		}
	}

	if count1.Load() != 5 || count2.Load() != 5 {
		t.Errorf("expected 5 hits each, got gw1=%d, gw2=%d", count1.Load(), count2.Load())
	}
}
