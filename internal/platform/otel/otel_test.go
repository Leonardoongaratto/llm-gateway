package otel_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
	globalotel "go.opentelemetry.io/otel"
)

func TestOTel_InitAndPrometheusMetrics(t *testing.T) {
	ctx := context.Background()
	provider, err := otel.Init(ctx, otel.Config{
		ServiceName:  "test-service",
		OTelEndpoint: "", // no endpoint, uses no-op/in-memory
	})
	if err != nil {
		t.Fatalf("failed to init otel: %v", err)
	}
	defer func() {
		if err := provider.Shutdown(ctx); err != nil {
			t.Errorf("error during otel shutdown: %v", err)
		}
	}()

	// Verify global propagator is configured
	propagator := globalotel.GetTextMapPropagator()
	if propagator == nil || len(propagator.Fields()) == 0 {
		t.Fatal("expected configured W3C text map propagator")
	}

	// Record a sample metric
	meter := globalotel.Meter("test-meter")
	counter, err := meter.Int64Counter("test_requests_total")
	if err != nil {
		t.Fatalf("failed to create counter: %v", err)
	}
	counter.Add(ctx, 42)

	// Verify Prometheus handler serves metric
	handler := provider.MetricsHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "test_requests_total") {
		t.Errorf("expected prometheus metrics to contain test_requests_total, got: %s", body)
	}
}

func TestOTel_WithEndpoint(t *testing.T) {
	ctx := context.Background()
	provider, err := otel.Init(ctx, otel.Config{
		ServiceName:  "endpoint-service",
		OTelEndpoint: "127.0.0.1:4318",
	})
	if err != nil {
		t.Fatalf("failed to init otel with endpoint: %v", err)
	}
	defer func() { _ = provider.Shutdown(ctx) }()
}
