package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
)

func TestLivez(t *testing.T) {
	checker := health.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)

	checker.LivezHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var res map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode livez json: %v", err)
	}
	if res["status"] != "ok" {
		t.Fatalf("expected status=ok, got %s", res["status"])
	}
}

func TestReadyz_AllPassing(t *testing.T) {
	checker := health.New()
	checker.RegisterCheck("db", func(_ context.Context) error {
		return nil
	})
	checker.RegisterCheck("nats", func(_ context.Context) error {
		return nil
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	checker.ReadyzHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var res struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to parse readyz json: %v", err)
	}

	if res.Status != "ok" {
		t.Errorf("expected status=ok, got %s", res.Status)
	}
	if res.Checks["db"] != "ok" || res.Checks["nats"] != "ok" {
		t.Errorf("expected checks to be ok, got %v", res.Checks)
	}
}

func TestReadyz_CheckFails(t *testing.T) {
	checker := health.New()
	checker.RegisterCheck("db", func(_ context.Context) error {
		return nil
	})
	checker.RegisterCheck("nats", func(_ context.Context) error {
		return errors.New("connection refused")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	checker.ReadyzHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var res struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to parse readyz json: %v", err)
	}

	if res.Status != "down" {
		t.Errorf("expected status=down, got %s", res.Status)
	}
	if res.Checks["nats"] != "connection refused" {
		t.Errorf("expected nats error check, got %s", res.Checks["nats"])
	}
}
