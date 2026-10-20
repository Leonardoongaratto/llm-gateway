package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/httpapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

type mockBudgetStore struct {
	limits map[string]core.KeyLimits
	setErr error
	getErr error
}

func newMockBudgetStore() *mockBudgetStore {
	return &mockBudgetStore{
		limits: make(map[string]core.KeyLimits),
	}
}

func (m *mockBudgetStore) GetKeyLimits(_ context.Context, keyID string) (*core.KeyLimits, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	l, ok := m.limits[keyID]
	if !ok {
		return nil, core.ErrNotFound
	}
	return &l, nil
}

func (m *mockBudgetStore) SetKeyLimits(_ context.Context, limits core.KeyLimits) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.limits[limits.KeyID] = limits
	return nil
}

func TestBudgetHTTP_Unauthorized(t *testing.T) {
	store := newMockBudgetStore()
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	req := httptest.NewRequest(http.MethodGet, "/admin/limits/key-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec.Code)
	}
}

func TestBudgetHTTP_SetAndGetLimits(t *testing.T) {
	store := newMockBudgetStore()
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	payload := map[string]any{
		"key_id":               "key-test-1",
		"daily_limit_micros":   100_000,
		"monthly_limit_micros": 500_000,
		"rpm":                  60,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/limits", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for POST /admin/limits, got %d: %s", rec.Code, rec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/admin/limits/key-test-1", nil)
	getReq.Header.Set("Authorization", "Bearer secret-token")
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /admin/limits/key-test-1, got %d: %s", getRec.Code, getRec.Body.String())
	}

	var res struct {
		KeyID            string `json:"key_id"`
		DailyLimitMicros int64  `json:"daily_limit_micros"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if res.KeyID != "key-test-1" {
		t.Errorf("expected key_id key-test-1, got %v", res.KeyID)
	}
	if res.DailyLimitMicros != 100_000 {
		t.Errorf("expected daily 100000, got %v", res.DailyLimitMicros)
	}
}

func TestBudgetHTTP_PUTWithKeyInPath(t *testing.T) {
	store := newMockBudgetStore()
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	payload := map[string]any{
		"daily_limit_micros":   200_000,
		"monthly_limit_micros": 800_000,
		"rpm":                  120,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/admin/limits/key-put-path", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for PUT /admin/limits/key-put-path, got %d: %s", rec.Code, rec.Body.String())
	}

	if store.limits["key-put-path"].DailyLimitMicros != money.Micros(200_000) {
		t.Errorf("expected daily limit 200000, got %d", store.limits["key-put-path"].DailyLimitMicros)
	}
}

func TestBudgetHTTP_NotFound(t *testing.T) {
	store := newMockBudgetStore()
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	req := httptest.NewRequest(http.MethodGet, "/admin/limits/non-existent", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d", rec.Code)
	}
}

func TestBudgetHTTP_ValidationErrors(t *testing.T) {
	store := newMockBudgetStore()
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	// Negative limit
	body := []byte(`{"key_id":"k1","daily_limit_micros":-5}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/limits", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for negative limits, got %d", rec.Code)
	}

	// Missing key_id
	bodyNoKey := []byte(`{"daily_limit_micros":50}`)
	reqNoKey := httptest.NewRequest(http.MethodPost, "/admin/limits", bytes.NewReader(bodyNoKey))
	reqNoKey.Header.Set("Authorization", "Bearer secret-token")
	recNoKey := httptest.NewRecorder()
	handler.ServeHTTP(recNoKey, reqNoKey)
	if recNoKey.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing key_id, got %d", recNoKey.Code)
	}

	// Invalid json
	reqBadJSON := httptest.NewRequest(http.MethodPost, "/admin/limits", bytes.NewReader([]byte(`not-json`)))
	reqBadJSON.Header.Set("Authorization", "Bearer secret-token")
	recBadJSON := httptest.NewRecorder()
	handler.ServeHTTP(recBadJSON, reqBadJSON)
	if recBadJSON.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for malformed json, got %d", recBadJSON.Code)
	}
}

func TestBudgetHTTP_InternalErrors(t *testing.T) {
	store := newMockBudgetStore()
	store.setErr = errors.New("db error")
	store.getErr = errors.New("db error")
	handler := httpapi.NewHandler(store, httpapi.Config{AdminToken: "secret-token"})

	body := []byte(`{"key_id":"k1","daily_limit_micros":50}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/limits", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for setErr, got %d", rec.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/admin/limits/k1", nil)
	getReq.Header.Set("Authorization", "Bearer secret-token")
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for getErr, got %d", getRec.Code)
	}
}
