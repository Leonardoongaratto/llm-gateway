package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/httpapi"
)

type mockStore struct {
	keys          map[string]core.VirtualKey
	pricing       []core.Pricing
	logicalModels []core.LogicalModel
}

func newMockStore() *mockStore {
	return &mockStore{
		keys: make(map[string]core.VirtualKey),
	}
}

func (m *mockStore) CreateKey(_ context.Context, k core.VirtualKey) (*core.VirtualKey, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	if k.ID == "" {
		k.ID = "generated-id"
	}
	k.CreatedAt = time.Now().UTC()
	m.keys[k.ID] = k
	return &k, nil
}

func (m *mockStore) GetKeyByID(_ context.Context, id string) (*core.VirtualKey, error) {
	k, ok := m.keys[id]
	if !ok {
		return nil, core.ErrKeyNotFound
	}
	return &k, nil
}

func (m *mockStore) ListKeys(_ context.Context) ([]core.VirtualKey, error) {
	res := make([]core.VirtualKey, 0, len(m.keys))
	for _, k := range m.keys {
		res = append(res, k)
	}
	return res, nil
}

func (m *mockStore) RevokeKey(_ context.Context, id string, revokedAt time.Time) error {
	k, ok := m.keys[id]
	if !ok {
		return core.ErrKeyNotFound
	}
	if k.RevokedAt != nil {
		return core.ErrKeyRevoked
	}
	k.RevokedAt = &revokedAt
	m.keys[id] = k
	return nil
}

func (m *mockStore) ListPricing(_ context.Context) ([]core.Pricing, error) {
	return m.pricing, nil
}

func (m *mockStore) UpsertPricing(_ context.Context, items []core.Pricing) error {
	for _, item := range items {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	m.pricing = items
	return nil
}

func (m *mockStore) ListLogicalModels(_ context.Context) ([]core.LogicalModel, error) {
	return m.logicalModels, nil
}

func (m *mockStore) UpsertLogicalModels(_ context.Context, models []core.LogicalModel) error {
	for _, model := range models {
		if err := model.Validate(); err != nil {
			return err
		}
	}
	m.logicalModels = models
	return nil
}

func newTestServer() (http.Handler, *mockStore) {
	ms := newMockStore()
	handler := httpapi.NewHandler(ms, httpapi.Config{
		AdminToken: "secret-admin-token",
		KeyPepper:  "secret-pepper",
	})
	return handler, ms
}

func executeRequest(handler http.Handler, req *http.Request, token string) *httptest.ResponseRecorder {
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func TestAuthMiddleware(t *testing.T) {
	handler, _ := newTestServer()

	reqNoAuth := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	rrNoAuth := executeRequest(handler, reqNoAuth, "")
	if rrNoAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized without auth, got %d", rrNoAuth.Code)
	}

	reqBadAuth := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	reqBadAuth.Header.Set("Authorization", "Basic invalid")
	rrBadAuth := httptest.NewRecorder()
	handler.ServeHTTP(rrBadAuth, reqBadAuth)
	if rrBadAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with bad auth header, got %d", rrBadAuth.Code)
	}

	reqWrongToken := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	rrWrongToken := executeRequest(handler, reqWrongToken, "wrong-token")
	if rrWrongToken.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong token, got %d", rrWrongToken.Code)
	}
}

func TestCreateKey_SuccessAndValidation(t *testing.T) {
	handler, _ := newTestServer()

	validBody := map[string]any{
		"name":                 "Production Client",
		"allowed_models":       []string{"claude-3-5-sonnet"},
		"daily_limit_micros":   100000,
		"monthly_limit_micros": 1000000,
		"rpm":                  60,
	}
	bodyBytes, _ := json.Marshal(validBody)
	req := httptest.NewRequest(http.MethodPost, "/admin/keys", bytes.NewReader(bodyBytes))
	rr := executeRequest(handler, req, "secret-admin-token")

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	rawKey, ok := resp["key"].(string)
	if !ok || !strings.HasPrefix(rawKey, core.KeyPrefix) {
		t.Errorf("expected raw key with prefix lgw_, got %v", resp["key"])
	}
	if _, hasHash := resp["key_hash"]; hasHash {
		t.Error("key_hash must never be exposed in HTTP response")
	}

	invalidReq := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader(`{"name":""}`))
	rrInvalid := executeRequest(handler, invalidReq, "secret-admin-token")
	if rrInvalid.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request on invalid key, got %d", rrInvalid.Code)
	}
}

func TestListKeys_ExcludesKeyHash(t *testing.T) {
	handler, ms := newTestServer()
	ms.keys["k-1"] = core.VirtualKey{
		ID:        "k-1",
		Name:      "Existing Key",
		KeyHash:   "secret-hash-value",
		CreatedAt: time.Now().UTC(),
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	rr := executeRequest(handler, req, "secret-admin-token")
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	if strings.Contains(rr.Body.String(), "secret-hash-value") {
		t.Error("key hash was leaked in list keys response")
	}
}

func TestRevokeKey_Scenarios(t *testing.T) {
	handler, ms := newTestServer()
	ms.keys["k-to-del"] = core.VirtualKey{
		ID:        "k-to-del",
		Name:      "Revokable Key",
		KeyHash:   "hash",
		CreatedAt: time.Now().UTC(),
	}

	reqSuccess := httptest.NewRequest(http.MethodDelete, "/admin/keys/k-to-del", nil)
	rrSuccess := executeRequest(handler, reqSuccess, "secret-admin-token")
	if rrSuccess.Code != http.StatusNoContent {
		t.Errorf("expected 204 No Content, got %d", rrSuccess.Code)
	}

	reqConflict := httptest.NewRequest(http.MethodDelete, "/admin/keys/k-to-del", nil)
	rrConflict := executeRequest(handler, reqConflict, "secret-admin-token")
	if rrConflict.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict when already revoked, got %d", rrConflict.Code)
	}

	reqNotFound := httptest.NewRequest(http.MethodDelete, "/admin/keys/unknown", nil)
	rrNotFound := executeRequest(handler, reqNotFound, "secret-admin-token")
	if rrNotFound.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", rrNotFound.Code)
	}
}

func TestPricingEndpoints(t *testing.T) {
	handler, _ := newTestServer()

	pricingData := []map[string]any{
		{
			"model":                       "claude-3-5-sonnet",
			"provider":                    "anthropic",
			"input_per_mtok_micros":       3000000,
			"output_per_mtok_micros":      15000000,
			"cache_read_per_mtok_micros":  300000,
			"cache_write_per_mtok_micros": 3750000,
		},
	}
	body, _ := json.Marshal(pricingData)
	putReq := httptest.NewRequest(http.MethodPut, "/admin/pricing", bytes.NewReader(body))
	rrPut := executeRequest(handler, putReq, "secret-admin-token")
	if rrPut.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on put pricing, got %d: %s", rrPut.Code, rrPut.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/admin/pricing", nil)
	rrGet := executeRequest(handler, getReq, "secret-admin-token")
	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on get pricing, got %d", rrGet.Code)
	}

	badPut := httptest.NewRequest(http.MethodPut, "/admin/pricing", strings.NewReader(`[{"model":""}]`))
	rrBadPut := executeRequest(handler, badPut, "secret-admin-token")
	if rrBadPut.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request on invalid pricing, got %d", rrBadPut.Code)
	}
}

func TestModelsEndpoints(t *testing.T) {
	handler, _ := newTestServer()

	modelsData := []map[string]any{
		{
			"name":  "claude-fast",
			"chain": []string{"claude-3-5-haiku"},
		},
	}
	body, _ := json.Marshal(modelsData)
	putReq := httptest.NewRequest(http.MethodPut, "/admin/models", bytes.NewReader(body))
	rrPut := executeRequest(handler, putReq, "secret-admin-token")
	if rrPut.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on put models, got %d", rrPut.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/admin/models", nil)
	rrGet := executeRequest(handler, getReq, "secret-admin-token")
	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on get models, got %d", rrGet.Code)
	}

	badPut := httptest.NewRequest(http.MethodPut, "/admin/models", strings.NewReader(`[{"name":""}]`))
	rrBadPut := executeRequest(handler, badPut, "secret-admin-token")
	if rrBadPut.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request on invalid model, got %d", rrBadPut.Code)
	}

	malformedPut := httptest.NewRequest(http.MethodPut, "/admin/models", strings.NewReader(`invalid-json`))
	rrMalformed := executeRequest(handler, malformedPut, "secret-admin-token")
	if rrMalformed.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request on malformed JSON, got %d", rrMalformed.Code)
	}
}

type failingStore struct {
	mockStore
}

func (f *failingStore) ListKeys(_ context.Context) ([]core.VirtualKey, error) {
	return nil, errors.New("db error")
}

func (f *failingStore) CreateKey(_ context.Context, _ core.VirtualKey) (*core.VirtualKey, error) {
	return nil, errors.New("db error")
}

func (f *failingStore) RevokeKey(_ context.Context, _ string, _ time.Time) error {
	return errors.New("db error")
}

func (f *failingStore) ListPricing(_ context.Context) ([]core.Pricing, error) {
	return nil, errors.New("db error")
}

func (f *failingStore) UpsertPricing(_ context.Context, _ []core.Pricing) error {
	return errors.New("db error")
}

func (f *failingStore) ListLogicalModels(_ context.Context) ([]core.LogicalModel, error) {
	return nil, errors.New("db error")
}

func (f *failingStore) UpsertLogicalModels(_ context.Context, _ []core.LogicalModel) error {
	return errors.New("db error")
}

func TestStoreFailuresAndMalformedBodies(t *testing.T) {
	fs := &failingStore{mockStore: *newMockStore()}
	handler := httpapi.NewHandler(fs, httpapi.Config{
		AdminToken: "token",
		KeyPepper:  "pepper",
	})

	// Malformed POST /admin/keys
	reqBadJSON := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader(`{bad`))
	rrBadJSON := executeRequest(handler, reqBadJSON, "token")
	if rrBadJSON.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on malformed body, got %d", rrBadJSON.Code)
	}

	// Store error on CreateKey
	reqCreate := httptest.NewRequest(http.MethodPost, "/admin/keys", strings.NewReader(`{"name":"test"}`))
	rrCreate := executeRequest(handler, reqCreate, "token")
	if rrCreate.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on store error, got %d", rrCreate.Code)
	}

	// Store error on ListKeys
	reqList := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	rrList := executeRequest(handler, reqList, "token")
	if rrList.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on list keys store error, got %d", rrList.Code)
	}

	// Store error on RevokeKey
	reqRevoke := httptest.NewRequest(http.MethodDelete, "/admin/keys/k1", nil)
	rrRevoke := executeRequest(handler, reqRevoke, "token")
	if rrRevoke.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on revoke store error, got %d", rrRevoke.Code)
	}

	// Store error on ListPricing
	reqListP := httptest.NewRequest(http.MethodGet, "/admin/pricing", nil)
	rrListP := executeRequest(handler, reqListP, "token")
	if rrListP.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on list pricing store error, got %d", rrListP.Code)
	}

	// Store error on UpsertPricing
	reqUpsertP := httptest.NewRequest(http.MethodPut, "/admin/pricing", strings.NewReader(`[{"model":"m","provider":"p"}]`))
	rrUpsertP := executeRequest(handler, reqUpsertP, "token")
	if rrUpsertP.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on upsert pricing store error, got %d", rrUpsertP.Code)
	}

	// Store error on ListModels
	reqListM := httptest.NewRequest(http.MethodGet, "/admin/models", nil)
	rrListM := executeRequest(handler, reqListM, "token")
	if rrListM.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on list models store error, got %d", rrListM.Code)
	}

	// Store error on UpsertModels
	reqUpsertM := httptest.NewRequest(http.MethodPut, "/admin/models", strings.NewReader(`[{"name":"m","chain":["p"]}]`))
	rrUpsertM := executeRequest(handler, reqUpsertM, "token")
	if rrUpsertM.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on upsert models store error, got %d", rrUpsertM.Code)
	}

	// Empty admin token config denies all
	noTokenHandler := httpapi.NewHandler(fs, httpapi.Config{AdminToken: ""})
	reqAny := httptest.NewRequest(http.MethodGet, "/admin/keys", nil)
	reqAny.Header.Set("Authorization", "Bearer token")
	rrAny := httptest.NewRecorder()
	noTokenHandler.ServeHTTP(rrAny, reqAny)
	if rrAny.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 when AdminToken is not configured, got %d", rrAny.Code)
	}
}
