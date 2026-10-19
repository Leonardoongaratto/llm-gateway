package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/httpapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func setupTestServer(t *testing.T, token string) (*httptest.Server, *store.Store) {
	t.Helper()
	ctx := context.Background()

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-api-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}

	st := store.New(db)
	rep := report.NewEngine(st)
	ccImp := claudecode.New(st, t.TempDir(), nil)
	orgImp := org.New(st, org.Config{}, nil)

	handler := httpapi.NewHandler(st, rep, ccImp, orgImp, httpapi.Config{AdminToken: token})
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
		_ = db.Close()
	})

	return srv, st
}

func doRequest(t *testing.T, method, url, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var bodyReader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal error: %v", err)
		}
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		t.Fatalf("new request error: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	return resp, buf.Bytes()
}

func TestHealthEndpoints_AC1E14(t *testing.T) {
	srv, st := setupTestServer(t, "secret-token")

	// /livez
	resp, body := doRequest(t, http.MethodGet, srv.URL+"/livez", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected livez 200, got %d", resp.StatusCode)
	}
	if !bytes.Contains(body, []byte(`"status":"ok"`)) {
		t.Errorf("expected livez ok, got %s", string(body))
	}

	// /readyz
	respReady, _ := doRequest(t, http.MethodGet, srv.URL+"/readyz", "", nil)
	if respReady.StatusCode != http.StatusOK {
		t.Errorf("expected readyz 200, got %d", respReady.StatusCode)
	}

	// /metrics
	respMetrics, _ := doRequest(t, http.MethodGet, srv.URL+"/metrics", "", nil)
	if respMetrics.StatusCode != http.StatusOK {
		t.Errorf("expected metrics 200, got %d", respMetrics.StatusCode)
	}

	// Close DB to verify 503 on readyz
	_ = st.DB().Close()
	respDown, bodyDown := doRequest(t, http.MethodGet, srv.URL+"/readyz", "", nil)
	if respDown.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when db is closed, got %d: %s", respDown.StatusCode, string(bodyDown))
	}
}

func TestAuth(t *testing.T) {
	srv, _ := setupTestServer(t, "valid-token")

	// Unauthorized without token
	resp, _ := doRequest(t, http.MethodGet, srv.URL+"/admin/usage", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without token, got %d", resp.StatusCode)
	}

	// Unauthorized with wrong token
	respWrong, _ := doRequest(t, http.MethodGet, srv.URL+"/admin/usage", "wrong-token", nil)
	if respWrong.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong token, got %d", respWrong.StatusCode)
	}

	// Authorized with correct token
	respOK, _ := doRequest(t, http.MethodGet, srv.URL+"/admin/usage", "valid-token", nil)
	if respOK.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with correct token, got %d", respOK.StatusCode)
	}
}

func testSubscriptionsWorkflow(t *testing.T, srvURL, token string) {
	// Create
	subPayload := map[string]any{
		"name":                "Claude Code Pro",
		"provider":            "anthropic",
		"monthly_cost_micros": 20000000,
		"renewal_day":         15,
	}
	respCreate, bodyCreate := doRequest(t, http.MethodPost, srvURL+"/admin/subscriptions", token, subPayload)
	if respCreate.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 on create sub, got %d: %s", respCreate.StatusCode, string(bodyCreate))
	}

	var created map[string]any
	_ = json.Unmarshal(bodyCreate, &created)
	subID, _ := created["id"].(string)
	if subID == "" {
		t.Fatalf("expected non-empty sub ID in response: %s", string(bodyCreate))
	}

	// Get by ID
	respGet, bodyGet := doRequest(t, http.MethodGet, srvURL+"/admin/subscriptions/"+subID, token, nil)
	if respGet.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on get sub, got %d: %s", respGet.StatusCode, string(bodyGet))
	}

	// List
	respList, bodyList := doRequest(t, http.MethodGet, srvURL+"/admin/subscriptions", token, nil)
	if respList.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on list subs, got %d: %s", respList.StatusCode, string(bodyList))
	}

	// Delete
	respDel, _ := doRequest(t, http.MethodDelete, srvURL+"/admin/subscriptions/"+subID, token, nil)
	if respDel.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", respDel.StatusCode)
	}

	// Delete again -> 404
	respDel2, _ := doRequest(t, http.MethodDelete, srvURL+"/admin/subscriptions/"+subID, token, nil)
	if respDel2.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 on deleting non-existent sub, got %d", respDel2.StatusCode)
	}
}

func TestSubscriptions_CRUD(t *testing.T) {
	srv, _ := setupTestServer(t, "")
	testSubscriptionsWorkflow(t, srv.URL, "")

	// Invalid create payloads
	resp, _ := doRequest(t, http.MethodPost, srv.URL+"/admin/subscriptions", "", map[string]any{"name": ""})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for empty name, got %d", resp.StatusCode)
	}

	// Get not found
	resp404, _ := doRequest(t, http.MethodGet, srv.URL+"/admin/subscriptions/fake-id", "", nil)
	if resp404.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for fake sub, got %d", resp404.StatusCode)
	}
}

func TestUsageAndSummaryAPIs(t *testing.T) {
	ctx := context.Background()
	srv, st := setupTestServer(t, "")

	// Seed one record
	now := time.Now().UTC()
	rec := store.UsageRecord{
		RequestID:   "req-api-1",
		KeyID:       "key-test-1",
		ModelUsed:   "claude-3-5-sonnet",
		Provider:    "anthropic",
		Source:      "gateway",
		CostKind:    "cobrado",
		CostMicros:  5000,
		InputTokens: 1000,
		CreatedAt:   now,
	}
	_, _ = st.RecordUsage(ctx, rec)

	// GET /admin/usage
	respUsage, bodyUsage := doRequest(t, http.MethodGet, srv.URL+"/admin/usage?key_id=key-test-1", "", nil)
	if respUsage.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /admin/usage, got %d: %s", respUsage.StatusCode, string(bodyUsage))
	}

	var usageMap map[string]any
	_ = json.Unmarshal(bodyUsage, &usageMap)
	if total, ok := usageMap["total"].(float64); !ok || int(total) != 1 {
		t.Errorf("expected total=1 in usage response: %s", string(bodyUsage))
	}

	// GET /admin/summary
	respSum, bodySum := doRequest(t, http.MethodGet, srv.URL+"/admin/summary?days=30", "", nil)
	if respSum.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /admin/summary, got %d: %s", respSum.StatusCode, string(bodySum))
	}

	var sumMap map[string]any
	_ = json.Unmarshal(bodySum, &sumMap)
	if spend, ok := sumMap["spend_micros"].(float64); !ok || int64(spend) != 5000 {
		t.Errorf("expected spend_micros=5000 in summary response: %s", string(bodySum))
	}
}

func TestImportsEndpoints(t *testing.T) {
	srv, _ := setupTestServer(t, "")

	// POST /admin/imports/claude-code
	respCC, bodyCC := doRequest(t, http.MethodPost, srv.URL+"/admin/imports/claude-code", "", nil)
	if respCC.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from claude-code import, got %d: %s", respCC.StatusCode, string(bodyCC))
	}

	// POST /admin/imports/anthropic-org (disabled -> 400)
	respOrg, _ := doRequest(t, http.MethodPost, srv.URL+"/admin/imports/anthropic-org", "", nil)
	if respOrg.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for disabled org import, got %d", respOrg.StatusCode)
	}

	// POST /admin/imports/unknown -> 400
	respUnknown, _ := doRequest(t, http.MethodPost, srv.URL+"/admin/imports/unknown-source", "", nil)
	if respUnknown.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for unknown import source, got %d", respUnknown.StatusCode)
	}

	// GET /admin/imports
	respList, bodyList := doRequest(t, http.MethodGet, srv.URL+"/admin/imports", "", nil)
	if respList.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from list imports, got %d: %s", respList.StatusCode, string(bodyList))
	}
}
