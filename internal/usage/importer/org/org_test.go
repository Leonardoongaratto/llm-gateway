package org_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-org-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}

	return store.New(db)
}

func TestOrgImporter_AC1E12_DisabledWhenNoKeys(t *testing.T) {
	t.Setenv("LGW_ANTHROPIC_ADMIN_KEY", "")
	t.Setenv("LGW_OPENAI_ADMIN_KEY", "")

	ctx := context.Background()
	st := setupTestStore(t)

	imp := org.New(st, org.Config{}, nil)

	if imp.IsEnabled("anthropic-org") {
		t.Errorf("expected anthropic-org to be disabled when no key is set")
	}
	if imp.IsEnabled("openai-org") {
		t.Errorf("expected openai-org to be disabled when no key is set")
	}

	_, err := imp.Import(ctx, "anthropic-org")
	if !errors.Is(err, org.ErrSourceNotEnabled) {
		t.Errorf("expected ErrSourceNotEnabled, got %v", err)
	}

	_, err = imp.Import(ctx, "openai-org")
	if !errors.Is(err, org.ErrSourceNotEnabled) {
		t.Errorf("expected ErrSourceNotEnabled, got %v", err)
	}
}

func TestOrgImporter_AnthropicSuccess(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "ant-secret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			http.Error(w, "missing anthropic-version", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := org.AnthropicCostReportResponse{
			Data: []org.AnthropicCostBucket{
				{
					StartingAt: "2026-10-18T00:00:00Z",
					EndingAt:   "2026-10-18T23:59:59Z",
					Results: []org.AnthropicCostResult{
						{
							Amount:      "30.00",
							Currency:    "USD",
							Model:       "claude-3-5-sonnet-20241022",
							WorkspaceID: "ws-test-1",
						},
					},
				},
			},
			HasMore: false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "ant-secret-key",
		AnthropicBaseURL:  server.URL,
	}, nil)

	if !imp.IsEnabled("anthropic-org") {
		t.Fatalf("expected anthropic-org to be enabled")
	}

	run, err := imp.Import(ctx, "anthropic-org")
	if err != nil || run.Status != "success" || run.RecordsImported != 1 {
		t.Fatalf("expected success with 1 record imported: status=%s, count=%d, err=%v", run.Status, run.RecordsImported, err)
	}

	records, total, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceAnthropicOrg)})
	if err != nil || total != 1 || len(records) != 1 {
		t.Fatalf("expected 1 usage record for anthropic-org: total=%d, err=%v", total, err)
	}
	rec := records[0]
	if rec.CostKind != string(core.CostKindCobrado) || rec.Provider != "anthropic" {
		t.Errorf("unexpected record properties: %+v", rec)
	}
	if rec.CostMicros != 30_000_000 {
		t.Errorf("expected 30_000_000 micros, got %d", rec.CostMicros)
	}
	if rec.RequestID != "org-anthropic-2026-10-18-claude-3-5-sonnet-20241022-ws-test-1" {
		t.Errorf("unexpected request ID: %s", rec.RequestID)
	}
}

func TestOrgImporter_OpenAISuccess(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	ts := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC).Unix()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer oai-secret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := org.OpenAICostsResponse{
			Object: "page",
			Data: []org.OpenAICostItem{
				{
					Amount: org.OpenAICostAmount{
						Value:    25.50,
						Currency: "USD",
					},
					LineItem:  "gpt-4o",
					ProjectID: "proj-test-1",
					Timestamp: ts,
				},
			},
			HasMore: false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		OpenAIAdminKey: "oai-secret-key",
		OpenAIBaseURL:  server.URL,
	}, nil)

	if !imp.IsEnabled("openai-org") {
		t.Fatalf("expected openai-org to be enabled")
	}

	run, err := imp.Import(ctx, "openai-org")
	if err != nil || run.Status != "success" || run.RecordsImported != 1 {
		t.Fatalf("expected success with 1 record imported: status=%s, count=%d, err=%v", run.Status, run.RecordsImported, err)
	}

	records, total, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceOpenAIOrg)})
	if err != nil || total != 1 || len(records) != 1 {
		t.Fatalf("expected 1 record for openai-org: total=%d, err=%v", total, err)
	}
	rec := records[0]
	if rec.CostKind != string(core.CostKindCobrado) || rec.Provider != "openai" {
		t.Errorf("unexpected record properties: %+v", rec)
	}
	if rec.CostMicros != 25_500_000 {
		t.Errorf("expected 25_500_000 micros, got %d", rec.CostMicros)
	}
	if rec.RequestID != "org-openai-2026-10-18-gpt-4o-proj-test-1" {
		t.Errorf("unexpected request ID: %s", rec.RequestID)
	}
}

func TestOrgImporter_Pagination(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	page2 := "page-2"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		if page == "" {
			resp := org.AnthropicCostReportResponse{
				Data: []org.AnthropicCostBucket{
					{
						StartingAt: "2026-10-17T00:00:00Z",
						Results: []org.AnthropicCostResult{
							{Amount: 10.00, Model: "claude-3-5-haiku"},
						},
					},
				},
				HasMore:  true,
				NextPage: &page2,
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}

		resp := org.AnthropicCostReportResponse{
			Data: []org.AnthropicCostBucket{
				{
					StartingAt: "2026-10-18T00:00:00Z",
					Results: []org.AnthropicCostResult{
						{Amount: 20.00, Model: "claude-3-5-sonnet"},
					},
				},
			},
			HasMore: false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "ant-key",
		AnthropicBaseURL:  server.URL,
	}, nil)

	run, err := imp.Import(ctx, "anthropic-org")
	if err != nil || run.Status != "success" || run.RecordsImported != 2 {
		t.Fatalf("expected 2 records across 2 pages: status=%s, count=%d, err=%v", run.Status, run.RecordsImported, err)
	}
}

func TestOrgImporter_SnapshotCostIncrease(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	currentAmount := "10.00"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := org.AnthropicCostReportResponse{
			Data: []org.AnthropicCostBucket{
				{
					StartingAt: "2026-10-18T00:00:00Z",
					Results: []org.AnthropicCostResult{
						{
							Amount: currentAmount,
							Model:  "claude-3-5-sonnet",
						},
					},
				},
			},
			HasMore: false,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "ant-key",
		AnthropicBaseURL:  server.URL,
	}, nil)

	// First run: $10.00
	run1, err := imp.Import(ctx, "anthropic-org")
	if err != nil || run1.RecordsImported != 1 {
		t.Fatalf("first import failed: %v", err)
	}

	aggs1, err := st.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs1) != 1 {
		t.Fatalf("expected 1 daily aggregate: %v", err)
	}
	if aggs1[0].CostMicros != 10_000_000 {
		t.Errorf("expected 10M micros in daily aggregate, got %d", aggs1[0].CostMicros)
	}

	// Second run: $25.00 snapshot (increased throughout the day)
	currentAmount = "25.00"
	run2, err := imp.Import(ctx, "anthropic-org")
	if err != nil || run2.RecordsImported != 1 {
		t.Fatalf("second import failed: %v", err)
	}

	// Verify usage record has updated cost
	records, _, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceAnthropicOrg)})
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 record: %v", err)
	}
	if records[0].CostMicros != 25_000_000 {
		t.Errorf("expected updated record to have 25M micros, got %d", records[0].CostMicros)
	}

	// Verify daily_aggregates updated by delta +15M to 25M total
	aggs2, err := st.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs2) != 1 {
		t.Fatalf("expected 1 daily aggregate: %v", err)
	}
	if aggs2[0].CostMicros != 25_000_000 {
		t.Errorf("expected 25M micros in daily aggregate after delta update, got %d", aggs2[0].CostMicros)
	}
}

func TestOrgImporter_ServerError(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "secret",
		AnthropicBaseURL:  server.URL,
	}, nil)

	run, err := imp.Import(ctx, "anthropic-org")
	if err == nil || run.Status != "failed" {
		t.Fatalf("expected failed run on 500 error: status=%s, err=%v", run.Status, err)
	}
}

func TestOrgImporter_UnsupportedSource(t *testing.T) {
	st := setupTestStore(t)

	imp := org.New(st, org.Config{}, nil)
	if imp.IsEnabled("unknown-org") {
		t.Errorf("expected unknown-org to be disabled")
	}

	_, err := imp.Import(context.Background(), "unknown-org")
	if !errors.Is(err, org.ErrSourceNotEnabled) {
		t.Errorf("expected ErrSourceNotEnabled, got %v", err)
	}
}
