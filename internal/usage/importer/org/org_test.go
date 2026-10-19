package org_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
		fmt.Fprint(w, `{
			"data": [
				{
					"starting_at": "2026-10-18T00:00:00Z",
					"ending_at": "2026-10-18T23:59:59Z",
					"results": [
						{
							"amount": "123.45",
							"currency": "USD",
							"model": "claude-3-5-sonnet-20241022",
							"workspace_id": "ws-test-1"
						}
					]
				}
			],
			"has_more": false,
			"next_page": null
		}`)
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
	if rec.CostMicros != 1_234_500 {
		t.Errorf("expected 1_234_500 micros for amount '123.45', got %d", rec.CostMicros)
	}
	if rec.RequestID != "org-anthropic-2026-10-18-claude-3-5-sonnet-20241022-ws-test-1" {
		t.Errorf("unexpected request ID: %s", rec.RequestID)
	}
}

func TestOrgImporter_AnthropicNullModelDefaultsToOrgTotal(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": [
				{
					"starting_at": "2026-10-18T00:00:00Z",
					"results": [
						{
							"amount": "50.00",
							"currency": "USD",
							"model": null
						}
					]
				}
			],
			"has_more": false
		}`)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "ant-secret-key",
		AnthropicBaseURL:  server.URL,
	}, nil)

	run, err := imp.Import(ctx, "anthropic-org")
	if err != nil || run.RecordsImported != 1 {
		t.Fatalf("expected 1 record imported: %v", err)
	}

	records, _, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceAnthropicOrg)})
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 record: %v", err)
	}
	if records[0].ModelRequested != "org-total" || records[0].ModelUsed != "org-total" {
		t.Errorf("expected model 'org-total', got '%s'", records[0].ModelRequested)
	}
	if records[0].CostMicros != 500_000 {
		t.Errorf("expected 500_000 micros for 50.00 cents, got %d", records[0].CostMicros)
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
		if !strings.Contains(r.URL.RawQuery, "group_by=line_item") || !strings.Contains(r.URL.RawQuery, "group_by=project_id") {
			http.Error(w, "missing group_by query params", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{
			"object": "page",
			"data": [
				{
					"object": "bucket",
					"start_time": %d,
					"end_time": %d,
					"results": [
						{
							"object": "organization.costs.result",
							"amount": {
								"value": 25.50,
								"currency": "usd"
							},
							"line_item": "gpt-4o",
							"project_id": "proj-test-1"
						}
					]
				}
			],
			"has_more": false,
			"next_page": null
		}`, ts, ts+86400)
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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		if page == "" {
			fmt.Fprint(w, `{
				"data": [
					{
						"starting_at": "2026-10-17T00:00:00Z",
						"results": [{"amount": "10.00", "model": "claude-3-5-haiku"}]
					}
				],
				"has_more": true,
				"next_page": "page-2"
			}`)
			return
		}

		fmt.Fprint(w, `{
			"data": [
				{
					"starting_at": "2026-10-18T00:00:00Z",
					"results": [{"amount": "20.00", "model": "claude-3-5-sonnet"}]
				}
			],
			"has_more": false,
			"next_page": null
		}`)
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

func assertOrgImportAndDailyCost(t *testing.T, imp *org.Importer, st *store.Store, expectedMicros int64) {
	t.Helper()
	ctx := context.Background()
	run, err := imp.Import(ctx, "anthropic-org")
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if run.RecordsImported != 1 {
		t.Fatalf("expected 1 record imported, got %d", run.RecordsImported)
	}

	records, _, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceAnthropicOrg)})
	if err != nil {
		t.Fatalf("query usage failed: %v", err)
	}
	if len(records) != 1 || records[0].CostMicros != expectedMicros {
		t.Fatalf("expected record cost %d, got %v", expectedMicros, records)
	}

	aggs, err := st.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil {
		t.Fatalf("query daily aggregates failed: %v", err)
	}
	if len(aggs) != 1 || aggs[0].CostMicros != expectedMicros {
		t.Fatalf("expected aggregate cost %d, got %v", expectedMicros, aggs)
	}
}

func TestOrgImporter_SnapshotCostIncreaseAndAdjustment(t *testing.T) {
	st := setupTestStore(t)

	currentAmount := "1000.00"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{
			"data": [
				{
					"starting_at": "2026-10-18T00:00:00Z",
					"results": [
						{
							"amount": "%s",
							"model": "claude-3-5-sonnet-20241022"
						}
					]
				}
			],
			"has_more": false
		}`, currentAmount)
	}))
	defer server.Close()

	imp := org.New(st, org.Config{
		AnthropicAdminKey: "ant-key",
		AnthropicBaseURL:  server.URL,
	}, nil)

	assertOrgImportAndDailyCost(t, imp, st, 10_000_000)

	currentAmount = "2500.00"
	assertOrgImportAndDailyCost(t, imp, st, 25_000_000)

	currentAmount = "2200.00"
	assertOrgImportAndDailyCost(t, imp, st, 22_000_000)
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
