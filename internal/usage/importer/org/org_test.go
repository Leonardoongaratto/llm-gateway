package org_test

import (
	"context"
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
}

func TestOrgImporter_AnthropicSuccess(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "ant-secret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"date":"2026-10-18","model":"claude-3-5-sonnet","input_tokens":5000,"output_tokens":1000,"cost_micros":30000}]}`)
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
	if records[0].CostKind != string(core.CostKindCobrado) || records[0].Provider != "anthropic" {
		t.Errorf("unexpected record properties: %+v", records[0])
	}
}

func TestOrgImporter_OpenAISuccess(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer oai-secret-key" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"date":"2026-10-18","model":"gpt-4o","input_tokens":10000,"output_tokens":2000}]}`)
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
	if records[0].CostMicros <= 0 {
		t.Errorf("expected cost calculated from tokens, got %d", records[0].CostMicros)
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
}
