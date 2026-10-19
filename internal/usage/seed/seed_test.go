package seed_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/seed"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-seed-%d.db", time.Now().UnixNano()))
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

func verifySeededSummary(t *testing.T, sum *report.SummaryReport) {
	if sum.PeriodDays != 30 {
		t.Errorf("expected 30 period days, got %d", sum.PeriodDays)
	}
	if sum.SpendMicros <= 0 {
		t.Errorf("expected positive spend micros, got %d", sum.SpendMicros)
	}
	if sum.PrevSpendMicros <= 0 {
		t.Errorf("expected positive prev spend micros, got %d", sum.PrevSpendMicros)
	}
	if sum.ProjectedMonthMicros <= 0 {
		t.Errorf("expected positive projected month micros, got %d", sum.ProjectedMonthMicros)
	}
	if len(sum.Daily) == 0 {
		t.Errorf("expected non-empty daily series")
	}
	if len(sum.ByModel) == 0 {
		t.Errorf("expected non-empty by_model")
	}
	if len(sum.ByKey) == 0 {
		t.Errorf("expected non-empty by_key")
	}
	if len(sum.ByProvider) == 0 {
		t.Errorf("expected non-empty by_provider")
	}
	if len(sum.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription, got %d", len(sum.Subscriptions))
	}
	if sum.Subscriptions[0].Aproveitamento != 4.2 {
		t.Errorf("expected aproveitamento 4.2, got %v", sum.Subscriptions[0].Aproveitamento)
	}
}

func TestSeedDemo_AC1E13(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	outputPath := filepath.Join(t.TempDir(), "docs", "summary.example.json")
	asOf := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	sum, err := seed.Demo(ctx, st, outputPath, asOf)
	if err != nil {
		t.Fatalf("Demo failed: %v", err)
	}

	verifySeededSummary(t, sum)

	// Verify file was written and is valid JSON
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}

	var parsed report.SummaryReport
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal saved json: %v", err)
	}

	if parsed.PeriodDays != 30 || parsed.Subscriptions[0].Aproveitamento != 4.2 {
		t.Errorf("mismatch in parsed json file: %+v", parsed)
	}
}
