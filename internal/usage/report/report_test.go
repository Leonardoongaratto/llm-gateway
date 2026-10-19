package report_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-report-%d.db", time.Now().UnixNano()))
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

func seedTestData(ctx context.Context, t *testing.T, st *store.Store, asOf time.Time) {
	// 1. Current period gateway traffic (Oct 17, 2026)
	day17 := time.Date(asOf.Year(), asOf.Month(), 17, 10, 0, 0, 0, time.UTC)
	rec1 := store.UsageRecord{
		RequestID:        "r-1",
		KeyID:            "key-prod-1",
		ModelUsed:        "claude-3-5-sonnet",
		Provider:         "anthropic",
		Source:           "gateway",
		CostKind:         "cobrado",
		InputTokens:      1000000,
		OutputTokens:     200000,
		CacheReadTokens:  50000,
		CacheWriteTokens: 10000,
		CostMicros:       50000000,
		CreatedAt:        day17,
	}
	if _, err := st.RecordUsage(ctx, rec1); err != nil {
		t.Fatalf("failed to record r1: %v", err)
	}

	// Another key on Oct 17
	rec2 := store.UsageRecord{
		RequestID:   "r-2",
		KeyID:       "key-prod-2",
		ModelUsed:   "gpt-4o",
		Provider:    "openai",
		Source:      "gateway",
		CostKind:    "cobrado",
		InputTokens: 500000,
		CostMicros:  30000000,
		CreatedAt:   day17,
	}
	if _, err := st.RecordUsage(ctx, rec2); err != nil {
		t.Fatalf("failed to record r2: %v", err)
	}

	// 2. Org spend with overlap for anthropic (55M total, 5M discrepancy) (AC-1E-11)
	rec3 := store.UsageRecord{
		RequestID:   "org-ant-1",
		ModelUsed:   "claude-3-5-sonnet",
		Provider:    "anthropic",
		Source:      "anthropic-org",
		CostKind:    "cobrado",
		CostMicros:  55000000,
		InputTokens: 1100000,
		CreatedAt:   day17,
	}
	if _, err := st.RecordUsage(ctx, rec3); err != nil {
		t.Fatalf("failed to record org: %v", err)
	}

	// 3. Claude Code equivalent usage: $84 (84_000_000 micros) (AC-1E-10)
	rec4 := store.UsageRecord{
		RequestID:   "ccl-1",
		ModelUsed:   "claude-3-5-sonnet",
		Provider:    "anthropic",
		Source:      "claude-code-local",
		CostKind:    "equivalente",
		CostMicros:  84000000,
		InputTokens: 20000000,
		CreatedAt:   day17,
	}
	if _, err := st.RecordUsage(ctx, rec4); err != nil {
		t.Fatalf("failed to record equivalent: %v", err)
	}

	// 4. Previous period traffic (60 days ago)
	prevDay := asOf.AddDate(0, 0, -45)
	recPrev := store.UsageRecord{
		RequestID:   "prev-1",
		KeyID:       "key-prod-1",
		ModelUsed:   "claude-3-5-sonnet",
		Provider:    "anthropic",
		Source:      "gateway",
		CostKind:    "cobrado",
		CostMicros:  40000000,
		InputTokens: 800000,
		CreatedAt:   prevDay,
	}
	if _, err := st.RecordUsage(ctx, recPrev); err != nil {
		t.Fatalf("failed to record prev: %v", err)
	}

	// 5. Subscription: US$ 20/mês (20_000_000 micros)
	sub := store.Subscription{
		ID:                "sub-1",
		Name:              "Claude Code Pro",
		Provider:          "anthropic",
		MonthlyCostMicros: 20000000,
		RenewalDay:        15,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}
	if err := st.CreateSubscription(ctx, sub); err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
}

func verifyAC1E08SummaryBreakdowns(t *testing.T, rep *report.SummaryReport) {
	if len(rep.Daily) == 0 {
		t.Errorf("expected daily breakdown items")
	}
	if len(rep.ByModel) == 0 {
		t.Errorf("expected model breakdown items")
	}
	if len(rep.ByKey) == 0 {
		t.Errorf("expected key breakdown items")
	}
	if len(rep.ByProvider) == 0 {
		t.Errorf("expected provider breakdown items")
	}
	if rep.PrevSpendMicros != 40000000 {
		t.Errorf("expected 40M prev spend, got %d", rep.PrevSpendMicros)
	}
}

func verifyAC1E10Aproveitamento(t *testing.T, rep *report.SummaryReport) {
	if len(rep.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription item, got %d", len(rep.Subscriptions))
	}
	s := rep.Subscriptions[0]
	if s.MonthlyCostMicros != 20000000 {
		t.Errorf("expected monthly cost 20M, got %d", s.MonthlyCostMicros)
	}
	if s.EquivalentValueMicros != 84000000 {
		t.Errorf("expected equivalent value 84M, got %d", s.EquivalentValueMicros)
	}
	if s.Aproveitamento != 4.2 {
		t.Errorf("expected aproveitamento 4.2, got %v", s.Aproveitamento)
	}
}

func verifyAC1E11Reconciliation(t *testing.T, rep *report.SummaryReport) {
	ant := rep.Reconciliation.ByProvider["anthropic"]
	if ant.GatewaySpendMicros != 50000000 {
		t.Errorf("expected 50M gateway spend for anthropic, got %d", ant.GatewaySpendMicros)
	}
	if ant.OrgSpendMicros != 55000000 {
		t.Errorf("expected 55M org spend for anthropic, got %d", ant.OrgSpendMicros)
	}
	if ant.OverlapMicros != 50000000 {
		t.Errorf("expected 50M overlap, got %d", ant.OverlapMicros)
	}
	if ant.DiscrepancyMicros != 5000000 {
		t.Errorf("expected 5M discrepancy, got %d", ant.DiscrepancyMicros)
	}
	if ant.ReconciledSpendMicros != 55000000 {
		t.Errorf("expected 55M reconciled spend, got %d", ant.ReconciledSpendMicros)
	}

	// Total spend: Anthropic reconciled (55M) + OpenAI gateway (30M) = 85M
	// Notice: NOT double counted (50M + 55M + 30M = 135M would be double counted!)
	if rep.SpendMicros != 85000000 {
		t.Errorf("expected 85M total reconciled spend, got %d", rep.SpendMicros)
	}
}

func TestReportEngine_SummaryACs(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	// As of October 17, 2026 (day 17 of 31 days)
	asOf := time.Date(2026, 10, 17, 18, 0, 0, 0, time.UTC)
	seedTestData(ctx, t, st, asOf)

	engine := report.NewEngine(st)
	rep, err := engine.GenerateSummary(ctx, 30, asOf)
	if err != nil {
		t.Fatalf("GenerateSummary failed: %v", err)
	}

	t.Run("AC-1E-08: General metrics and breakdowns", func(t *testing.T) {
		verifyAC1E08SummaryBreakdowns(t, rep)
	})

	t.Run("AC-1E-09: Projected month micros calculation", func(t *testing.T) {
		// Day 17 of 31 days with 85_000_000 spend in month to date
		// projected = (85000000 * 31) / 17 = 155000000
		expectedProj := (int64(85000000) * 31) / 17
		if rep.ProjectedMonthMicros != expectedProj {
			t.Errorf("expected projected month micros %d, got %d", expectedProj, rep.ProjectedMonthMicros)
		}
	})

	t.Run("AC-1E-10: Subscriptions and aproveitamento", func(t *testing.T) {
		verifyAC1E10Aproveitamento(t, rep)
	})

	t.Run("AC-1E-11: Reconciliation and anti-double counting", func(t *testing.T) {
		verifyAC1E11Reconciliation(t, rep)
	})
}
