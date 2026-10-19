package core_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
)

func TestSourceValidation(t *testing.T) {
	valid := []string{"gateway", "claude-code-local", "anthropic-org", "openai-org"}
	for _, s := range valid {
		if !core.ValidSource(s) {
			t.Errorf("expected %s to be valid", s)
		}
	}

	invalid := []string{"", "unknown", "azure", "google"}
	for _, s := range invalid {
		if core.ValidSource(s) {
			t.Errorf("expected %s to be invalid", s)
		}
	}

	if core.SourceGateway.String() != "gateway" {
		t.Errorf("unexpected string representation: %s", core.SourceGateway.String())
	}
}

func TestNormalizeCostKind(t *testing.T) {
	tests := []struct {
		input    string
		expected core.CostKind
		wantErr  bool
	}{
		{"cobrado", core.CostKindCobrado, false},
		{"billed", core.CostKindCobrado, false},
		{"COBRADO", core.CostKindCobrado, false},
		{" BILLED ", core.CostKindCobrado, false},
		{"equivalente", core.CostKindEquivalente, false},
		{"equivalent", core.CostKindEquivalente, false},
		{"EQUIVALENT", core.CostKindEquivalente, false},
		{"invalid", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		got, err := core.NormalizeCostKind(tt.input)
		if tt.wantErr {
			if !errors.Is(err, core.ErrInvalidCostKind) {
				t.Errorf("input %q: expected ErrInvalidCostKind, got %v", tt.input, err)
			}
		} else {
			if err != nil || got != tt.expected {
				t.Errorf("input %q: expected %v, got %v (err: %v)", tt.input, tt.expected, got, err)
			}
			if got.String() != string(tt.expected) {
				t.Errorf("unexpected String(): %s", got.String())
			}
		}
	}
}

func TestProjectedMonthMicros(t *testing.T) {
	// AC-1E-09: day 17 of 31 days with 142000000 micros spend
	// (142000000 * 31) / 17 = 258941176
	proj := core.ProjectedMonthMicros(142000000, 17, 31)
	if proj != 258941176 {
		t.Errorf("expected 258941176, got %d", proj)
	}

	// Edge cases
	if p := core.ProjectedMonthMicros(100, 0, 31); p != 100 {
		t.Errorf("expected 100 for currentDay 0, got %d", p)
	}
	if p := core.ProjectedMonthMicros(100, 10, 0); p != 100 {
		t.Errorf("expected 100 for totalDays 0, got %d", p)
	}
	if p := core.ProjectedMonthMicros(100, 31, 31); p != 100 {
		t.Errorf("expected 100 for currentDay >= totalDays, got %d", p)
	}
}

func TestDaysInMonth(t *testing.T) {
	oct2026 := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	if d := core.DaysInMonth(oct2026); d != 31 {
		t.Errorf("expected 31 days in Oct 2026, got %d", d)
	}

	feb2024Leap := time.Date(2024, 2, 10, 0, 0, 0, 0, time.UTC)
	if d := core.DaysInMonth(feb2024Leap); d != 29 {
		t.Errorf("expected 29 days in Feb 2024, got %d", d)
	}

	feb2026 := time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)
	if d := core.DaysInMonth(feb2026); d != 28 {
		t.Errorf("expected 28 days in Feb 2026, got %d", d)
	}
}

func TestCalculateAproveitamento(t *testing.T) {
	// AC-1E-10: $84 equivalent vs $20 monthly cost -> 4.2
	ap := core.CalculateAproveitamento(84000000, 20000000)
	if ap != 4.2 {
		t.Errorf("expected 4.2, got %v", ap)
	}

	// Zero or negative cost
	if a := core.CalculateAproveitamento(84000000, 0); a != 0.0 {
		t.Errorf("expected 0.0, got %v", a)
	}
	if a := core.CalculateAproveitamento(0, 20000000); a != 0.0 {
		t.Errorf("expected 0.0, got %v", a)
	}
}

func TestPricingCalculation(t *testing.T) {
	// 1000 input tokens, 200 output tokens on Sonnet
	// input: (1000 * 3000000)/1000000 = 3000 micros
	// output: (200 * 15000000)/1000000 = 3000 micros
	// total = 6000 micros
	cost := core.CalculateTokenCostMicros("claude-3-5-sonnet", 1000, 200, 0, 0)
	if cost != 6000 {
		t.Errorf("expected 6000 micros, got %d", cost)
	}

	// Opus
	costOpus := core.CalculateTokenCostMicros("claude-3-opus", 1000, 200, 1000, 100)
	if costOpus <= 0 {
		t.Errorf("expected positive opus cost, got %d", costOpus)
	}

	// Haiku
	costHaiku := core.CalculateTokenCostMicros("claude-3-5-haiku", 1000, 200, 0, 0)
	if costHaiku <= 0 {
		t.Errorf("expected positive haiku cost, got %d", costHaiku)
	}

	// GPT-4o
	costGPT4o := core.CalculateTokenCostMicros("gpt-4o", 1000, 200, 0, 0)
	if costGPT4o <= 0 {
		t.Errorf("expected positive gpt-4o cost, got %d", costGPT4o)
	}

	// GPT-4o-mini
	costMini := core.CalculateTokenCostMicros("gpt-4o-mini", 1000, 200, 0, 0)
	if costMini <= 0 {
		t.Errorf("expected positive mini cost, got %d", costMini)
	}
}

func TestReconciliation(t *testing.T) {
	// AC-1E-11: Reconciliation and anti-double counting
	gw := map[string]int64{
		"anthropic": 50000000,
		"openai":    30000000,
	}
	org := map[string]int64{
		"anthropic": 55000000, // org had extra 5M not in gateway
		"cohere":    10000000, // only in org
	}

	rep := core.ReconcileAll(gw, org)

	anthropic := rep.ByProvider["anthropic"]
	if anthropic.GatewaySpendMicros != 50000000 {
		t.Errorf("expected 50M gateway, got %d", anthropic.GatewaySpendMicros)
	}
	if anthropic.OrgSpendMicros != 55000000 {
		t.Errorf("expected 55M org, got %d", anthropic.OrgSpendMicros)
	}
	if anthropic.OverlapMicros != 50000000 {
		t.Errorf("expected 50M overlap, got %d", anthropic.OverlapMicros)
	}
	if anthropic.DiscrepancyMicros != 5000000 {
		t.Errorf("expected 5M discrepancy, got %d", anthropic.DiscrepancyMicros)
	}
	if anthropic.ReconciledSpendMicros != 55000000 {
		t.Errorf("expected 55M reconciled, got %d", anthropic.ReconciledSpendMicros)
	}

	openai := rep.ByProvider["openai"]
	if openai.ReconciledSpendMicros != 30000000 || openai.OverlapMicros != 0 {
		t.Errorf("unexpected openai reconciliation: %+v", openai)
	}

	cohere := rep.ByProvider["cohere"]
	if cohere.ReconciledSpendMicros != 10000000 || cohere.OverlapMicros != 0 {
		t.Errorf("unexpected cohere reconciliation: %+v", cohere)
	}

	// Provider where gateway > org
	rec := core.ReconcileProvider("anthropic", 60000000, 50000000)
	if rec.OverlapMicros != 50000000 || rec.DiscrepancyMicros != 10000000 || rec.ReconciledSpendMicros != 60000000 {
		t.Errorf("unexpected rec where gw > org: %+v", rec)
	}
}
