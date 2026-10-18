package money_test

import (
	"math"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

// AC-1A-03: Given 1000 tokens de entrada a 3.000.000 micros/MTok e 500 de saída a 15.000.000 micros/MTok,
// when CostFor é somado, then o resultado é 10.500 micros.
func TestCostFor_And_Sum_AC1A03(t *testing.T) {
	inputCost, err := money.CostFor(1000, 3_000_000)
	if err != nil {
		t.Fatalf("unexpected error for input tokens: %v", err)
	}
	if inputCost != 3000 {
		t.Fatalf("expected 3000 micros, got %d", inputCost)
	}

	outputCost, err := money.CostFor(500, 15_000_000)
	if err != nil {
		t.Fatalf("unexpected error for output tokens: %v", err)
	}
	if outputCost != 7500 {
		t.Fatalf("expected 7500 micros, got %d", outputCost)
	}

	total, err := money.Add(inputCost, outputCost)
	if err != nil {
		t.Fatalf("unexpected error adding costs: %v", err)
	}
	if total != 10500 {
		t.Fatalf("expected total 10500 micros, got %d", total)
	}
}

// AC-1A-04: Given multiplicação ou soma que excederia int64, when calculada,
// then retorna erro de overflow e nunca um valor errado.
func TestOverflow_AC1A04(t *testing.T) {
	// Addition overflow
	_, err := money.Add(math.MaxInt64, 1)
	if err != money.ErrOverflow {
		t.Fatalf("expected ErrOverflow on addition, got %v", err)
	}

	// Subtraction overflow (underflow with negative result)
	_, err = money.Sub(math.MinInt64, 1)
	if err != money.ErrOverflow {
		t.Fatalf("expected ErrOverflow on subtraction underflow, got %v", err)
	}

	// CostFor multiplication overflow
	_, err = money.CostFor(math.MaxInt64/2, 3_000_000)
	if err != money.ErrOverflow {
		t.Fatalf("expected ErrOverflow on CostFor, got %v", err)
	}
}

func TestSub(t *testing.T) {
	res, err := money.Sub(1000, 400)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 600 {
		t.Fatalf("expected 600, got %d", res)
	}
}

func TestFormatUSD(t *testing.T) {
	cases := []struct {
		m        money.Micros
		expected string
	}{
		{0, "$0.00"},
		{1_000_000, "$1.00"},
		{1_234_560_000, "$1,234.56"},
		{10_500, "$0.0105"},
	}

	for _, tc := range cases {
		actual := money.FormatUSD(tc.m)
		if actual != tc.expected {
			t.Errorf("for %d micros, expected %s, got %s", tc.m, tc.expected, actual)
		}
	}
}
