package money_test

import (
	"errors"
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
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("expected ErrOverflow on addition, got %v", err)
	}

	// Subtraction overflow (underflow with negative result)
	_, err = money.Sub(math.MinInt64, 1)
	if !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("expected ErrOverflow on subtraction underflow, got %v", err)
	}

	// CostFor multiplication overflow table test
	costCases := []struct {
		name         string
		tokens       int64
		pricePerMTok money.Micros
	}{
		{"half max with 3M", math.MaxInt64 / 2, 3_000_000},
		{"half max with 15M (quotient overflow)", math.MaxInt64 / 2, 15_000_000},
		{"max tokens with max price", math.MaxInt64, math.MaxInt64},
		{"2M tokens with max price", 2_000_000, math.MaxInt64},
	}

	for _, tc := range costCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := money.CostFor(tc.tokens, tc.pricePerMTok)
			if !errors.Is(err, money.ErrOverflow) {
				t.Fatalf("expected ErrOverflow on CostFor(%d, %d), got %v", tc.tokens, tc.pricePerMTok, err)
			}
		})
	}

	// 1 token with MaxInt64 should NOT overflow (result is MaxInt64 / 1M)
	c, err := money.CostFor(1, math.MaxInt64)
	if err != nil {
		t.Fatalf("unexpected error for 1 token at max price: %v", err)
	}
	expected := money.Micros(math.MaxInt64 / 1_000_000)
	if c != expected {
		t.Fatalf("expected %d micros, got %d", expected, c)
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
		{1_234_567_000_000, "$1,234,567.00"},
		{-1_000_000, "-$1.00"},
		{-1_234_560_000, "-$1,234.56"},
		{10_500, "$0.0105"},
	}

	for _, tc := range cases {
		actual := money.FormatUSD(tc.m)
		if actual != tc.expected {
			t.Errorf("FormatUSD(%d), expected %s, got %s", tc.m, tc.expected, actual)
		}
	}
}

func TestFormatPTBR(t *testing.T) {
	cases := []struct {
		m        money.Micros
		expected string
	}{
		{0, "US$ 0,00"},
		{1_000_000, "US$ 1,00"},
		{1_234_560_000, "US$ 1.234,56"},
		{1_234_567_000_000, "US$ 1.234.567,00"},
		{-1_000_000, "-US$ 1,00"},
	}

	for _, tc := range cases {
		actual := money.FormatPTBR(tc.m)
		if actual != tc.expected {
			t.Errorf("FormatPTBR(%d), expected %s, got %s", tc.m, tc.expected, actual)
		}
	}
}
