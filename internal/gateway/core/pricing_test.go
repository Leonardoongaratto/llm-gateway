package core_test

import (
	"errors"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

func TestCalculateCost_AC1D03(t *testing.T) {
	// AC-1D-03: 1,000 input tokens and 500 output tokens at 3 USD/Mtok and 15 USD/Mtok
	// Expected cost: exactly 10,500 micros (money.Micros(10500))
	pricing := &auth.PricingInfo{
		Model:                  "claude-3-5-sonnet",
		Provider:               "anthropic",
		InputPerMTokMicros:     3_000_000,  // $3 / Mtok
		OutputPerMTokMicros:    15_000_000, // $15 / Mtok
		CacheReadPerMTokMicros: 300_000,    // $0.30 / Mtok
	}

	usage := llm.Usage{
		InputTokens:  1000,
		OutputTokens: 500,
	}

	cost, err := core.CalculateCost(pricing, usage)
	if err != nil {
		t.Fatalf("unexpected error calculating cost: %v", err)
	}

	expected := money.Micros(10500)
	if cost != expected {
		t.Fatalf("expected %d micros, got %d micros", expected, cost)
	}
}

func TestCalculateCost_WithCache(t *testing.T) {
	pricing := &auth.PricingInfo{
		Model:                   "gpt-4o",
		Provider:                "openai",
		InputPerMTokMicros:      2_500_000,
		OutputPerMTokMicros:     10_000_000,
		CacheReadPerMTokMicros:  1_250_000,
		CacheWritePerMTokMicros: 3_750_000,
	}

	usage := llm.Usage{
		InputTokens:      1000, // 2,500 micros
		OutputTokens:     1000, // 10,000 micros
		CacheReadTokens:  2000, // 2,500 micros
		CacheWriteTokens: 1000, // 3,750 micros
	}

	cost, err := core.CalculateCost(pricing, usage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := money.Micros(18750)
	if cost != expected {
		t.Fatalf("expected %d micros, got %d", expected, cost)
	}
}

func TestEstimateMaxCost(t *testing.T) {
	pricing := &auth.PricingInfo{
		Model:               "claude-3-5-sonnet",
		Provider:            "anthropic",
		InputPerMTokMicros:  3_000_000,
		OutputPerMTokMicros: 15_000_000,
	}

	// 1000 prompt tokens, 2000 max tokens
	// Input: 3,000 micros, Output: 30,000 micros -> 33,000 micros
	est, err := core.EstimateMaxCost(pricing, 1000, 2000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := money.Micros(33000)
	if est != expected {
		t.Fatalf("expected %d micros, got %d", expected, est)
	}

	// When maxTokens is 0, defaults to DefaultMaxTokens (4096)
	estDefault, err := core.EstimateMaxCost(pricing, 1000, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedDefault := money.Micros(3000 + 4096*15)
	if estDefault != expectedDefault {
		t.Fatalf("expected %d micros, got %d", expectedDefault, estDefault)
	}
}

func TestPricing_Errors(t *testing.T) {
	// Nil pricing
	_, err := core.CalculateCost(nil, llm.Usage{})
	if !errors.Is(err, core.ErrPricingNotFound) {
		t.Fatalf("expected ErrPricingNotFound, got %v", err)
	}

	_, err = core.EstimateMaxCost(nil, 100, 100)
	if !errors.Is(err, core.ErrPricingNotFound) {
		t.Fatalf("expected ErrPricingNotFound, got %v", err)
	}

	// Negative prompt tokens should clamp to 0
	pricing := &auth.PricingInfo{
		Model:               "claude-3-5-sonnet",
		Provider:            "anthropic",
		InputPerMTokMicros:  3_000_000,
		OutputPerMTokMicros: 15_000_000,
	}
	est, err := core.EstimateMaxCost(pricing, -50, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if est != money.Micros(15000) {
		t.Fatalf("expected 15000 micros, got %d", est)
	}

	// Invalid negative values causing money.CostFor error
	invalidPricing := &auth.PricingInfo{
		Model:               "claude-3-5-sonnet",
		Provider:            "anthropic",
		InputPerMTokMicros:  -1000,
		OutputPerMTokMicros: 1000,
	}
	if _, err := core.EstimateMaxCost(invalidPricing, 100, 100); err == nil {
		t.Fatal("expected error with negative price")
	}

	invalidOutPricing := &auth.PricingInfo{
		Model:               "claude-3-5-sonnet",
		Provider:            "anthropic",
		InputPerMTokMicros:  1000,
		OutputPerMTokMicros: -1000,
	}
	if _, err := core.EstimateMaxCost(invalidOutPricing, 100, 100); err == nil {
		t.Fatal("expected error with negative output price")
	}

	if _, err := core.CalculateCost(invalidPricing, llm.Usage{InputTokens: 100}); err == nil {
		t.Fatal("expected error with negative input price")
	}

	if _, err := core.CalculateCost(invalidOutPricing, llm.Usage{OutputTokens: 100}); err == nil {
		t.Fatal("expected error with negative output price")
	}

	invalidCacheRead := &auth.PricingInfo{
		CacheReadPerMTokMicros: -1,
	}
	if _, err := core.CalculateCost(invalidCacheRead, llm.Usage{CacheReadTokens: 100}); err == nil {
		t.Fatal("expected error with negative cache read price")
	}

	invalidCacheWrite := &auth.PricingInfo{
		CacheWritePerMTokMicros: -1,
	}
	if _, err := core.CalculateCost(invalidCacheWrite, llm.Usage{CacheWriteTokens: 100}); err == nil {
		t.Fatal("expected error with negative cache write price")
	}
}

func TestEstimatePromptTokens(t *testing.T) {
	msgs := []llm.Message{
		{Role: "user", Content: "Hello world!"}, // 12 chars -> ~3-4 tokens
	}
	tokens := core.EstimatePromptTokens(msgs)
	if tokens < 1 {
		t.Fatalf("expected at least 1 token, got %d", tokens)
	}

	// Empty messages
	emptyTokens := core.EstimatePromptTokens(nil)
	if emptyTokens != 1 {
		t.Fatalf("expected 1 token for empty messages, got %d", emptyTokens)
	}
}
