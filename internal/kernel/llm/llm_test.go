package llm_test

import (
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestSentinelErrors(t *testing.T) {
	errs := []error{
		llm.ErrInvalidKey,
		llm.ErrModelNotAllowed,
		llm.ErrBudgetExceeded,
		llm.ErrRateLimited,
		llm.ErrBudgetUnavailable,
		llm.ErrRetryable,
		llm.ErrAllProvidersFailed,
		llm.ErrInvalidRequest,
	}

	seen := make(map[string]bool)
	for _, err := range errs {
		if err == nil {
			t.Fatal("expected non-nil error")
		}
		if seen[err.Error()] {
			t.Fatalf("duplicate error message: %s", err.Error())
		}
		seen[err.Error()] = true
	}
}

func TestUsageTotal(t *testing.T) {
	u := llm.Usage{
		InputTokens:      100,
		OutputTokens:     50,
		CacheReadTokens:  20,
		CacheWriteTokens: 10,
	}

	if u.Total() != 180 {
		t.Fatalf("expected total 180 tokens, got %d", u.Total())
	}
}

func TestConstants(t *testing.T) {
	if llm.CostKindBilled != "billed" || llm.CostKindEquivalent != "equivalent" {
		t.Fatal("invalid CostKind constants")
	}

	sources := []llm.Source{
		llm.SourceGateway,
		llm.SourceClaudeCodeLocal,
		llm.SourceAnthropicOrg,
		llm.SourceOpenAIOrg,
	}
	for _, s := range sources {
		if string(s) == "" {
			t.Fatal("empty source constant")
		}
	}
}
