package router_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestRouter_Execute_DirectSuccess(t *testing.T) {
	fp := fake.New(fake.WithName("anthropic"), fake.WithContent("hello direct"))
	r := router.New()
	r.Register("claude-3-5-sonnet", fp)

	snap := &auth.Snapshot{}

	req := &llm.Request{
		ID:    "req-1",
		Model: "claude-3-5-sonnet",
	}

	res, err := r.Execute(context.Background(), snap, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ProviderUsed != "anthropic" || res.ModelUsed != "claude-3-5-sonnet" {
		t.Errorf("unexpected provider/model: %s/%s", res.ProviderUsed, res.ModelUsed)
	}
	if res.Response.Content != "hello direct" {
		t.Errorf("unexpected content: %s", res.Response.Content)
	}
	if res.FallbackFrom != "" {
		t.Errorf("expected no fallback, got %s", res.FallbackFrom)
	}
}

func TestRouter_AC1D05_AutomaticFallback_On503(t *testing.T) {
	// AC-1D-05: Logical model with primary -> secondary.
	// Primary returns 503, router performs automatic fallback to secondary.
	err503 := &provider.UpstreamError{StatusCode: 503, Message: "primary overloaded", Retryable: true}
	pPrimary := fake.New(fake.WithName("primary-prov"), fake.WithError(err503))
	pSecondary := fake.New(fake.WithName("secondary-prov"), fake.WithContent("hello from secondary"))

	r := router.New()
	r.Register("primary-model", pPrimary)
	r.Register("secondary-model", pSecondary)

	snap := &auth.Snapshot{
		LogicalModels: map[string][]string{
			"rapido": {"primary-model", "secondary-model"},
		},
	}

	req := &llm.Request{
		ID:    "req-fallback-1",
		Model: "rapido",
	}

	var fallbackRecorded bool
	var fallbackFrom, fallbackTo string
	r.SetFallbackHook(func(_ context.Context, _, from, to, _ string) {
		fallbackRecorded = true
		fallbackFrom = from
		fallbackTo = to
	})

	res, err := r.Execute(context.Background(), snap, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ProviderUsed != "secondary-prov" {
		t.Errorf("expected provider 'secondary-prov', got %s", res.ProviderUsed)
	}
	if res.ModelUsed != "secondary-model" {
		t.Errorf("expected model 'secondary-model', got %s", res.ModelUsed)
	}
	if res.FallbackFrom != "primary-prov" {
		t.Errorf("expected FallbackFrom 'primary-prov', got %s", res.FallbackFrom)
	}
	if !fallbackRecorded || fallbackFrom != "primary-prov" || fallbackTo != "secondary-prov" {
		t.Errorf("fallback hook was not called properly: %v, from=%s, to=%s", fallbackRecorded, fallbackFrom, fallbackTo)
	}
	if pPrimary.CallCount() != 1 || pSecondary.CallCount() != 1 {
		t.Errorf("expected 1 call each, got primary=%d, secondary=%d", pPrimary.CallCount(), pSecondary.CallCount())
	}
}

func TestRouter_AC1D06_NoFallback_On400(t *testing.T) {
	// AC-1D-06: 400 validation error does NOT trigger fallback and is returned directly.
	err400 := &provider.UpstreamError{StatusCode: 400, Message: "invalid param", Retryable: false}
	pPrimary := fake.New(fake.WithName("primary-prov"), fake.WithError(err400))
	pSecondary := fake.New(fake.WithName("secondary-prov"), fake.WithContent("should not be called"))

	r := router.New()
	r.Register("primary-model", pPrimary)
	r.Register("secondary-model", pSecondary)

	snap := &auth.Snapshot{
		LogicalModels: map[string][]string{
			"logical-model": {"primary-model", "secondary-model"},
		},
	}

	req := &llm.Request{
		ID:    "req-400-test",
		Model: "logical-model",
	}

	_, err := r.Execute(context.Background(), snap, req)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, llm.ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
	// Secondary provider must NOT have been called!
	if pSecondary.CallCount() != 0 {
		t.Fatalf("secondary provider was called %d times; must be 0 on 400 error", pSecondary.CallCount())
	}
}

func TestRouter_AllProvidersFail(t *testing.T) {
	err503 := &provider.UpstreamError{StatusCode: 503, Message: "down", Retryable: true}
	p1 := fake.New(fake.WithName("p1"), fake.WithError(err503))
	p2 := fake.New(fake.WithName("p2"), fake.WithError(err503))

	r := router.New()
	r.Register("m1", p1)
	r.Register("m2", p2)

	snap := &auth.Snapshot{
		LogicalModels: map[string][]string{
			"chain-down": {"m1", "m2"},
		},
	}

	_, err := r.Execute(context.Background(), snap, &llm.Request{Model: "chain-down"})
	if !errors.Is(err, llm.ErrAllProvidersFailed) {
		t.Fatalf("expected ErrAllProvidersFailed, got %v", err)
	}
}

func TestRouter_ExecuteStream_FallbackAndSuccess(t *testing.T) {
	err503 := &provider.UpstreamError{StatusCode: 503, Message: "stream fail", Retryable: true}
	p1 := fake.New(fake.WithName("p1"), fake.WithError(err503))
	p2 := fake.New(fake.WithName("p2"), fake.WithStreamChunks(llm.Chunk{Content: "stream chunk"}))

	r := router.New()
	r.Register("m1", p1)
	r.Register("m2", p2)

	snap := &auth.Snapshot{
		LogicalModels: map[string][]string{
			"stream-chain": {"m1", "m2"},
		},
	}

	res, err := r.ExecuteStream(context.Background(), snap, &llm.Request{Model: "stream-chain"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ProviderUsed != "p2" || res.FallbackFrom != "p1" {
		t.Errorf("unexpected fallback result: %+v", res)
	}

	var count int
	for range res.Stream {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 chunk, got %d", count)
	}
}

func TestRouter_ResolveHeuristicsAndDefault(t *testing.T) {
	pAnthropic := fake.New(fake.WithName("anthropic"), fake.WithContent("claude response"))
	pOpenAI := fake.New(fake.WithName("openai"), fake.WithContent("gpt response"))
	pGemini := fake.New(fake.WithName("gemini"), fake.WithContent("gemini response"))
	pDefault := fake.New(fake.WithName("default"), fake.WithContent("default response"))

	r := router.New()
	r.Register("anthropic", pAnthropic)
	r.Register("openai", pOpenAI)
	r.Register("gemini", pGemini)
	r.SetDefaultProvider(pDefault)

	snap := &auth.Snapshot{}

	// Heuristic claude
	res, err := r.Execute(context.Background(), snap, &llm.Request{Model: "claude-3-5-haiku"})
	if err != nil || res.ProviderUsed != "anthropic" {
		t.Fatalf("expected anthropic for claude model, got %s (err: %v)", res.ProviderUsed, err)
	}

	// Heuristic gpt
	res, err = r.Execute(context.Background(), snap, &llm.Request{Model: "gpt-4o-mini"})
	if err != nil || res.ProviderUsed != "openai" {
		t.Fatalf("expected openai for gpt model, got %s (err: %v)", res.ProviderUsed, err)
	}

	// Heuristic gemini
	res, err = r.Execute(context.Background(), snap, &llm.Request{Model: "gemini-1.5-flash"})
	if err != nil || res.ProviderUsed != "gemini" {
		t.Fatalf("expected gemini for gemini model, got %s (err: %v)", res.ProviderUsed, err)
	}

	// Prefix syntax "anthropic/claude-custom"
	res, err = r.Execute(context.Background(), snap, &llm.Request{Model: "anthropic/claude-custom"})
	if err != nil || res.ProviderUsed != "anthropic" {
		t.Fatalf("expected anthropic for prefix model, got %s (err: %v)", res.ProviderUsed, err)
	}

	// Unknown model uses default provider
	res, err = r.Execute(context.Background(), snap, &llm.Request{Model: "unknown-custom-model"})
	if err != nil || res.ProviderUsed != "default" {
		t.Fatalf("expected default for unknown model, got %s (err: %v)", res.ProviderUsed, err)
	}
}
