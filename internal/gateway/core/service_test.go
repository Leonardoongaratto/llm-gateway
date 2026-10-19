package core_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

type mockBudgetClient struct {
	reserveFunc  func(ctx context.Context, keyID string, maxMicros int64, requestID string) (string, int64, error)
	settleCalls  []settleRecord
	releaseCalls []string
}

type settleRecord struct {
	reservationID string
	actualMicros  int64
}

func (m *mockBudgetClient) Reserve(ctx context.Context, keyID string, maxMicros int64, requestID string) (string, int64, error) {
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, keyID, maxMicros, requestID)
	}
	return "res-default", 1_000_000, nil
}

func (m *mockBudgetClient) SettleAsync(reservationID string, actualMicros int64) {
	m.settleCalls = append(m.settleCalls, settleRecord{reservationID: reservationID, actualMicros: actualMicros})
}

func (m *mockBudgetClient) Release(ctx context.Context, reservationID string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	m.releaseCalls = append(m.releaseCalls, reservationID)
	return nil
}

type mockEventPublisher struct {
	usageEvents    []*eventsv1.UsageRecorded
	fallbackEvents []*eventsv1.ProviderFallback
}

func (m *mockEventPublisher) PublishUsage(_ context.Context, evt *eventsv1.UsageRecorded) {
	m.usageEvents = append(m.usageEvents, evt)
}

func (m *mockEventPublisher) PublishFallback(_ context.Context, evt *eventsv1.ProviderFallback) {
	m.fallbackEvents = append(m.fallbackEvents, evt)
}

func hashKey(raw, pepper string) string {
	h := sha256.Sum256([]byte(raw + pepper))
	return hex.EncodeToString(h[:])
}

const (
	testPepper = "service-pepper"
	testRawKey = "lgw_1234567890abcdef1234567890abcdef"
)

func setupServiceTestEnv() (*core.Service, *mockBudgetClient, *fake.Provider, *mockEventPublisher, *auth.Manager) {
	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-1",
				Name:          "Test Key",
				KeyHash:       keyHash,
				AllowedModels: []string{"claude-3-5-sonnet", "gpt-4o", "fake-model"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"claude-3-5-sonnet": {
				Model:               "claude-3-5-sonnet",
				Provider:            "anthropic",
				InputPerMTokMicros:  3_000_000,
				OutputPerMTokMicros: 15_000_000,
			},
			"fake-model": {
				Model:               "fake-model",
				Provider:            "fake",
				InputPerMTokMicros:  3_000_000,
				OutputPerMTokMicros: 15_000_000,
			},
		},
	})

	mockBudget := &mockBudgetClient{}
	mockEvents := &mockEventPublisher{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))

	r := router.New()
	fp := fake.New(
		fake.WithName("fake"),
		fake.WithContent("Generated answer"),
		fake.WithUsage(llm.Usage{InputTokens: 1000, OutputTokens: 500}),
	)
	r.Register("fake-model", fp)
	r.Register("claude-3-5-sonnet", fp)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: mockEvents,
		LocalCache:     localCache,
		Router:         r,
	})

	return svc, mockBudget, fp, mockEvents, authMgr
}

func TestService_AC1D01_ChatCompletion_Success(t *testing.T) {
	svc, mockBudget, _, mockEvents, _ := setupServiceTestEnv()

	req := &llm.Request{
		ID:        "req-chat-1",
		Model:     "fake-model",
		MaxTokens: 500,
		Messages: []llm.Message{
			{Role: "user", Content: "Hello gateway"},
		},
	}

	result, err := svc.ChatCompletion(context.Background(), testRawKey, req, 0.7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Provider != "fake" {
		t.Errorf("expected provider 'fake', got %s", result.Provider)
	}
	if result.CacheHit {
		t.Errorf("expected CacheHit=false on temperature 0.7, got true")
	}
	// AC-1D-03: 1000 input tokens @ 3 USD + 500 output tokens @ 15 USD = 10,500 micros
	if result.CostMicros != money.Micros(10500) {
		t.Errorf("expected 10500 micros, got %d", result.CostMicros)
	}

	// SettleAsync was called with actual cost
	if len(mockBudget.settleCalls) != 1 || mockBudget.settleCalls[0].actualMicros != 10500 {
		t.Errorf("unexpected settle calls: %+v", mockBudget.settleCalls)
	}

	// Usage event published
	if len(mockEvents.usageEvents) != 1 || mockEvents.usageEvents[0].CostMicros != 10500 {
		t.Errorf("unexpected usage events: %+v", mockEvents.usageEvents)
	}
}

func TestService_AC1D04_BudgetExceeded(t *testing.T) {
	svc, mockBudget, fp, _, _ := setupServiceTestEnv()

	mockBudget.reserveFunc = func(_ context.Context, _ string, _ int64, _ string) (string, int64, error) {
		return "", 0, llm.ErrBudgetExceeded
	}

	req := &llm.Request{
		ID:    "req-exceeded",
		Model: "fake-model",
	}

	_, err := svc.ChatCompletion(context.Background(), testRawKey, req, 0.7)
	if !errors.Is(err, llm.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}

	// AC-1D-04: Provider must NOT be called
	if fp.CallCount() != 0 {
		t.Fatalf("provider was called %d times; must be 0 when budget exceeded", fp.CallCount())
	}
}

func TestService_AC1D08_CacheHit_TemperatureZero(t *testing.T) {
	svc, mockBudget, fp, mockEvents, _ := setupServiceTestEnv()

	req := &llm.Request{
		ID:        "req-cached",
		Model:     "fake-model",
		MaxTokens: 500,
		Messages: []llm.Message{
			{Role: "user", Content: "Deterministic question"},
		},
	}

	// 1st call: Cache miss, hits provider and reserves/settles budget
	res1, err := svc.ChatCompletion(context.Background(), testRawKey, req, 0.0)
	if err != nil {
		t.Fatalf("unexpected error on 1st call: %v", err)
	}
	if res1.CacheHit {
		t.Fatal("1st call should be cache miss")
	}
	if fp.CallCount() != 1 {
		t.Fatalf("expected 1 call on 1st call, got %d", fp.CallCount())
	}

	// 2nd call: Identical request with temperature 0.0 -> Cache hit!
	res2, err := svc.ChatCompletion(context.Background(), testRawKey, req, 0.0)
	if err != nil {
		t.Fatalf("unexpected error on 2nd call: %v", err)
	}
	if !res2.CacheHit {
		t.Fatal("2nd call MUST be a cache hit")
	}
	// Cost is 0 on cache hit
	if res2.CostMicros != 0 {
		t.Errorf("cache hit cost must be 0, got %d", res2.CostMicros)
	}
	// Provider was NOT called a 2nd time (remains 1)
	if fp.CallCount() != 1 {
		t.Fatalf("provider was called on cache hit: call count is %d", fp.CallCount())
	}

	// Budget Reserve was NOT called for 2nd request
	if len(mockBudget.settleCalls) != 1 {
		t.Errorf("expected exactly 1 settle call, got %d", len(mockBudget.settleCalls))
	}

	// Usage event recorded for cache hit
	if len(mockEvents.usageEvents) != 2 || !mockEvents.usageEvents[1].CacheHit {
		t.Errorf("expected 2 usage events with 2nd marked CacheHit, got %+v", mockEvents.usageEvents)
	}
}

func TestService_AuthErrors(t *testing.T) {
	svc, _, _, _, _ := setupServiceTestEnv()

	req := &llm.Request{
		Model: "fake-model",
	}

	// Invalid key
	_, err := svc.ChatCompletion(context.Background(), "invalid-key-format", req, 0.7)
	if !errors.Is(err, llm.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got %v", err)
	}

	// Model not allowed
	unallowedReq := &llm.Request{
		Model: "unallowed-model",
	}
	_, err = svc.ChatCompletion(context.Background(), testRawKey, unallowedReq, 0.7)
	if !errors.Is(err, llm.ErrModelNotAllowed) {
		t.Fatalf("expected ErrModelNotAllowed, got %v", err)
	}
}

func TestService_RouterError_ReleasesBudget(t *testing.T) {
	svc, mockBudget, fp, _, _ := setupServiceTestEnv()

	fp.SetError(errors.New("fatal provider error"))

	req := &llm.Request{
		ID:        "req-fail",
		Model:     "fake-model",
		MaxTokens: 100,
	}

	_, err := svc.ChatCompletion(context.Background(), testRawKey, req, 0.7)
	if err == nil {
		t.Fatal("expected router error, got nil")
	}

	if len(mockBudget.releaseCalls) != 1 || mockBudget.releaseCalls[0] != "res-default" {
		t.Errorf("expected budget release for reservation, got %+v", mockBudget.releaseCalls)
	}
}

func TestService_AC1D09_Streaming_Success(t *testing.T) {
	svc, mockBudget, fp, mockEvents, _ := setupServiceTestEnv()

	fp.SetStreamChunks([]llm.Chunk{
		{ID: "c1", Content: "Hello "},
		{ID: "c2", Content: "world!", Usage: &llm.Usage{InputTokens: 500, OutputTokens: 200}},
	})

	req := &llm.Request{
		ID:        "req-stream-1",
		Model:     "fake-model",
		MaxTokens: 500,
		Messages:  []llm.Message{{Role: "user", Content: "stream me"}},
	}

	res, err := svc.ChatCompletionStream(context.Background(), testRawKey, req)
	if err != nil {
		t.Fatalf("unexpected streaming error: %v", err)
	}

	if res.Provider != "fake" {
		t.Errorf("expected provider 'fake', got %s", res.Provider)
	}

	var collected string
	for chunk := range res.Stream {
		collected += chunk.Content
	}

	if collected != "Hello world!" {
		t.Errorf("expected 'Hello world!', got %q", collected)
	}

	time.Sleep(50 * time.Millisecond)

	if len(mockBudget.settleCalls) != 1 || mockBudget.settleCalls[0].actualMicros != 4500 {
		t.Errorf("expected settle with 4500 micros, got %+v", mockBudget.settleCalls)
	}

	if len(mockEvents.usageEvents) != 1 || mockEvents.usageEvents[0].CostMicros != 4500 {
		t.Errorf("expected usage event with 4500 micros, got %+v", mockEvents.usageEvents)
	}
}

func TestService_AC1D14_Streaming_Disconnect(t *testing.T) {
	svc, mockBudget, fp, _, _ := setupServiceTestEnv()

	fp.SetDelay(50 * time.Millisecond)
	fp.SetStreamChunks([]llm.Chunk{
		{ID: "c1", Content: "Chunk 1"},
		{ID: "c2", Content: "Chunk 2"},
	})

	ctx, cancel := context.WithCancel(context.Background())

	req := &llm.Request{
		ID:        "req-cancel",
		Model:     "fake-model",
		MaxTokens: 500,
	}

	res, err := svc.ChatCompletionStream(ctx, testRawKey, req)
	if err != nil {
		t.Fatalf("unexpected streaming error: %v", err)
	}

	<-res.Stream
	cancel()

	for range res.Stream {
	}

	time.Sleep(50 * time.Millisecond)

	if len(mockBudget.releaseCalls) != 1 {
		t.Errorf("expected 1 budget release call on cancel, got %d", len(mockBudget.releaseCalls))
	}
}

func TestService_AC1D14_ChatCompletion_ClientCancel_ReleaseNotAborted(t *testing.T) {
	svc, mockBudget, fp, _, _ := setupServiceTestEnv()

	fp.SetError(errors.New("provider temporary execution error"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := &llm.Request{
		ID:        "req-unary-cancel",
		Model:     "fake-model",
		MaxTokens: 500,
		Messages: []llm.Message{
			{Role: "user", Content: "Test unary cancel"},
		},
	}

	_, err := svc.ChatCompletion(ctx, testRawKey, req, 0.7)
	if err == nil {
		t.Fatal("expected error on canceled/failing request, got nil")
	}

	if len(mockBudget.releaseCalls) != 1 {
		t.Errorf("expected 1 release call using uncancelled context, got %d", len(mockBudget.releaseCalls))
	}
}

func TestService_PricingNotFound(t *testing.T) {
	svc, _, _, _, authMgr := setupServiceTestEnv()

	// Add key allowed for model without pricing
	keyHash := hashKey("lgw_9999999999abcdef9999999999abcdef", testPepper)
	snap := authMgr.GetSnapshot()
	snap.KeysByHash[keyHash] = &auth.KeyInfo{
		ID:            "key-nopricing",
		KeyHash:       keyHash,
		AllowedModels: []string{"unknown-model"},
		Active:        true,
	}

	req := &llm.Request{
		Model: "unknown-model",
	}

	_, err := svc.ChatCompletion(context.Background(), "lgw_9999999999abcdef9999999999abcdef", req, 0.7)
	if err == nil {
		t.Fatal("expected error for missing pricing, got nil")
	}

	_, err = svc.ChatCompletionStream(context.Background(), "lgw_9999999999abcdef9999999999abcdef", req)
	if err == nil {
		t.Fatal("expected error for missing pricing in stream, got nil")
	}
}
