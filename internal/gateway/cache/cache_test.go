package cache_test

import (
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestCache_Eligibility(t *testing.T) {
	tests := []struct {
		name        string
		temperature float64
		stream      bool
		want        bool
	}{
		{name: "eligible temp 0 non-stream", temperature: 0.0, stream: false, want: true},
		{name: "ineligible temp > 0", temperature: 0.7, stream: false, want: false},
		{name: "ineligible stream true", temperature: 0.0, stream: true, want: false},
		{name: "ineligible stream true temp > 0", temperature: 0.5, stream: true, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cache.IsEligible(tc.temperature, tc.stream)
			if got != tc.want {
				t.Fatalf("expected eligible=%v, got %v", tc.want, got)
			}
		})
	}
}

func TestCache_ComputeKey(t *testing.T) {
	req1 := &llm.Request{
		Model: "gpt-4o",
		Messages: []llm.Message{
			{Role: "user", Content: "Hello world"},
		},
	}
	req2 := &llm.Request{
		Model: "gpt-4o",
		Messages: []llm.Message{
			{Role: "user", Content: "Hello world"},
		},
	}
	reqDiff := &llm.Request{
		Model: "gpt-4o",
		Messages: []llm.Message{
			{Role: "user", Content: "Different message"},
		},
	}

	key1 := cache.ComputeKey("key-1", req1)
	key2 := cache.ComputeKey("key-1", req2)
	keyDiff := cache.ComputeKey("key-1", reqDiff)
	keyDiffKey := cache.ComputeKey("key-2", req1)

	if key1 != key2 {
		t.Errorf("expected deterministic keys: %s vs %s", key1, key2)
	}
	if key1 == keyDiff {
		t.Errorf("different content should produce different key: %s", key1)
	}
	if key1 == keyDiffKey {
		t.Errorf("different keyID should produce different key: %s", key1)
	}
}

func TestCache_AC1D08_SecondCallHit(t *testing.T) {
	c := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))

	req := &llm.Request{
		Model: "claude-3-5-sonnet",
		Messages: []llm.Message{
			{Role: "user", Content: "Explain deterministic cache"},
		},
	}
	cacheKey := cache.ComputeKey("k-1", req)

	// Miss on 1st call
	_, hit := c.Get(cacheKey)
	if hit {
		t.Fatal("expected cache miss on 1st call")
	}

	// 1st call saves response to cache
	resp := &llm.Response{
		ID:       "resp-1",
		Model:    "claude-3-5-sonnet",
		Provider: "anthropic",
		Content:  "Deterministic caching explanation...",
		Usage: llm.Usage{
			InputTokens:  20,
			OutputTokens: 50,
		},
	}
	c.Set(cacheKey, resp)

	// 2nd call: cache hit!
	cachedResp, hit := c.Get(cacheKey)
	if !hit {
		t.Fatal("expected cache hit on 2nd call")
	}
	if !cachedResp.Cached {
		t.Errorf("expected Cached=true on response, got false")
	}
	if cachedResp.Content != resp.Content {
		t.Errorf("unexpected cached content: %s", cachedResp.Content)
	}
}

func TestCache_TTLExpiration(t *testing.T) {
	c := cache.New(cache.WithCapacity(10), cache.WithTTL(30*time.Millisecond))

	c.Set("k-exp", &llm.Response{Content: "expiring"})

	// Immediate get succeeds
	_, hit := c.Get("k-exp")
	if !hit {
		t.Fatal("expected hit immediately")
	}

	// Wait for TTL to expire
	time.Sleep(50 * time.Millisecond)

	_, hit = c.Get("k-exp")
	if hit {
		t.Fatal("expected item to be expired after TTL")
	}
}

func TestCache_LRUEviction(t *testing.T) {
	// Capacity of 2
	c := cache.New(cache.WithCapacity(2), cache.WithTTL(1*time.Minute))

	c.Set("k1", &llm.Response{ID: "1"})
	c.Set("k2", &llm.Response{ID: "2"})

	// Access k1 to make k2 least recently used
	_, hit := c.Get("k1")
	if !hit {
		t.Fatal("expected hit for k1")
	}

	// Insert k3 -> causes eviction of k2
	c.Set("k3", &llm.Response{ID: "3"})

	// k1 should still exist
	_, hit = c.Get("k1")
	if !hit {
		t.Error("k1 should still exist in cache")
	}

	// k2 should have been evicted
	_, hit = c.Get("k2")
	if hit {
		t.Error("k2 should have been evicted by LRU policy")
	}

	// k3 should exist
	_, hit = c.Get("k3")
	if !hit {
		t.Error("k3 should exist in cache")
	}
}
