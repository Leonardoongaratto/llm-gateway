package anthropic_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/anthropic"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestAnthropicAdapter_Do_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "test-anthropic-key" {
			http.Error(w, `{"error":{"message":"unauthorized"}}`, http.StatusUnauthorized)
			return
		}
		if r.Header.Get("anthropic-version") != "2023-06-01" {
			http.Error(w, `{"error":{"message":"bad version"}}`, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "msg_123",
			"type": "message",
			"role": "assistant",
			"model": "claude-3-5-sonnet-20241022",
			"content": [
				{"type": "text", "text": "Hello from Claude!"}
			],
			"usage": {
				"input_tokens": 100,
				"output_tokens": 50,
				"cache_read_input_tokens": 20,
				"cache_creation_input_tokens": 10
			}
		}`))
	}))
	defer ts.Close()

	adapter := anthropic.New("test-anthropic-key",
		anthropic.WithBaseURL(ts.URL),
	)

	req := &llm.Request{
		ID:        "req-ant-1",
		Model:     "claude-3-5-sonnet",
		MaxTokens: 1000,
		Messages: []llm.Message{
			{Role: "user", Content: "Hello"},
		},
	}

	resp, err := adapter.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Provider != "anthropic" {
		t.Errorf("expected provider 'anthropic', got %s", resp.Provider)
	}
	if resp.Content != "Hello from Claude!" {
		t.Errorf("expected 'Hello from Claude!', got %s", resp.Content)
	}
	if resp.Usage.InputTokens != 100 || resp.Usage.OutputTokens != 50 || resp.Usage.CacheReadTokens != 20 || resp.Usage.CacheWriteTokens != 10 {
		t.Errorf("unexpected usage: %+v", resp.Usage)
	}
}

func TestAnthropicAdapter_Do_Error400_NonRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens is required"}}`, http.StatusBadRequest)
	}))
	defer ts.Close()

	adapter := anthropic.New("key", anthropic.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "claude"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, llm.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
	if provider.IsRetryable(err) {
		t.Errorf("error 400 must NOT be retryable, got retryable=true")
	}
}

func TestAnthropicAdapter_Do_Error500_Retryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"overloaded"}}`, http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	adapter := anthropic.New("key", anthropic.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "claude"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, llm.ErrRetryable) {
		t.Errorf("expected ErrRetryable, got %v", err)
	}
	if !provider.IsRetryable(err) {
		t.Errorf("503 error must be retryable")
	}
}
