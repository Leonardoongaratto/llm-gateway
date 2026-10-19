package anthropic_test

import (
	"context"
	"encoding/json"
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

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		if temp, ok := body["temperature"].(float64); !ok || temp != 0.7 {
			t.Errorf("expected temperature 0.7 in upstream body, got %v", body["temperature"])
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

	tempVal := 0.7
	req := &llm.Request{
		ID:          "req-ant-1",
		Model:       "claude-3-5-sonnet",
		MaxTokens:   1000,
		Temperature: &tempVal,
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

func TestAnthropicAdapter_DoStream_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		sseData := []string{
			`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_1","model":"claude","usage":{"input_tokens":15,"cache_read_input_tokens":5,"cache_creation_input_tokens":2}}}` + "\n\n",
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello "}}` + "\n\n",
			`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"stream!"}}` + "\n\n",
			`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":10}}` + "\n\n",
			`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n",
		}

		for _, msg := range sseData {
			_, _ = w.Write([]byte(msg))
			flusher.Flush()
		}
	}))
	defer ts.Close()

	adapter := anthropic.New("key", anthropic.WithBaseURL(ts.URL))
	stream, err := adapter.DoStream(context.Background(), &llm.Request{Model: "claude-3-5-sonnet"})
	if err != nil {
		t.Fatalf("unexpected streaming error: %v", err)
	}

	var content string
	var finalUsage *llm.Usage
	for chunk := range stream {
		content += chunk.Content
		if chunk.Usage != nil {
			finalUsage = chunk.Usage
		}
	}

	if content != "Hello stream!" {
		t.Errorf("expected 'Hello stream!', got %q", content)
	}
	if finalUsage == nil || finalUsage.InputTokens != 15 || finalUsage.OutputTokens != 10 || finalUsage.CacheReadTokens != 5 || finalUsage.CacheWriteTokens != 2 {
		t.Errorf("unexpected final usage: %+v", finalUsage)
	}
}

func TestAnthropicAdapter_DoStream_HttpError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"rate limited"}}`, http.StatusTooManyRequests)
	}))
	defer ts.Close()

	adapter := anthropic.New("key", anthropic.WithBaseURL(ts.URL))
	_, err := adapter.DoStream(context.Background(), &llm.Request{Model: "claude"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !provider.IsRetryable(err) {
		t.Errorf("429 error must be retryable")
	}
}

func TestAnthropicAdapter_Do_NetworkError_IsRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	ts.Close() // immediately close to force network error

	adapter := anthropic.New("key", anthropic.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "claude"})
	if err == nil {
		t.Fatal("expected error on closed server, got nil")
	}
	if !provider.IsRetryable(err) {
		t.Errorf("network connection error must be retryable UpstreamError")
	}
}
