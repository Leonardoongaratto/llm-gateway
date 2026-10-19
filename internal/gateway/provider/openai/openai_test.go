package openai_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/openai"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestOpenAIAdapter_Do_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-openai-key" {
			http.Error(w, `{"error":{"message":"unauthorized"}}`, http.StatusUnauthorized)
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
			"id": "chatcmpl_123",
			"object": "chat.completion",
			"created": 1677652288,
			"model": "gpt-4o",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "Hello from OpenAI!"
				},
				"finish_reason": "stop"
			}],
			"usage": {
				"prompt_tokens": 80,
				"completion_tokens": 40,
				"total_tokens": 120,
				"prompt_tokens_details": {
					"cached_tokens": 15
				}
			}
		}`))
	}))
	defer ts.Close()

	adapter := openai.New("test-openai-key",
		openai.WithBaseURL(ts.URL),
	)

	tempVal := 0.7
	req := &llm.Request{
		ID:          "req-oai-1",
		Model:       "gpt-4o",
		MaxTokens:   1000,
		Temperature: &tempVal,
		Messages: []llm.Message{
			{Role: "user", Content: "Hello OpenAI"},
		},
	}

	resp, err := adapter.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Provider != "openai" {
		t.Errorf("expected provider 'openai', got %s", resp.Provider)
	}
	if resp.Content != "Hello from OpenAI!" {
		t.Errorf("expected 'Hello from OpenAI!', got %s", resp.Content)
	}
	if resp.Usage.InputTokens != 80 || resp.Usage.OutputTokens != 40 || resp.Usage.CacheReadTokens != 15 {
		t.Errorf("unexpected usage: %+v", resp.Usage)
	}
}

func TestOpenAIAdapter_Do_Error400_NonRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"invalid model name","type":"invalid_request_error"}}`, http.StatusBadRequest)
	}))
	defer ts.Close()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "invalid"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, llm.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", err)
	}
	if provider.IsRetryable(err) {
		t.Errorf("400 error must NOT be retryable")
	}
}

func TestOpenAIAdapter_Do_Error500_Retryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"server error"}}`, http.StatusInternalServerError)
	}))
	defer ts.Close()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, llm.ErrRetryable) {
		t.Errorf("expected ErrRetryable, got %v", err)
	}
	if !provider.IsRetryable(err) {
		t.Errorf("500 error must be retryable")
	}
}

func TestOpenAIAdapter_Options(t *testing.T) {
	adapter := openai.New("custom-key",
		openai.WithName("custom-provider"),
		openai.WithHTTPClient(&http.Client{}),
	)
	if adapter.Name() != "custom-provider" {
		t.Errorf("expected custom-provider, got %s", adapter.Name())
	}
}

func TestOpenAIAdapter_DoStream_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("expected flusher")
		}

		sseData := []string{
			`data: {"id":"chatcmpl_1","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello "},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"chatcmpl_1","choices":[{"index":0,"delta":{"content":"world!"},"finish_reason":null}]}` + "\n\n",
			`data: {"id":"chatcmpl_1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3}}}` + "\n\n",
			`data: [DONE]` + "\n\n",
		}

		for _, msg := range sseData {
			_, _ = w.Write([]byte(msg))
			flusher.Flush()
		}
	}))
	defer ts.Close()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	stream, err := adapter.DoStream(context.Background(), &llm.Request{Model: "gpt-4o"})
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

	if content != "Hello world!" {
		t.Errorf("expected 'Hello world!', got %q", content)
	}
	if finalUsage == nil || finalUsage.InputTokens != 10 || finalUsage.OutputTokens != 5 || finalUsage.CacheReadTokens != 3 {
		t.Errorf("unexpected final usage: %+v", finalUsage)
	}
}

func TestOpenAIAdapter_DoStream_HttpError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"service unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	_, err := adapter.DoStream(context.Background(), &llm.Request{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !provider.IsRetryable(err) {
		t.Errorf("503 error must be retryable")
	}
}

func TestOpenAIAdapter_Do_NetworkError_IsRetryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	ts.Close() // immediately close to force network error

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error on closed server, got nil")
	}
	if !provider.IsRetryable(err) {
		t.Errorf("network connection error must be retryable UpstreamError")
	}
}

func TestOpenAIAdapter_Do_TruncatedJSON_Returns502Retryable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":`))
	}))
	defer ts.Close()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	_, err := adapter.Do(context.Background(), &llm.Request{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error on truncated JSON, got nil")
	}
	var upstreamErr *provider.UpstreamError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("expected UpstreamError, got %T: %v", err, err)
	}
	if upstreamErr.StatusCode != http.StatusBadGateway {
		t.Errorf("expected status 502, got %d", upstreamErr.StatusCode)
	}
	if !upstreamErr.Retryable {
		t.Errorf("expected truncated JSON error to be retryable")
	}
}

func TestOpenAIAdapter_Do_BodyReadCancel_ReturnsCanceled(t *testing.T) {
	blockCh := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-blockCh
	}))
	defer func() {
		close(blockCh)
		ts.Close()
	}()

	adapter := openai.New("key", openai.WithBaseURL(ts.URL))
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := adapter.Do(ctx, &llm.Request{Model: "gpt-4o"})
	if err == nil {
		t.Fatal("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
	if provider.IsRetryable(err) {
		t.Errorf("canceled context error must NOT be retryable UpstreamError")
	}
}
