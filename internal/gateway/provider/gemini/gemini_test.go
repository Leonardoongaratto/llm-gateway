package gemini_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/gemini"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestGeminiAdapter_Do_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-gemini-key" {
			http.Error(w, `{"error":{"message":"unauthorized"}}`, http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl_gemini_123",
			"object": "chat.completion",
			"created": 1677652288,
			"model": "gemini-1.5-flash",
			"choices": [{
				"index": 0,
				"message": {
					"role": "assistant",
					"content": "Hello from Gemini!"
				},
				"finish_reason": "stop"
			}],
			"usage": {
				"prompt_tokens": 12,
				"completion_tokens": 8,
				"total_tokens": 20
			}
		}`))
	}))
	defer ts.Close()

	adapter := gemini.New("test-gemini-key",
		gemini.WithBaseURL(ts.URL),
	)

	if adapter.Name() != "gemini" {
		t.Errorf("expected provider name 'gemini', got %s", adapter.Name())
	}

	req := &llm.Request{
		ID:        "req-gem-1",
		Model:     "gemini-1.5-flash",
		MaxTokens: 500,
		Messages: []llm.Message{
			{Role: "user", Content: "Hello Gemini"},
		},
	}

	resp, err := adapter.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Provider != "gemini" {
		t.Errorf("expected provider 'gemini', got %s", resp.Provider)
	}
	if resp.Content != "Hello from Gemini!" {
		t.Errorf("expected 'Hello from Gemini!', got %s", resp.Content)
	}
	if resp.Usage.InputTokens != 12 || resp.Usage.OutputTokens != 8 {
		t.Errorf("unexpected usage: %+v", resp.Usage)
	}

	ch, err := adapter.DoStream(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error on DoStream: %v", err)
	}
	for range ch {
	}
}
