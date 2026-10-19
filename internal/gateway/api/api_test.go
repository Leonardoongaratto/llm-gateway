package api_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/api"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/budgetclient"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/openai"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

type mockBudgetClient struct {
	reserveFunc func(ctx context.Context, keyID string, maxMicros int64, requestID string) (string, int64, error)
}

func (m *mockBudgetClient) Reserve(ctx context.Context, keyID string, maxMicros int64, requestID string) (string, int64, error) {
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, keyID, maxMicros, requestID)
	}
	return "res-api-1", 5_000_000, nil
}

func (m *mockBudgetClient) SettleAsync(_ string, _ int64)             {}
func (m *mockBudgetClient) Release(_ context.Context, _ string) error { return nil }

type mockEventPublisher struct{}

func (m *mockEventPublisher) PublishUsage(_ context.Context, _ *eventsv1.UsageRecorded)       {}
func (m *mockEventPublisher) PublishFallback(_ context.Context, _ *eventsv1.ProviderFallback) {}

const (
	testPepper = "api-pepper"
	testRawKey = "lgw_1234567890abcdef1234567890abcdef"
)

func hashKey(raw, pepper string) string {
	h := sha256.Sum256([]byte(raw + pepper))
	return hex.EncodeToString(h[:])
}

func setupAPITestServer() (*httptest.Server, *mockBudgetClient, *fake.Provider) {
	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-api-1",
				Name:          "API Key",
				KeyHash:       keyHash,
				AllowedModels: []string{"fake-model", "claude-3-5-sonnet", "gpt-4o", "fast"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"fake-model": {
				Model:               "fake-model",
				Provider:            "fake",
				InputPerMTokMicros:  3_000_000,
				OutputPerMTokMicros: 15_000_000,
			},
			"gpt-4o": {
				Model:               "gpt-4o",
				Provider:            "fake",
				InputPerMTokMicros:  5_000_000,
				OutputPerMTokMicros: 15_000_000,
			},
		},
		LogicalModels: map[string][]string{
			"fast": {"fake-model"},
		},
	})

	mockBudget := &mockBudgetClient{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))
	r := router.New()
	fp := fake.New(
		fake.WithName("fake"),
		fake.WithContent("Fake answer from gateway"),
		fake.WithUsage(llm.Usage{InputTokens: 1000, OutputTokens: 500}),
	)
	r.Register("fake-model", fp)
	r.Register("gpt-4o", fp)
	r.Register("fast", fp)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: &mockEventPublisher{},
		LocalCache:     localCache,
		Router:         r,
	})

	handler := api.NewHandler(svc, authMgr)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	return httptest.NewServer(mux), mockBudget, fp
}

func TestAPI_AC1D01_ChatCompletions_Success(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"Hi"}],"temperature":0.7,"max_tokens":500}`
	httpReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	// Verify headers (AC-1D-01)
	if resp.Header.Get("X-LGW-Cost-Micros") != "10500" {
		t.Errorf("expected X-LGW-Cost-Micros: 10500, got %s", resp.Header.Get("X-LGW-Cost-Micros"))
	}
	if resp.Header.Get("X-LGW-Provider") != "fake" {
		t.Errorf("expected X-LGW-Provider: fake, got %s", resp.Header.Get("X-LGW-Provider"))
	}
	if resp.Header.Get("X-LGW-Cache") != "miss" {
		t.Errorf("expected X-LGW-Cache: miss, got %s", resp.Header.Get("X-LGW-Cache"))
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Errorf("missing X-Request-Id header")
	}

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	choices, ok := parsed["choices"].([]interface{})
	if !ok || len(choices) == 0 {
		t.Fatalf("expected choices in response: %+v", parsed)
	}
	firstChoice, ok := choices[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected choice map: %+v", choices[0])
	}
	msg, ok := firstChoice["message"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected message map: %+v", firstChoice)
	}
	if msg["content"] != "Fake answer from gateway" {
		t.Errorf("unexpected content: %v", msg["content"])
	}
}

func TestAPI_AC1D02_Messages_Success(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"Hi Anthropic"}],"max_tokens":500}`
	httpReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/messages", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpReq.Header.Set("x-api-key", testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	if resp.Header.Get("X-LGW-Provider") != "fake" {
		t.Errorf("expected X-LGW-Provider: fake, got %s", resp.Header.Get("X-LGW-Provider"))
	}

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if parsed["type"] != "message" || parsed["role"] != "assistant" {
		t.Errorf("unexpected Anthropic response structure: %+v", parsed)
	}
}

func TestAPI_AC1D04_BudgetExceeded_Returns402(t *testing.T) {
	ts, mockBudget, _ := setupAPITestServer()
	defer ts.Close()

	mockBudget.reserveFunc = func(_ context.Context, _ string, _ int64, _ string) (string, int64, error) {
		return "", 0, llm.ErrBudgetExceeded
	}

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"Hi"}]}`
	httpReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("expected 402 Payment Required, got %d", resp.StatusCode)
	}

	var errResp map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	errObj, ok := errResp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error object: %+v", errResp)
	}
	if errObj["code"] != "budget_exceeded" {
		t.Errorf("expected error code budget_exceeded, got %v", errObj["code"])
	}
}

func TestAPI_AC1D08_CacheHit_TemperatureZero(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"deterministic"}],"temperature":0.0}`

	// 1st call -> miss
	httpReq1, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq1.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq1.Header.Set("Content-Type", "application/json")
	resp1, err := http.DefaultClient.Do(httpReq1)
	if err != nil {
		t.Fatalf("call 1 failed: %v", err)
	}
	resp1.Body.Close()
	if resp1.Header.Get("X-LGW-Cache") != "miss" {
		t.Errorf("call 1 should be miss, got %s", resp1.Header.Get("X-LGW-Cache"))
	}

	// 2nd call -> hit!
	httpReq2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq2.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(httpReq2)
	if err != nil {
		t.Fatalf("call 2 failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.Header.Get("X-LGW-Cache") != "hit" {
		t.Errorf("call 2 MUST be hit, got %s", resp2.Header.Get("X-LGW-Cache"))
	}
	if resp2.Header.Get("X-LGW-Cost-Micros") != "0" {
		t.Errorf("cache hit cost must be 0, got %s", resp2.Header.Get("X-LGW-Cost-Micros"))
	}
}

func TestAPI_AC1D09_ChatCompletions_StreamingSSE(t *testing.T) {
	ts, _, fp := setupAPITestServer()
	defer ts.Close()

	fp.SetStreamChunks([]llm.Chunk{
		{ID: "c1", Content: "Hello "},
		{ID: "c2", Content: "streaming ", FinishReason: "stop", Usage: &llm.Usage{InputTokens: 100, OutputTokens: 50}},
	})

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"stream"}],"stream":true}`
	httpReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("streaming request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected text/event-stream Content-Type, got %s", resp.Header.Get("Content-Type"))
	}

	scanner := bufio.NewScanner(resp.Body)
	var events []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			events = append(events, line)
		}
	}

	if len(events) < 3 {
		t.Fatalf("expected at least 3 SSE data lines, got %d: %+v", len(events), events)
	}
	lastEvent := events[len(events)-1]
	if lastEvent != "data: [DONE]" {
		t.Errorf("expected last SSE event to be 'data: [DONE]', got %s", lastEvent)
	}
}

func TestAPI_AC1D10_AuthErrors_401_and_403(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	// 401 on bad key
	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"test"}]}`
	httpReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq.Header.Set("Authorization", "Bearer invalid-key")
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	// 403 on unallowed model
	badModelBody := `{"model":"unallowed-model","messages":[{"role":"user","content":"test"}]}`
	httpReq2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(badModelBody))
	httpReq2.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(httpReq2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden, got %d", resp2.StatusCode)
	}
}

func TestAPI_AC1D11_RateLimitError_Returns429WithRetryAfter(t *testing.T) {
	ts, mockBudget, _ := setupAPITestServer()
	defer ts.Close()

	mockBudget.reserveFunc = func(_ context.Context, _ string, _ int64, _ string) (string, int64, error) {
		return "", 0, &budgetclient.RateLimitError{RetryAfterSeconds: 45}
	}

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"test"}]}`
	httpReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("expected 429 Too Many Requests, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "45" {
		t.Errorf("expected Retry-After: 45, got %s", resp.Header.Get("Retry-After"))
	}
}

func TestAPI_AC1D16_BudgetUnavailable_Returns503(t *testing.T) {
	ts, mockBudget, _ := setupAPITestServer()
	defer ts.Close()

	mockBudget.reserveFunc = func(_ context.Context, _ string, _ int64, _ string) (string, int64, error) {
		return "", 0, llm.ErrBudgetUnavailable
	}

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"test"}]}`
	httpReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable, got %d", resp.StatusCode)
	}
}

func TestAPI_Models_List(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	httpReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode models list: %v", err)
	}

	if parsed["object"] != "list" {
		t.Errorf("expected object: list, got %v", parsed["object"])
	}
	dataList, ok := parsed["data"].([]interface{})
	if !ok || len(dataList) == 0 {
		t.Errorf("expected models in data list, got %v", parsed["data"])
	}
}

func TestAPI_AC1D15_Stream_ProviderFailureAfterFirstByte(t *testing.T) {
	ts, _, fp := setupAPITestServer()
	defer ts.Close()

	fp.SetStreamChunks([]llm.Chunk{
		{ID: "c1", Content: "Hello partial world"},
		{ID: "c2", Err: errors.New("provider stream network reset")},
	})

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"Stream failing test"}],"stream":true}`
	httpReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("streaming request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK initial response, got %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	bodyStr := string(bodyBytes)

	if !strings.Contains(bodyStr, "Hello partial world") {
		t.Errorf("expected initial chunk in response, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "event: error") {
		t.Errorf("expected event: error emitted on mid-stream failure, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "stream error from upstream provider") {
		t.Errorf("expected error message in payload, got: %s", bodyStr)
	}
	if strings.Contains(bodyStr, "data: [DONE]") {
		t.Errorf("expected NO [DONE] on mid-stream failure, got: %s", bodyStr)
	}
}

func TestAPI_Messages_Streaming_SSE(t *testing.T) {
	ts, _, fp := setupAPITestServer()
	defer ts.Close()

	fp.SetStreamChunks([]llm.Chunk{
		{ID: "c1", Content: "Anthropic stream content"},
		{ID: "c2", FinishReason: "stop", Usage: &llm.Usage{InputTokens: 10, OutputTokens: 5}},
	})

	reqBody := `{"model":"fake-model","messages":[{"role":"user","content":"Stream Anthropic"}],"stream":true}`
	httpReq, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/messages", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	httpReq.Header.Set("x-api-key", testRawKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("streaming request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("expected text/event-stream Content-Type, got %s", ct)
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	bodyStr := string(bodyBytes)

	if !strings.Contains(bodyStr, "event: message_start") {
		t.Errorf("expected message_start event, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "event: content_block_delta") {
		t.Errorf("expected content_block_delta event, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Anthropic stream content") {
		t.Errorf("expected delta content, got: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "event: message_stop") {
		t.Errorf("expected message_stop event, got: %s", bodyStr)
	}
}

func TestAPI_Models_UnauthorizedWithGarbageKey(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	httpReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
	httpReq.Header.Set("Authorization", "Bearer garbage-virtual-key")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for garbage key, got %d", resp.StatusCode)
	}
}

func TestAPI_Models_FilteredByAllowedModels(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	httpReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/models", nil)
	httpReq.Header.Set("Authorization", "Bearer "+testRawKey)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var parsed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("failed to decode models list: %v", err)
	}
	dataList, ok := parsed["data"].([]interface{})
	if !ok || len(dataList) == 0 {
		t.Fatalf("expected models in data list, got %v", parsed["data"])
	}
}

func TestAPI_AC1D05_ProviderFallbackHeader(t *testing.T) {
	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-api-1",
				AllowedModels: []string{"smart-route", "fake-primary", "fake-fallback"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"fake-primary": {
				Model:               "fake-primary",
				Provider:            "p1",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
			"fake-fallback": {
				Model:               "fake-fallback",
				Provider:            "p2",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
		},
		LogicalModels: map[string][]string{
			"smart-route": {"fake-primary", "fake-fallback"},
		},
	})

	mockBudget := &mockBudgetClient{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))
	r := router.New()
	p1 := fake.New(
		fake.WithName("p1"),
		fake.WithError(&provider.UpstreamError{StatusCode: 503, Message: "primary down", Retryable: true}),
	)
	p2 := fake.New(
		fake.WithName("p2"),
		fake.WithContent("Answer from fallback provider"),
		fake.WithUsage(llm.Usage{InputTokens: 10, OutputTokens: 10}),
	)
	r.Register("fake-primary", p1)
	r.Register("fake-fallback", p2)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: &mockEventPublisher{},
		LocalCache:     localCache,
		Router:         r,
	})

	handler := api.NewHandler(svc, authMgr)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := bytes.NewBufferString(`{"model":"smart-route","messages":[{"role":"user","content":"hello"}]}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", body)
	req.Header.Set("Authorization", "Bearer "+testRawKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after fallback, got %d", resp.StatusCode)
	}
	provHeader := resp.Header.Get("X-LGW-Provider")
	if provHeader != "p2" {
		t.Errorf("expected X-LGW-Provider to be 'p2', got %q", provHeader)
	}
	if p1.CallCount() != 1 {
		t.Errorf("expected p1 to be called once, got %d", p1.CallCount())
	}
	if p2.CallCount() != 1 {
		t.Errorf("expected p2 to be called once, got %d", p2.CallCount())
	}
}

func TestAPI_AC1D06_NoFallbackOn400(t *testing.T) {
	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-api-1",
				AllowedModels: []string{"smart-route", "fake-primary", "fake-fallback"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"fake-primary": {
				Model:               "fake-primary",
				Provider:            "p1",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
		},
		LogicalModels: map[string][]string{
			"smart-route": {"fake-primary", "fake-fallback"},
		},
	})

	mockBudget := &mockBudgetClient{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))
	r := router.New()
	p1 := fake.New(
		fake.WithName("p1"),
		fake.WithError(&provider.UpstreamError{StatusCode: 400, Message: "bad prompt", Retryable: false}),
	)
	p2 := fake.New(
		fake.WithName("p2"),
		fake.WithContent("should not be called"),
	)
	r.Register("fake-primary", p1)
	r.Register("fake-fallback", p2)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: &mockEventPublisher{},
		LocalCache:     localCache,
		Router:         r,
	})

	handler := api.NewHandler(svc, authMgr)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := bytes.NewBufferString(`{"model":"smart-route","messages":[{"role":"user","content":"bad input"}]}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", body)
	req.Header.Set("Authorization", "Bearer "+testRawKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request without fallback, got %d", resp.StatusCode)
	}
	if p1.CallCount() != 1 {
		t.Errorf("expected p1 to be called once, got %d", p1.CallCount())
	}
	if p2.CallCount() != 0 {
		t.Errorf("expected p2 not to be called, got %d calls", p2.CallCount())
	}
}

func assertErrorPayloadFormat(t *testing.T, endpoint string, body map[string]interface{}) {
	t.Helper()
	if endpoint == "/v1/messages" {
		if body["type"] != "error" {
			t.Errorf("expected Anthropic error type, got: %v", body)
		}
		return
	}
	if body["error"] == nil {
		t.Errorf("expected OpenAI error field, got: %v", body)
	}
}

type invalidRequestTestCase struct {
	name        string
	endpoint    string
	payload     string
	expectedMsg string
}

func executeInvalidRequestBodyTest(t *testing.T, client *http.Client, url string, tc invalidRequestTestCase) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+tc.endpoint, strings.NewReader(tc.payload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+testRawKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	assertErrorPayloadFormat(t, tc.endpoint, body)
	if tc.expectedMsg != "" {
		errMap, _ := body["error"].(map[string]interface{})
		if msg, _ := errMap["message"].(string); msg != tc.expectedMsg {
			t.Errorf("expected message %q, got %q", tc.expectedMsg, msg)
		}
	}
}

func TestAPI_InvalidRequest_MalformedAndMissingFields(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	cases := []invalidRequestTestCase{
		{"chat malformed json", "/v1/chat/completions", `{"model": "fake-model", invalid`, "invalid json request body"},
		{"messages malformed json", "/v1/messages", `{"model": "fake-model", invalid`, "invalid json request body"},
		{"chat missing model", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}]}`, "model is required"},
		{"messages missing model", "/v1/messages", `{"messages":[{"role":"user","content":"hi"}]}`, "model is required"},
		{"chat missing messages", "/v1/chat/completions", `{"model":"fake-model"}`, "messages is required and must not be empty"},
		{"messages missing messages", "/v1/messages", `{"model":"fake-model"}`, "messages is required and must not be empty"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executeInvalidRequestBodyTest(t, client, ts.URL, tc)
		})
	}
}

func TestAPI_MaxBytesError_Returns413(t *testing.T) {
	ts, _, _ := setupAPITestServer()
	defer ts.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	largePayload := `{"model":"fake-model","messages":[{"role":"user","content":"` + strings.Repeat("a", 10<<20+100) + `"}]}`

	endpoints := []string{"/v1/chat/completions", "/v1/messages"}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, ts.URL+ep, strings.NewReader(largePayload))
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+testRawKey)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusRequestEntityTooLarge {
				t.Errorf("expected 413 Request Entity Too Large, got %d", resp.StatusCode)
			}

			var body map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&body)
			assertErrorPayloadFormat(t, ep, body)
		})
	}
}

func TestAPI_UpstreamTruncatedJSON_Returns502(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":`))
	}))
	defer upstream.Close()

	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-api-1",
				AllowedModels: []string{"gpt-truncated"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"gpt-truncated": {
				Model:               "gpt-truncated",
				Provider:            "openai",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
		},
		LogicalModels: map[string][]string{
			"gpt-truncated": {"gpt-truncated"},
		},
	})

	mockBudget := &mockBudgetClient{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))
	r := router.New()
	p := openai.New("key", openai.WithBaseURL(upstream.URL))
	r.Register("gpt-truncated", p)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: &mockEventPublisher{},
		LocalCache:     localCache,
		Router:         r,
	})

	handler := api.NewHandler(svc, authMgr)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := bytes.NewBufferString(`{"model":"gpt-truncated","messages":[{"role":"user","content":"hello"}]}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", body)
	req.Header.Set("Authorization", "Bearer "+testRawKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway for upstream truncated JSON, got %d", resp.StatusCode)
	}

	var respBody map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&respBody)
	errObj, ok := respBody["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error object, got: %v", respBody)
	}
	if errObj["code"] != "all_providers_failed" {
		t.Errorf("expected error code 'all_providers_failed', got %v", errObj["code"])
	}
}

func TestAPI_UpstreamTruncatedJSON_FallbackToSecondary(t *testing.T) {
	primaryUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":`))
	}))
	defer primaryUpstream.Close()

	authMgr, _ := auth.NewManager(testPepper, nil)
	keyHash := hashKey(testRawKey, testPepper)

	authMgr.UpdateSnapshot(&auth.Snapshot{
		Version: 1,
		KeysByHash: map[string]*auth.KeyInfo{
			keyHash: {
				ID:            "key-api-1",
				AllowedModels: []string{"smart-route", "primary-model", "fallback-model"},
				Active:        true,
			},
		},
		Pricing: map[string]*auth.PricingInfo{
			"primary-model": {
				Model:               "primary-model",
				Provider:            "primary-prov",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
			"fallback-model": {
				Model:               "fallback-model",
				Provider:            "fallback-prov",
				InputPerMTokMicros:  1_000_000,
				OutputPerMTokMicros: 2_000_000,
			},
		},
		LogicalModels: map[string][]string{
			"smart-route": {"primary-model", "fallback-model"},
		},
	})

	mockBudget := &mockBudgetClient{}
	localCache := cache.New(cache.WithCapacity(100), cache.WithTTL(5*time.Minute))
	r := router.New()
	p1 := openai.New("key", openai.WithName("primary-prov"), openai.WithBaseURL(primaryUpstream.URL))
	p2 := fake.New(
		fake.WithName("fallback-prov"),
		fake.WithContent("Answer from fallback"),
		fake.WithUsage(llm.Usage{InputTokens: 10, OutputTokens: 10}),
	)
	r.Register("primary-model", p1)
	r.Register("fallback-model", p2)

	svc := core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   mockBudget,
		EventPublisher: &mockEventPublisher{},
		LocalCache:     localCache,
		Router:         r,
	})

	handler := api.NewHandler(svc, authMgr)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body := bytes.NewBufferString(`{"model":"smart-route","messages":[{"role":"user","content":"hello"}]}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", body)
	req.Header.Set("Authorization", "Bearer "+testRawKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after fallback from truncated JSON, got %d", resp.StatusCode)
	}

	provHeader := resp.Header.Get("X-LGW-Provider")
	if provHeader != "fallback-prov" {
		t.Errorf("expected X-LGW-Provider to be 'fallback-prov', got %q", provHeader)
	}
}
