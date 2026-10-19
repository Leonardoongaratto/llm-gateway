package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/budgetclient"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// Handler handles HTTP requests for OpenAI and Anthropic compatible gateway endpoints.
type Handler struct {
	svc     *core.Service
	authMgr *auth.Manager
}

// NewHandler creates a new HTTP api Handler.
func NewHandler(svc *core.Service, authMgr *auth.Manager) *Handler {
	return &Handler{
		svc:     svc,
		authMgr: authMgr,
	}
}

// RegisterRoutes registers /v1/chat/completions, /v1/messages, and /v1/models routes on the mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/chat/completions", h.handleChatCompletions)
	mux.HandleFunc("/v1/messages", h.handleMessages)
	mux.HandleFunc("/v1/models", h.handleModels)
}

func extractKey(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	}
	if apiKey := r.Header.Get("x-api-key"); apiKey != "" {
		return strings.TrimSpace(apiKey)
	}
	return ""
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

func mapErrorDetails(err error) (int, string, string, string, string) {
	var rateLimitErr *budgetclient.RateLimitError
	if errors.As(err, &rateLimitErr) {
		return http.StatusTooManyRequests, "rate limit exceeded", "rate_limit_error", "rate_limited", strconv.FormatInt(rateLimitErr.RetryAfterSeconds, 10)
	}

	if errors.Is(err, llm.ErrInvalidKey) {
		return http.StatusUnauthorized, "invalid virtual key", "invalid_request_error", "invalid_key", ""
	}
	if errors.Is(err, llm.ErrModelNotAllowed) {
		return http.StatusForbidden, "model not allowed for virtual key", "permission_error", "model_not_allowed", ""
	}
	if errors.Is(err, llm.ErrBudgetExceeded) {
		return http.StatusPaymentRequired, "budget exceeded", "budget_error", "budget_exceeded", ""
	}
	if errors.Is(err, llm.ErrRateLimited) {
		return http.StatusTooManyRequests, "rate limit exceeded", "rate_limit_error", "rate_limited", "60"
	}
	if errors.Is(err, llm.ErrAllProvidersFailed) {
		return http.StatusBadGateway, "all providers failed in chain", "api_error", "all_providers_failed", ""
	}
	if errors.Is(err, llm.ErrBudgetUnavailable) {
		return http.StatusServiceUnavailable, "budget service unavailable", "api_error", "budget_unavailable", ""
	}

	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) {
		return upstreamErr.StatusCode, upstreamErr.Message, "invalid_request_error", "invalid_request", ""
	}

	return http.StatusBadRequest, err.Error(), "invalid_request_error", "invalid_request", ""
}

func writeError(w http.ResponseWriter, err error) {
	statusCode, msg, errType, code, retryAfter := mapErrorDetails(err)
	w.Header().Set("Content-Type", "application/json")
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Error: errorDetail{
			Message: msg,
			Type:    errType,
			Code:    code,
		},
	})
}

func setResponseHeaders(w http.ResponseWriter, costMicros int64, providerUsed string, cacheHit bool, remainingMicros int64, reqID string) {
	w.Header().Set("X-LGW-Cost-Micros", strconv.FormatInt(costMicros, 10))
	w.Header().Set("X-LGW-Provider", providerUsed)
	if cacheHit {
		w.Header().Set("X-LGW-Cache", "hit")
	} else {
		w.Header().Set("X-LGW-Cache", "miss")
	}
	w.Header().Set("X-LGW-Budget-Remaining-Micros", strconv.FormatInt(remainingMicros, 10))
	w.Header().Set("X-Request-Id", reqID)
}

type openAIChatReq struct {
	Model       string        `json:"model"`
	Messages    []llm.Message `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   int64         `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

type openAIChoice struct {
	Index        int         `json:"index"`
	Message      llm.Message `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type openAIUsageResp struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

type openAIChatResp struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []openAIChoice  `json:"choices"`
	Usage   openAIUsageResp `json:"usage"`
}

func (h *Handler) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rawKey := extractKey(r)
	if rawKey == "" {
		writeError(w, llm.ErrInvalidKey)
		return
	}

	var req openAIChatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("invalid json request: %w", err))
		return
	}

	temp := 0.7
	if req.Temperature != nil {
		temp = *req.Temperature
	}

	llmReq := &llm.Request{
		Model:     req.Model,
		Messages:  req.Messages,
		MaxTokens: req.MaxTokens,
		Stream:    req.Stream,
	}

	if req.Stream {
		h.streamOpenAI(w, r, rawKey, llmReq)
		return
	}

	res, err := h.svc.ChatCompletion(r.Context(), rawKey, llmReq, temp)
	if err != nil {
		writeError(w, err)
		return
	}

	setResponseHeaders(w, int64(res.CostMicros), res.Provider, res.CacheHit, res.RemainingBudgetMicros, res.Response.ID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(openAIChatResp{
		ID:      res.Response.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   res.Model,
		Choices: []openAIChoice{
			{
				Index:        0,
				Message:      llm.Message{Role: "assistant", Content: res.Response.Content},
				FinishReason: "stop",
			},
		},
		Usage: openAIUsageResp{
			PromptTokens:     res.Response.Usage.InputTokens,
			CompletionTokens: res.Response.Usage.OutputTokens,
			TotalTokens:      res.Response.Usage.Total(),
		},
	})
}

func (h *Handler) streamOpenAI(w http.ResponseWriter, r *http.Request, rawKey string, req *llm.Request) {
	streamRes, err := h.svc.ChatCompletionStream(r.Context(), rawKey, req)
	if err != nil {
		writeError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, errors.New("streaming unsupported by client"))
		return
	}

	setResponseHeaders(w, 0, streamRes.Provider, false, streamRes.RemainingBudgetMicros, req.ID)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	type chunkDelta struct {
		Content string `json:"content,omitempty"`
		Role    string `json:"role,omitempty"`
	}
	type chunkChoice struct {
		Index        int        `json:"index"`
		Delta        chunkDelta `json:"delta"`
		FinishReason *string    `json:"finish_reason"`
	}
	type openAIStreamChunk struct {
		ID      string        `json:"id"`
		Object  string        `json:"object"`
		Created int64         `json:"created"`
		Model   string        `json:"model"`
		Choices []chunkChoice `json:"choices"`
	}

	created := time.Now().Unix()
	for chunk := range streamRes.Stream {
		var finishReason *string
		if chunk.FinishReason != "" {
			fr := chunk.FinishReason
			finishReason = &fr
		}
		c := openAIStreamChunk{
			ID:      chunk.ID,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   streamRes.Model,
			Choices: []chunkChoice{
				{
					Index:        0,
					Delta:        chunkDelta{Content: chunk.Content},
					FinishReason: finishReason,
				},
			},
		}
		data, _ := json.Marshal(c)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

type anthropicReq struct {
	Model     string        `json:"model"`
	Messages  []llm.Message `json:"messages"`
	MaxTokens int64         `json:"max_tokens,omitempty"`
	Stream    bool          `json:"stream,omitempty"`
}

type anthropicTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthropicUsageBlock struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type anthropicResp struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Role       string                 `json:"role"`
	Model      string                 `json:"model"`
	Content    []anthropicTextContent `json:"content"`
	StopReason string                 `json:"stop_reason"`
	Usage      anthropicUsageBlock    `json:"usage"`
}

func (h *Handler) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rawKey := extractKey(r)
	if rawKey == "" {
		writeError(w, llm.ErrInvalidKey)
		return
	}

	var req anthropicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, fmt.Errorf("invalid json request: %w", err))
		return
	}

	llmReq := &llm.Request{
		Model:     req.Model,
		Messages:  req.Messages,
		MaxTokens: req.MaxTokens,
		Stream:    req.Stream,
	}

	res, err := h.svc.ChatCompletion(r.Context(), rawKey, llmReq, 0.7)
	if err != nil {
		writeError(w, err)
		return
	}

	setResponseHeaders(w, int64(res.CostMicros), res.Provider, res.CacheHit, res.RemainingBudgetMicros, res.Response.ID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(anthropicResp{
		ID:         res.Response.ID,
		Type:       "message",
		Role:       "assistant",
		Model:      res.Model,
		StopReason: "end_turn",
		Content: []anthropicTextContent{
			{Type: "text", Text: res.Response.Content},
		},
		Usage: anthropicUsageBlock{
			InputTokens:  res.Response.Usage.InputTokens,
			OutputTokens: res.Response.Usage.OutputTokens,
		},
	})
}

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type modelListResponse struct {
	Object string       `json:"object"`
	Data   []modelEntry `json:"data"`
}

func (h *Handler) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rawKey := extractKey(r)
	if rawKey == "" {
		writeError(w, llm.ErrInvalidKey)
		return
	}

	snap := h.authMgr.GetSnapshot()
	modelSet := make(map[string]bool)

	if snap != nil {
		for _, p := range snap.Pricing {
			if p != nil && p.Model != "" {
				modelSet[p.Model] = true
			}
		}
		for m := range snap.LogicalModels {
			modelSet[m] = true
		}
	}

	models := make([]modelEntry, 0, len(modelSet))
	for m := range modelSet {
		models = append(models, modelEntry{
			ID:      m,
			Object:  "model",
			Created: 1700000000,
			OwnedBy: "llm-gateway",
		})
	}
	sort.Slice(models, func(i, j int) bool {
		return models[i].ID < models[j].ID
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(modelListResponse{
		Object: "list",
		Data:   models,
	})
}
