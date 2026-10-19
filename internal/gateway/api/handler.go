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

type RequestError struct {
	StatusCode int
	Message    string
	Type       string
	Code       string
}

func (e *RequestError) Error() string {
	return e.Message
}

func mapClientErrors(err error) (int, string, string, string, string, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		errType := reqErr.Type
		if errType == "" {
			errType = "invalid_request_error"
		}
		code := reqErr.Code
		if code == "" {
			code = "invalid_request"
		}
		status := reqErr.StatusCode
		if status == 0 {
			status = http.StatusBadRequest
		}
		return status, reqErr.Message, errType, code, "", true
	}

	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return http.StatusRequestEntityTooLarge, "request body too large (maximum 10MB)", "invalid_request_error", "request_too_large", "", true
	}

	var rateLimitErr *budgetclient.RateLimitError
	if errors.As(err, &rateLimitErr) {
		return http.StatusTooManyRequests, "rate limit exceeded", "rate_limit_error", "rate_limited", strconv.FormatInt(rateLimitErr.RetryAfterSeconds, 10), true
	}

	return 0, "", "", "", "", false
}

func mapKernelErrors(err error) (int, string, string, string, string, bool) {
	switch {
	case errors.Is(err, llm.ErrInvalidKey):
		return http.StatusUnauthorized, "invalid virtual key", "invalid_request_error", "invalid_key", "", true
	case errors.Is(err, llm.ErrModelNotAllowed):
		return http.StatusForbidden, "model not allowed for virtual key", "permission_error", "model_not_allowed", "", true
	case errors.Is(err, llm.ErrBudgetExceeded):
		return http.StatusPaymentRequired, "budget exceeded", "budget_error", "budget_exceeded", "", true
	case errors.Is(err, llm.ErrRateLimited):
		return http.StatusTooManyRequests, "rate limit exceeded", "rate_limit_error", "rate_limited", "60", true
	case errors.Is(err, llm.ErrAllProvidersFailed):
		return http.StatusBadGateway, "all providers failed in chain", "api_error", "all_providers_failed", "", true
	case errors.Is(err, llm.ErrBudgetUnavailable):
		return http.StatusServiceUnavailable, "budget service unavailable", "api_error", "budget_unavailable", "", true
	default:
		return 0, "", "", "", "", false
	}
}

func mapUpstreamError(err error) (int, string, string, string, string, bool) {
	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) {
		errType := "invalid_request_error"
		code := "invalid_request"
		if upstreamErr.StatusCode >= 500 {
			errType = "api_error"
			code = "upstream_error"
		}
		return upstreamErr.StatusCode, upstreamErr.Message, errType, code, "", true
	}
	return 0, "", "", "", "", false
}

func mapErrorDetails(err error) (int, string, string, string, string) {
	if status, msg, errType, code, retryAfter, ok := mapClientErrors(err); ok {
		return status, msg, errType, code, retryAfter
	}
	if status, msg, errType, code, retryAfter, ok := mapKernelErrors(err); ok {
		return status, msg, errType, code, retryAfter
	}
	if status, msg, errType, code, retryAfter, ok := mapUpstreamError(err); ok {
		return status, msg, errType, code, retryAfter
	}
	return http.StatusInternalServerError, "internal server error", "api_error", "internal_error", ""
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

func writeAnthropicError(w http.ResponseWriter, err error) {
	statusCode, msg, errType, _, retryAfter := mapErrorDetails(err)
	w.Header().Set("Content-Type", "application/json")
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errType,
			"message": msg,
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

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)

	var req openAIChatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, err)
			return
		}
		writeError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "invalid json request body",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	if strings.TrimSpace(req.Model) == "" {
		writeError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "model is required",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	if len(req.Messages) == 0 {
		writeError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "messages is required and must not be empty",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	temp := 0.7
	if req.Temperature != nil {
		temp = *req.Temperature
	}

	llmReq := &llm.Request{
		ID:          r.Header.Get("X-Request-Id"),
		Model:       req.Model,
		Messages:    req.Messages,
		MaxTokens:   req.MaxTokens,
		Stream:      req.Stream,
		Temperature: req.Temperature,
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

	setResponseHeaders(w, int64(res.CostMicros), res.Provider, res.CacheHit, res.RemainingBudgetMicros, llmReq.ID)
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
	var streamErr error
	for chunk := range streamRes.Stream {
		if chunk.Err != nil {
			streamErr = chunk.Err
			break
		}
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

	if streamErr != nil {
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": "stream error from upstream provider",
				"type":    "api_error",
				"code":    "stream_error",
			},
		})
		_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", errPayload)
		flusher.Flush()
		return
	}

	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

type anthropicReq struct {
	Model       string        `json:"model"`
	Messages    []llm.Message `json:"messages"`
	MaxTokens   int64         `json:"max_tokens,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
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
		writeAnthropicError(w, llm.ErrInvalidKey)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)

	var req anthropicReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeAnthropicError(w, err)
			return
		}
		writeAnthropicError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "invalid json request body",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	if strings.TrimSpace(req.Model) == "" {
		writeAnthropicError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "model is required",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	if len(req.Messages) == 0 {
		writeAnthropicError(w, &RequestError{
			StatusCode: http.StatusBadRequest,
			Message:    "messages is required and must not be empty",
			Type:       "invalid_request_error",
			Code:       "invalid_request",
		})
		return
	}

	temp := 0.7
	if req.Temperature != nil {
		temp = *req.Temperature
	}

	llmReq := &llm.Request{
		ID:          r.Header.Get("X-Request-Id"),
		Model:       req.Model,
		Messages:    req.Messages,
		MaxTokens:   req.MaxTokens,
		Stream:      req.Stream,
		Temperature: req.Temperature,
	}

	if req.Stream {
		h.streamAnthropic(w, r, rawKey, llmReq)
		return
	}

	res, err := h.svc.ChatCompletion(r.Context(), rawKey, llmReq, temp)
	if err != nil {
		writeAnthropicError(w, err)
		return
	}

	setResponseHeaders(w, int64(res.CostMicros), res.Provider, res.CacheHit, res.RemainingBudgetMicros, llmReq.ID)
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

func (h *Handler) streamAnthropic(w http.ResponseWriter, r *http.Request, rawKey string, req *llm.Request) {
	streamRes, err := h.svc.ChatCompletionStream(r.Context(), rawKey, req)
	if err != nil {
		writeAnthropicError(w, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAnthropicError(w, errors.New("streaming unsupported by client"))
		return
	}

	setResponseHeaders(w, 0, streamRes.Provider, false, streamRes.RemainingBudgetMicros, req.ID)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	inputTokens := core.EstimatePromptTokens(req.Messages)
	msgStartPayload, _ := json.Marshal(map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":          req.ID,
			"type":        "message",
			"role":        "assistant",
			"model":       streamRes.Model,
			"content":     []any{},
			"stop_reason": nil,
			"usage": map[string]any{
				"input_tokens":  inputTokens,
				"output_tokens": 0,
			},
		},
	})
	_, _ = fmt.Fprintf(w, "event: message_start\ndata: %s\n\n", msgStartPayload)

	blockStartPayload, _ := json.Marshal(map[string]any{
		"type":  "content_block_start",
		"index": 0,
		"content_block": map[string]any{
			"type": "text",
			"text": "",
		},
	})
	_, _ = fmt.Fprintf(w, "event: content_block_start\ndata: %s\n\n", blockStartPayload)
	flusher.Flush()

	var streamErr error
	var finalOutputTokens int64
	var estimatedOutputTokens int64
	for chunk := range streamRes.Stream {
		if chunk.Err != nil {
			streamErr = chunk.Err
			break
		}
		if chunk.Usage != nil {
			finalOutputTokens = chunk.Usage.OutputTokens
		} else if chunk.Content != "" {
			estimatedOutputTokens += core.EstimateOutputTokens(chunk.Content)
		}
		if chunk.Content != "" {
			deltaPayload, _ := json.Marshal(map[string]any{
				"type":  "content_block_delta",
				"index": 0,
				"delta": map[string]any{
					"type": "text_delta",
					"text": chunk.Content,
				},
			})
			_, _ = fmt.Fprintf(w, "event: content_block_delta\ndata: %s\n\n", deltaPayload)
			flusher.Flush()
		}
	}

	if finalOutputTokens == 0 {
		finalOutputTokens = estimatedOutputTokens
	}

	if streamErr != nil {
		errPayload, _ := json.Marshal(map[string]any{
			"type": "error",
			"error": map[string]any{
				"type":    "api_error",
				"message": "stream error from upstream provider",
			},
		})
		_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", errPayload)
		flusher.Flush()
		return
	}

	blockStopPayload, _ := json.Marshal(map[string]any{
		"type":  "content_block_stop",
		"index": 0,
	})
	_, _ = fmt.Fprintf(w, "event: content_block_stop\ndata: %s\n\n", blockStopPayload)

	msgDeltaPayload, _ := json.Marshal(map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
		},
		"usage": map[string]any{
			"output_tokens": finalOutputTokens,
		},
	})
	_, _ = fmt.Fprintf(w, "event: message_delta\ndata: %s\n\n", msgDeltaPayload)

	msgStopPayload, _ := json.Marshal(map[string]any{
		"type": "message_stop",
	})
	_, _ = fmt.Fprintf(w, "event: message_stop\ndata: %s\n\n", msgStopPayload)
	flusher.Flush()
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

func collectAllowedModels(snap *auth.Snapshot, keyInfo *auth.KeyInfo) []modelEntry {
	if snap == nil {
		return []modelEntry{}
	}

	modelSet := make(map[string]bool)
	for _, p := range snap.Pricing {
		if p != nil && p.Model != "" && keyInfo.IsModelAllowed(p.Model) {
			modelSet[p.Model] = true
		}
	}
	for m := range snap.LogicalModels {
		if keyInfo.IsModelAllowed(m) {
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
	return models
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

	keyInfo, err := h.authMgr.Authenticate(rawKey)
	if err != nil {
		writeError(w, err)
		return
	}

	models := collectAllowedModels(h.authMgr.GetSnapshot(), keyInfo)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(modelListResponse{
		Object: "list",
		Data:   models,
	})
}
