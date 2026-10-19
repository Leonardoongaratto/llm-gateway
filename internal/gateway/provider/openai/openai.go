package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

const (
	DefaultBaseURL = "https://api.openai.com"
)

// Option configures the OpenAI adapter.
type Option func(*Adapter)

// WithBaseURL overrides the API base URL.
func WithBaseURL(url string) Option {
	return func(a *Adapter) {
		if url != "" {
			a.baseURL = strings.TrimRight(url, "/")
		}
	}
}

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(a *Adapter) {
		if client != nil {
			a.client = client
		}
	}
}

// WithName overrides the provider name (used e.g. for Gemini).
func WithName(name string) Option {
	return func(a *Adapter) {
		if name != "" {
			a.name = name
		}
	}
}

// Adapter implements provider.Provider for the OpenAI Chat Completions API.
type Adapter struct {
	apiKey  string
	baseURL string
	name    string
	client  *http.Client
}

// New creates a new OpenAI provider adapter.
func New(apiKey string, opts ...Option) *Adapter {
	a := &Adapter{
		apiKey:  apiKey,
		baseURL: DefaultBaseURL,
		name:    "openai",
		client:  &http.Client{Timeout: 60 * time.Second},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Name returns the provider name (default "openai").
func (a *Adapter) Name() string {
	return a.name
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatRequest struct {
	Model         string         `json:"model"`
	Messages      []chatMessage  `json:"messages"`
	MaxTokens     int64          `json:"max_tokens,omitempty"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
	Temperature   *float64       `json:"temperature,omitempty"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type promptTokensDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type chatUsage struct {
	PromptTokens        int64                `json:"prompt_tokens"`
	CompletionTokens    int64                `json:"completion_tokens"`
	TotalTokens         int64                `json:"total_tokens"`
	PromptTokensDetails *promptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   chatUsage    `json:"usage"`
	Error   *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func parseOpenAIError(body []byte, statusCode int) error {
	var resp chatResponse
	errMsg := fmt.Sprintf("status code %d", statusCode)
	if err := json.Unmarshal(body, &resp); err == nil && resp.Error != nil && resp.Error.Message != "" {
		errMsg = resp.Error.Message
	}
	return &provider.UpstreamError{
		StatusCode: statusCode,
		Message:    errMsg,
		Retryable:  statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError,
	}
}

// Do executes a synchronous completion request against the OpenAI Chat Completions API.
func (a *Adapter) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	start := time.Now()

	msgs := make([]chatMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, chatMessage{
			Role:    m.Role,
			Content: m.Content,
		})
	}

	bodyBytes, err := json.Marshal(chatRequest{
		Model:       req.Model,
		Messages:    msgs,
		MaxTokens:   req.MaxTokens,
		Stream:      false,
		Temperature: req.Temperature,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	endpoint := a.baseURL + "/v1/chat/completions"
	if strings.Contains(a.baseURL, "/chat/completions") {
		endpoint = a.baseURL
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		statusCode := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		return nil, &provider.UpstreamError{
			StatusCode: statusCode,
			Message:    err.Error(),
			Retryable:  true,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, parseOpenAIError(respBody, resp.StatusCode)
	}

	var parsed chatResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	content := ""
	if len(parsed.Choices) > 0 {
		content = parsed.Choices[0].Message.Content
	}

	var cachedTokens int64
	if parsed.Usage.PromptTokensDetails != nil {
		cachedTokens = parsed.Usage.PromptTokensDetails.CachedTokens
	}

	return &llm.Response{
		ID:       parsed.ID,
		Model:    parsed.Model,
		Provider: a.Name(),
		Content:  content,
		Usage: llm.Usage{
			InputTokens:     parsed.Usage.PromptTokens,
			OutputTokens:    parsed.Usage.CompletionTokens,
			CacheReadTokens: cachedTokens,
		},
		LatencyMs: time.Since(start).Milliseconds(),
	}, nil
}

type chatStreamChoice struct {
	Index int `json:"index"`
	Delta struct {
		Content string `json:"content"`
		Role    string `json:"role"`
	} `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type chatStreamChunk struct {
	ID      string             `json:"id"`
	Choices []chatStreamChoice `json:"choices"`
	Usage   *chatUsage         `json:"usage,omitempty"`
}

func parseOpenAIStreamLine(line string) (*llm.Chunk, bool) {
	if !strings.HasPrefix(line, "data: ") {
		return nil, true
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
	if payload == "[DONE]" {
		return nil, false
	}

	var chunk chatStreamChunk
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		return nil, true
	}

	var content string
	var finishReason string
	if len(chunk.Choices) > 0 {
		content = chunk.Choices[0].Delta.Content
		if chunk.Choices[0].FinishReason != nil {
			finishReason = *chunk.Choices[0].FinishReason
		}
	}

	var usage *llm.Usage
	if chunk.Usage != nil {
		var cachedTokens int64
		if chunk.Usage.PromptTokensDetails != nil {
			cachedTokens = chunk.Usage.PromptTokensDetails.CachedTokens
		}
		usage = &llm.Usage{
			InputTokens:     chunk.Usage.PromptTokens,
			OutputTokens:    chunk.Usage.CompletionTokens,
			CacheReadTokens: cachedTokens,
		}
	}

	if content == "" && finishReason == "" && usage == nil {
		return nil, true
	}

	return &llm.Chunk{
		ID:           chunk.ID,
		Content:      content,
		FinishReason: finishReason,
		Usage:        usage,
	}, true
}

func (a *Adapter) scanStream(ctx context.Context, body io.ReadCloser, out chan<- llm.Chunk) {
	defer func() { _ = body.Close() }()
	defer close(out)

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		chunk, cont := parseOpenAIStreamLine(line)
		if !cont {
			return
		}
		if chunk != nil {
			select {
			case <-ctx.Done():
				return
			case out <- *chunk:
			}
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case <-ctx.Done():
		case out <- llm.Chunk{Err: err}:
		}
	}
}

// DoStream executes a streaming request against the OpenAI Chat Completions API with SSE.
func (a *Adapter) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	msgs := make([]chatMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, chatMessage{
			Role:    m.Role,
			Content: m.Content,
		})
	}

	bodyBytes, err := json.Marshal(chatRequest{
		Model:         req.Model,
		Messages:      msgs,
		MaxTokens:     req.MaxTokens,
		Stream:        true,
		StreamOptions: &streamOptions{IncludeUsage: true},
		Temperature:   req.Temperature,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	endpoint := a.baseURL + "/v1/chat/completions"
	if strings.Contains(a.baseURL, "/chat/completions") {
		endpoint = a.baseURL
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		statusCode := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		return nil, &provider.UpstreamError{
			StatusCode: statusCode,
			Message:    err.Error(),
			Retryable:  true,
		}
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		respBody, _ := io.ReadAll(resp.Body)
		return nil, parseOpenAIError(respBody, resp.StatusCode)
	}

	out := make(chan llm.Chunk, 32)
	go a.scanStream(ctx, resp.Body, out)

	return out, nil
}
