package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

const (
	DefaultBaseURL   = "https://api.anthropic.com"
	AnthropicVersion = "2023-06-01"
)

// Option configures the Anthropic adapter.
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

// Adapter implements provider.Provider for the Anthropic Messages API.
type Adapter struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

// New creates a new Anthropic provider adapter.
func New(apiKey string, opts ...Option) *Adapter {
	a := &Adapter{
		apiKey:  apiKey,
		baseURL: DefaultBaseURL,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Name returns "anthropic".
func (a *Adapter) Name() string {
	return "anthropic"
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicRequest struct {
	Model     string             `json:"model"`
	Messages  []anthropicMessage `json:"messages"`
	MaxTokens int64              `json:"max_tokens"`
	Stream    bool               `json:"stream"`
}

type anthropicContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthropicUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
}

type anthropicResponse struct {
	ID      string             `json:"id"`
	Content []anthropicContent `json:"content"`
	Model   string             `json:"model"`
	Usage   anthropicUsage     `json:"usage"`
	Error   *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func parseAnthropicError(body []byte, statusCode int) error {
	var resp anthropicResponse
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

// Do executes a synchronous completion request against the Anthropic Messages API.
func (a *Adapter) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	start := time.Now()
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}

	msgs := make([]anthropicMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, anthropicMessage{
			Role:    m.Role,
			Content: m.Content,
		})
	}

	bodyBytes, err := json.Marshal(anthropicRequest{
		Model:     req.Model,
		Messages:  msgs,
		MaxTokens: maxTokens,
		Stream:    false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", AnthropicVersion)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, parseAnthropicError(respBody, resp.StatusCode)
	}

	var parsed anthropicResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	var text strings.Builder
	for _, c := range parsed.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}

	return &llm.Response{
		ID:       parsed.ID,
		Model:    parsed.Model,
		Provider: a.Name(),
		Content:  text.String(),
		Usage: llm.Usage{
			InputTokens:      parsed.Usage.InputTokens,
			OutputTokens:     parsed.Usage.OutputTokens,
			CacheReadTokens:  parsed.Usage.CacheReadInputTokens,
			CacheWriteTokens: parsed.Usage.CacheCreationInputTokens,
		},
		LatencyMs: time.Since(start).Milliseconds(),
	}, nil
}

type anthropicStreamEvent struct {
	Type    string `json:"type"`
	Message *struct {
		ID    string         `json:"id"`
		Model string         `json:"model"`
		Usage anthropicUsage `json:"usage"`
	} `json:"message,omitempty"`
	Delta *struct {
		Type       string  `json:"type"`
		Text       string  `json:"text"`
		StopReason *string `json:"stop_reason"`
	} `json:"delta,omitempty"`
	Usage *struct {
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage,omitempty"`
}

type anthropicStreamState struct {
	inputTokens      int64
	cacheReadTokens  int64
	cacheWriteTokens int64
}

func (s *anthropicStreamState) processLine(line string) *llm.Chunk {
	if !strings.HasPrefix(line, "data: ") {
		return nil
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
	var evt anthropicStreamEvent
	if err := json.Unmarshal([]byte(payload), &evt); err != nil {
		return nil
	}

	switch evt.Type {
	case "message_start":
		if evt.Message != nil {
			s.inputTokens = evt.Message.Usage.InputTokens
			s.cacheReadTokens = evt.Message.Usage.CacheReadInputTokens
			s.cacheWriteTokens = evt.Message.Usage.CacheCreationInputTokens
		}
		return nil
	case "content_block_delta":
		if evt.Delta != nil && evt.Delta.Text != "" {
			return &llm.Chunk{Content: evt.Delta.Text}
		}
		return nil
	case "message_delta":
		var outputTokens int64
		if evt.Usage != nil {
			outputTokens = evt.Usage.OutputTokens
		}
		stopReason := "stop"
		if evt.Delta != nil && evt.Delta.StopReason != nil {
			stopReason = *evt.Delta.StopReason
		}
		return &llm.Chunk{
			FinishReason: stopReason,
			Usage: &llm.Usage{
				InputTokens:      s.inputTokens,
				OutputTokens:     outputTokens,
				CacheReadTokens:  s.cacheReadTokens,
				CacheWriteTokens: s.cacheWriteTokens,
			},
		}
	default:
		return nil
	}
}

func (a *Adapter) scanStream(ctx context.Context, body io.ReadCloser, out chan<- llm.Chunk) {
	defer func() { _ = body.Close() }()
	defer close(out)

	state := &anthropicStreamState{}
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if chunk := state.processLine(line); chunk != nil {
			select {
			case <-ctx.Done():
				return
			case out <- *chunk:
			}
		}
	}
}

// DoStream executes a streaming request against the Anthropic Messages API with SSE.
func (a *Adapter) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}

	msgs := make([]anthropicMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, anthropicMessage{
			Role:    m.Role,
			Content: m.Content,
		})
	}

	bodyBytes, err := json.Marshal(anthropicRequest{
		Model:     req.Model,
		Messages:  msgs,
		MaxTokens: maxTokens,
		Stream:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", AnthropicVersion)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		respBody, _ := io.ReadAll(resp.Body)
		return nil, parseAnthropicError(respBody, resp.StatusCode)
	}

	out := make(chan llm.Chunk, 32)
	go a.scanStream(ctx, resp.Body, out)

	return out, nil
}
