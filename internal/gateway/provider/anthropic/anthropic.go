package anthropic

import (
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

// DoStream implements the Provider interface streaming stub. Detailed SSE parsing is provided in B-32.
func (a *Adapter) DoStream(_ context.Context, _ *llm.Request) (<-chan llm.Chunk, error) {
	ch := make(chan llm.Chunk)
	close(ch)
	return ch, nil
}
