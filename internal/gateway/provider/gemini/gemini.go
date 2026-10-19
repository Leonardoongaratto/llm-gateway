package gemini

import (
	"context"
	"net/http"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/openai"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

const (
	DefaultBaseURL = "https://generativelanguage.googleapis.com/v1beta/openai"
)

// Option configures the Gemini adapter.
type Option func(*Adapter)

// WithBaseURL overrides the API base URL.
func WithBaseURL(url string) Option {
	return func(a *Adapter) {
		a.baseURL = url
	}
}

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(a *Adapter) {
		a.client = client
	}
}

// Adapter implements provider.Provider for Google Gemini via its OpenAI-compatible completions endpoint.
type Adapter struct {
	inner   *openai.Adapter
	baseURL string
	client  *http.Client
}

// New creates a new Gemini provider adapter.
func New(apiKey string, opts ...Option) *Adapter {
	a := &Adapter{
		baseURL: DefaultBaseURL,
	}
	for _, opt := range opts {
		opt(a)
	}

	openaiOpts := []openai.Option{
		openai.WithName("gemini"),
		openai.WithBaseURL(a.baseURL),
	}
	if a.client != nil {
		openaiOpts = append(openaiOpts, openai.WithHTTPClient(a.client))
	}

	a.inner = openai.New(apiKey, openaiOpts...)
	return a
}

// Name returns "gemini".
func (a *Adapter) Name() string {
	return a.inner.Name()
}

// Do executes a synchronous completion request via the OpenAI-compatible Gemini endpoint.
func (a *Adapter) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	return a.inner.Do(ctx, req)
}

// DoStream executes a streaming request via the OpenAI-compatible Gemini endpoint.
func (a *Adapter) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	return a.inner.DoStream(ctx, req)
}
