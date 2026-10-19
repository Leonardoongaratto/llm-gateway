package provider

import (
	"context"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// Provider defines the interface for communicating with an LLM upstream (real or simulated).
type Provider interface {
	// Name returns the provider identifier (e.g. "anthropic", "openai", "gemini", "fake").
	Name() string

	// Do executes a synchronous (non-streaming) completion request.
	Do(ctx context.Context, req *llm.Request) (*llm.Response, error)

	// DoStream executes a streaming request and emits incremental chunks via a channel.
	// The channel is closed when the stream ends or encounters an error.
	DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error)
}
