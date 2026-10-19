package router

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// FallbackHook is invoked whenever automatic fallback transitions from one provider to another.
type FallbackHook func(ctx context.Context, requestID, fromProvider, toProvider, reason string)

// ExecutionResult contains the response and routing metadata for a non-streaming call.
type ExecutionResult struct {
	Response     *llm.Response
	ProviderUsed string
	ModelUsed    string
	FallbackFrom string
}

// StreamExecutionResult contains the stream channel and routing metadata for a streaming call.
type StreamExecutionResult struct {
	Stream       <-chan llm.Chunk
	ProviderUsed string
	ModelUsed    string
	FallbackFrom string
}

// Router dispatches requests to providers resolving logical models and performing selective fallback.
type Router struct {
	mu              sync.RWMutex
	registry        map[string]provider.Provider
	defaultProvider provider.Provider
	fallbackHook    FallbackHook
}

// New creates a new Router.
func New() *Router {
	return &Router{
		registry: make(map[string]provider.Provider),
	}
}

// Register maps a model name, prefix, or provider ID to a Provider implementation.
func (r *Router) Register(target string, p provider.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry[strings.ToLower(target)] = p
}

// SetDefaultProvider sets a fallback provider when no specific match is found in the registry.
func (r *Router) SetDefaultProvider(p provider.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultProvider = p
}

// SetFallbackHook registers a callback for provider fallback events.
func (r *Router) SetFallbackHook(h FallbackHook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallbackHook = h
}

func (r *Router) matchPrefix(target string) (provider.Provider, string, bool) {
	for _, sep := range []string{"/", ":"} {
		if parts := strings.SplitN(target, sep, 2); len(parts) == 2 {
			provName := strings.ToLower(parts[0])
			modelName := parts[1]
			if p, ok := r.registry[provName]; ok {
				return p, modelName, true
			}
		}
	}
	return nil, "", false
}

func (r *Router) matchHeuristics(targetLower string) (provider.Provider, bool) {
	switch {
	case strings.HasPrefix(targetLower, "claude"):
		p, ok := r.registry["anthropic"]
		return p, ok
	case strings.HasPrefix(targetLower, "gpt") || strings.HasPrefix(targetLower, "o1") || strings.HasPrefix(targetLower, "o3"):
		p, ok := r.registry["openai"]
		return p, ok
	case strings.HasPrefix(targetLower, "gemini"):
		p, ok := r.registry["gemini"]
		return p, ok
	default:
		return nil, false
	}
}

func (r *Router) resolveProvider(target string) (provider.Provider, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	targetLower := strings.ToLower(target)
	if p, ok := r.registry[targetLower]; ok {
		return p, target, nil
	}

	if p, modelName, ok := r.matchPrefix(target); ok {
		return p, modelName, nil
	}

	if p, ok := r.matchHeuristics(targetLower); ok {
		return p, target, nil
	}

	if r.defaultProvider != nil {
		return r.defaultProvider, target, nil
	}

	return nil, "", fmt.Errorf("no provider registered for model target: %s", target)
}

func (r *Router) notifyFallback(ctx context.Context, reqID string, i int, chain []string, fromProv provider.Provider, err error) string {
	if i+1 >= len(chain) {
		return ""
	}

	nextCandidate := chain[i+1]
	nextProv, _, resolveErr := r.resolveProvider(nextCandidate)
	toName := nextCandidate
	if resolveErr == nil && nextProv != nil {
		toName = nextProv.Name()
	}

	r.mu.RLock()
	hook := r.fallbackHook
	r.mu.RUnlock()

	if hook != nil {
		hook(ctx, reqID, fromProv.Name(), toName, err.Error())
	}
	return fromProv.Name()
}

// Execute dispatches a non-streaming request through the logical model fallback chain.
func (r *Router) Execute(ctx context.Context, snap *auth.Snapshot, req *llm.Request) (*ExecutionResult, error) {
	chain := snap.ResolveChain(req.Model)
	var lastFallbackFrom string
	var lastErr error

	for i, candidate := range chain {
		prov, targetModel, err := r.resolveProvider(candidate)
		if err != nil {
			lastErr = err
			continue
		}

		targetReq := *req
		targetReq.Model = targetModel
		targetReq.Provider = prov.Name()

		resp, err := prov.Do(ctx, &targetReq)
		if err == nil {
			return &ExecutionResult{
				Response:     resp,
				ProviderUsed: prov.Name(),
				ModelUsed:    targetModel,
				FallbackFrom: lastFallbackFrom,
			}, nil
		}

		lastErr = err
		if !provider.IsRetryable(err) {
			return nil, err
		}

		if fb := r.notifyFallback(ctx, req.ID, i, chain, prov, err); fb != "" {
			lastFallbackFrom = fb
		}
	}

	if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
		return nil, lastErr
	}

	return nil, fmt.Errorf("%w: %w", llm.ErrAllProvidersFailed, lastErr)
}

// ExecuteStream initiates a streaming request with pre-flight fallback if stream creation fails.
func (r *Router) ExecuteStream(ctx context.Context, snap *auth.Snapshot, req *llm.Request) (*StreamExecutionResult, error) {
	chain := snap.ResolveChain(req.Model)
	var lastFallbackFrom string
	var lastErr error

	for i, candidate := range chain {
		prov, targetModel, err := r.resolveProvider(candidate)
		if err != nil {
			lastErr = err
			continue
		}

		targetReq := *req
		targetReq.Model = targetModel
		targetReq.Provider = prov.Name()

		stream, err := prov.DoStream(ctx, &targetReq)
		if err == nil {
			return &StreamExecutionResult{
				Stream:       stream,
				ProviderUsed: prov.Name(),
				ModelUsed:    targetModel,
				FallbackFrom: lastFallbackFrom,
			}, nil
		}

		lastErr = err
		if !provider.IsRetryable(err) {
			return nil, err
		}

		if fb := r.notifyFallback(ctx, req.ID, i, chain, prov, err); fb != "" {
			lastFallbackFrom = fb
		}
	}

	return nil, fmt.Errorf("%w: %w", llm.ErrAllProvidersFailed, lastErr)
}
