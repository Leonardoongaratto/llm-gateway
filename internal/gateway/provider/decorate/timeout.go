package decorate

import (
	"context"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

type timeoutDecorator struct {
	inner   provider.Provider
	timeout time.Duration
}

// WithTimeout wraps a provider with a maximum execution timeout per request.
func WithTimeout(p provider.Provider, timeout time.Duration) provider.Provider {
	if timeout <= 0 {
		return p
	}
	return &timeoutDecorator{
		inner:   p,
		timeout: timeout,
	}
}

func (t *timeoutDecorator) Name() string {
	return t.inner.Name()
}

func (t *timeoutDecorator) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	reqCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	return t.inner.Do(reqCtx, req)
}

func (t *timeoutDecorator) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	ch, err := t.inner.DoStream(ctx, req)
	if err != nil {
		return nil, err
	}

	wrapped := make(chan llm.Chunk)
	go func() {
		defer close(wrapped)
		for c := range ch {
			select {
			case <-ctx.Done():
				return
			case wrapped <- c:
			}
		}
	}()
	return wrapped, nil
}
