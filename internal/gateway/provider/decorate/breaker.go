package decorate

import (
	"context"
	"sync"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// CircuitOpenError is returned when a call is attempted on an open circuit breaker.
type CircuitOpenError struct{}

func (e *CircuitOpenError) Error() string {
	return "circuit breaker is open"
}

func (e *CircuitOpenError) Is(target error) bool {
	return target == llm.ErrRetryable
}

// ErrCircuitOpen is the sentinel error representing an open circuit breaker.
var ErrCircuitOpen error = &CircuitOpenError{}

type circuitState int

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

const (
	DefaultFailureThreshold = 5
	DefaultCooldown         = 30 * time.Second
)

// BreakerOption configures the circuit breaker decorator.
type BreakerOption func(*breakerDecorator)

// WithFailureThreshold sets the number of consecutive retryable failures required to open the circuit.
func WithFailureThreshold(threshold int) BreakerOption {
	return func(b *breakerDecorator) {
		if threshold > 0 {
			b.threshold = threshold
		}
	}
}

// WithCooldown sets the duration the circuit remains open before entering half-open.
func WithCooldown(d time.Duration) BreakerOption {
	return func(b *breakerDecorator) {
		if d > 0 {
			b.cooldown = d
		}
	}
}

type breakerDecorator struct {
	inner     provider.Provider
	threshold int
	cooldown  time.Duration

	mu              sync.Mutex
	state           circuitState
	failures        int
	openedAt        time.Time
	halfOpenRunning bool
}

// WithBreaker wraps a provider with a circuit breaker state machine.
func WithBreaker(p provider.Provider, opts ...BreakerOption) provider.Provider {
	b := &breakerDecorator{
		inner:     p,
		threshold: DefaultFailureThreshold,
		cooldown:  DefaultCooldown,
		state:     stateClosed,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *breakerDecorator) Name() string {
	return b.inner.Name()
}

func (b *breakerDecorator) canExecute() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	if b.state == stateOpen {
		if now.Sub(b.openedAt) >= b.cooldown {
			b.state = stateHalfOpen
			b.halfOpenRunning = true
			return true
		}
		return false
	}
	if b.state == stateHalfOpen {
		if b.halfOpenRunning {
			return false
		}
		b.halfOpenRunning = true
		return true
	}
	return true
}

func (b *breakerDecorator) recordResult(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.halfOpenRunning = false

	if err == nil {
		b.failures = 0
		b.state = stateClosed
		return
	}

	if provider.IsRetryable(err) {
		b.failures++
		if b.state == stateHalfOpen || b.failures >= b.threshold {
			b.state = stateOpen
			b.openedAt = time.Now()
		}
	}
}

func (b *breakerDecorator) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	if !b.canExecute() {
		return nil, ErrCircuitOpen
	}

	resp, err := b.inner.Do(ctx, req)
	b.recordResult(err)
	return resp, err
}

func (b *breakerDecorator) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	if !b.canExecute() {
		return nil, ErrCircuitOpen
	}

	ch, err := b.inner.DoStream(ctx, req)
	if err != nil {
		b.recordResult(err)
		return nil, err
	}

	wrapped := make(chan llm.Chunk)
	go func() {
		var streamErr error
		defer func() {
			close(wrapped)
			b.recordResult(streamErr)
		}()
		for c := range ch {
			if c.Err != nil {
				streamErr = c.Err
			}
			select {
			case <-ctx.Done():
				if streamErr == nil {
					streamErr = ctx.Err()
				}
				return
			case wrapped <- c:
			}
		}
	}()
	return wrapped, nil
}
