package fake

import (
	"context"
	"sync"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// Option configures the programmable FakeProvider.
type Option func(*Provider)

// WithName sets the provider identifier.
func WithName(name string) Option {
	return func(p *Provider) {
		p.name = name
	}
}

// WithDelay sets simulated latency for requests.
func WithDelay(d time.Duration) Option {
	return func(p *Provider) {
		p.delay = d
	}
}

// WithContent sets the response text content.
func WithContent(c string) Option {
	return func(p *Provider) {
		p.content = c
	}
}

// WithUsage sets the simulated token usage.
func WithUsage(u llm.Usage) Option {
	return func(p *Provider) {
		p.usage = u
	}
}

// WithError configures an error to return on all calls.
func WithError(err error) Option {
	return func(p *Provider) {
		p.errorSeq = []error{err}
	}
}

// WithErrorSequence configures a sequence of errors per call attempt (last error repeats or nil succeeds).
func WithErrorSequence(errs ...error) Option {
	return func(p *Provider) {
		p.errorSeq = errs
	}
}

// WithStreamChunks configures the chunks emitted by DoStream.
func WithStreamChunks(chunks ...llm.Chunk) Option {
	return func(p *Provider) {
		p.streamChunks = chunks
	}
}

// Provider is a programmable fake implementation of provider.Provider for testing.
type Provider struct {
	mu           sync.Mutex
	name         string
	delay        time.Duration
	content      string
	usage        llm.Usage
	errorSeq     []error
	streamChunks []llm.Chunk
	calls        int
	reqs         []*llm.Request
}

// New creates a new FakeProvider with defaults and options applied.
func New(opts ...Option) *Provider {
	p := &Provider{
		name:    "fake",
		content: "Default fake content",
		usage: llm.Usage{
			InputTokens:  10,
			OutputTokens: 10,
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Name returns the provider name.
func (p *Provider) Name() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.name
}

// CallCount returns how many times Do or DoStream was called.
func (p *Provider) CallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// SetError updates the error sequence to return the given error.
func (p *Provider) SetError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errorSeq = []error{err}
}

// SetStreamChunks updates the stream chunks emitted by DoStream.
func (p *Provider) SetStreamChunks(chunks []llm.Chunk) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streamChunks = chunks
}

// SetDelay updates the simulated delay.
func (p *Provider) SetDelay(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.delay = d
}

// Requests returns all recorded requests received by this fake provider.
func (p *Provider) Requests() []*llm.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := make([]*llm.Request, len(p.reqs))
	copy(copied, p.reqs)
	return copied
}

func (p *Provider) recordCall(req *llm.Request) error {
	p.mu.Lock()
	p.calls++
	p.reqs = append(p.reqs, req)
	var err error
	if len(p.errorSeq) > 0 {
		idx := p.calls - 1
		if idx < len(p.errorSeq) {
			err = p.errorSeq[idx]
		} else if len(p.errorSeq) == 1 {
			err = p.errorSeq[0]
		}
	}
	p.mu.Unlock()
	return err
}

func (p *Provider) simulateDelay(ctx context.Context) error {
	p.mu.Lock()
	d := p.delay
	p.mu.Unlock()

	if d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return nil
}

// Do executes simulated non-streaming completion.
func (p *Provider) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	callErr := p.recordCall(req)
	if err := p.simulateDelay(ctx); err != nil {
		return nil, err
	}
	if callErr != nil {
		return nil, callErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return &llm.Response{
		ID:        req.ID,
		Model:     req.Model,
		Provider:  p.name,
		Content:   p.content,
		Usage:     p.usage,
		LatencyMs: p.delay.Milliseconds(),
	}, nil
}

// DoStream executes simulated streaming completion.
func (p *Provider) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	callErr := p.recordCall(req)
	if err := p.simulateDelay(ctx); err != nil {
		return nil, err
	}
	if callErr != nil {
		return nil, callErr
	}

	p.mu.Lock()
	chunks := make([]llm.Chunk, len(p.streamChunks))
	copy(chunks, p.streamChunks)
	if len(chunks) == 0 {
		chunks = []llm.Chunk{
			{ID: req.ID, Content: p.content, FinishReason: "stop", Usage: &p.usage},
		}
	}
	p.mu.Unlock()

	ch := make(chan llm.Chunk, len(chunks))
	go func() {
		defer close(ch)
		for _, c := range chunks {
			p.mu.Lock()
			d := p.delay
			p.mu.Unlock()
			if d > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				}
			}
			select {
			case <-ctx.Done():
				return
			case ch <- c:
			}
		}
	}()

	return ch, nil
}
