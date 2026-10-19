package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/control/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats.go/jetstream"
)

// Store defines the outbox persistence contract required by the relay.
type Store interface {
	FetchPendingOutbox(ctx context.Context, limit int) ([]store.OutboxMessage, error)
	MarkOutboxPublished(ctx context.Context, ids []string, publishedAt time.Time) error
}

// Relay polls pending outbox messages and publishes them to NATS JetStream.
type Relay struct {
	store     Store
	js        jetstream.JetStream
	batchSize int
	interval  time.Duration
	logger    *slog.Logger
}

// RelayOption configures relay runtime parameters.
type RelayOption func(*Relay)

// WithRelayBatchSize sets max outbox records fetched per iteration.
func WithRelayBatchSize(size int) RelayOption {
	return func(r *Relay) {
		if size > 0 {
			r.batchSize = size
		}
	}
}

// WithRelayInterval sets polling interval between iterations.
func WithRelayInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithRelayLogger sets the structured logger for relay diagnostics.
func WithRelayLogger(l *slog.Logger) RelayOption {
	return func(r *Relay) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewRelay constructs a new outbox relay worker.
func NewRelay(s Store, js jetstream.JetStream, opts ...RelayOption) *Relay {
	r := &Relay{
		store:     s,
		js:        js,
		batchSize: 50,
		interval:  100 * time.Millisecond,
		logger:    slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RelayOnce fetches pending messages from store and publishes them to JetStream.
func (r *Relay) RelayOnce(ctx context.Context) (int, error) {
	messages, err := r.store.FetchPendingOutbox(ctx, r.batchSize)
	if err != nil {
		r.logger.ErrorContext(ctx, "failed to fetch outbox messages", slog.Any("error", err))
		return 0, err
	}

	if len(messages) == 0 {
		return 0, nil
	}

	var publishedIDs []string
	for _, msg := range messages {
		_, err := natsx.Publish(ctx, r.js, msg.Subject, []byte(msg.Payload), msg.ID)
		if err != nil {
			r.logger.ErrorContext(ctx, "failed to publish outbox event to JetStream",
				slog.String("id", msg.ID),
				slog.String("subject", msg.Subject),
				slog.Any("error", err),
			)
			break
		}
		publishedIDs = append(publishedIDs, msg.ID)
	}

	if len(publishedIDs) > 0 {
		if err := r.store.MarkOutboxPublished(ctx, publishedIDs, time.Now().UTC()); err != nil {
			r.logger.ErrorContext(ctx, "failed to mark outbox messages as published", slog.Any("error", err))
			return len(publishedIDs), err
		}
	}

	return len(publishedIDs), nil
}

// Run executes the polling loop until the given context is canceled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, _ = r.RelayOnce(ctx)
		}
	}
}
