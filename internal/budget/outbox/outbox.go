package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

type Store interface {
	FetchPendingOutbox(ctx context.Context, limit int) ([]store.OutboxMessage, error)
	MarkOutboxPublished(ctx context.Context, ids []string, publishedAt time.Time) error
}

type LimitsStore interface {
	SetKeyLimits(ctx context.Context, limits core.KeyLimits) error
}

type Relay struct {
	store     Store
	js        jetstream.JetStream
	batchSize int
	interval  time.Duration
	logger    *slog.Logger
}

type RelayOption func(*Relay)

func WithRelayBatchSize(size int) RelayOption {
	return func(r *Relay) {
		if size > 0 {
			r.batchSize = size
		}
	}
}

func WithRelayInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.interval = d
		}
	}
}

func WithRelayLogger(l *slog.Logger) RelayOption {
	return func(r *Relay) {
		if l != nil {
			r.logger = l
		}
	}
}

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

type ConfigConsumer struct {
	store    LimitsStore
	consumer jetstream.Consumer
	logger   *slog.Logger
}

func NewConfigConsumer(s LimitsStore, js jetstream.JetStream) (*ConfigConsumer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamConfig, jetstream.ConsumerConfig{
		Durable:       "budget-config-consumer",
		FilterSubject: "lgw.config.changed.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create config consumer: %w", err)
	}

	return &ConfigConsumer{
		store:    s,
		consumer: consumer,
		logger:   slog.Default(),
	}, nil
}

func (c *ConfigConsumer) processConfigMessage(ctx context.Context, msg jetstream.Msg) error {
	var cfgEvent eventsv1.ConfigChanged
	if err := protojson.Unmarshal(msg.Data(), &cfgEvent); err != nil {
		c.logger.ErrorContext(ctx, "failed to unmarshal ConfigChanged event", slog.Any("error", err))
		return err
	}

	for _, kl := range cfgEvent.KeyLimits {
		limits := core.KeyLimits{
			KeyID:              kl.KeyId,
			DailyLimitMicros:   kl.DailyLimitMicros,
			MonthlyLimitMicros: kl.MonthlyLimitMicros,
			RPM:                kl.Rpm,
		}
		if err := c.store.SetKeyLimits(ctx, limits); err != nil {
			c.logger.ErrorContext(ctx, "failed to update key limits from event",
				slog.String("key_id", kl.KeyId),
				slog.Any("error", err),
			)
			return err
		}
	}

	return nil
}

func (c *ConfigConsumer) Start(ctx context.Context) (jetstream.ConsumeContext, error) {
	return c.consumer.Consume(func(msg jetstream.Msg) {
		msgCtx := natsx.ExtractTraceContext(ctx, msg)
		if err := c.processConfigMessage(msgCtx, msg); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
}
