package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	js       jetstream.JetStream
	store    *store.Store
	logger   *slog.Logger
	mu       sync.Mutex
	contexts []jetstream.ConsumeContext
	running  bool
}

func New(js jetstream.JetStream, st *store.Store, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		js:     js,
		store:  st,
		logger: logger,
	}
}

func unmarshalEvent(data []byte, msg proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := unmarshaler.Unmarshal(data, msg); err == nil {
		return nil
	}
	return proto.Unmarshal(data, msg)
}

func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	configs := []struct {
		stream  string
		durable string
	}{
		{stream: natsx.StreamUsage, durable: "usage_events_worker"},
		{stream: natsx.StreamBudget, durable: "usage_budget_worker"},
		{stream: natsx.StreamConfig, durable: "usage_config_worker"},
	}

	for _, item := range configs {
		consumer, err := c.js.CreateOrUpdateConsumer(ctx, item.stream, jetstream.ConsumerConfig{
			Durable:       item.durable,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			MaxDeliver:    10,
		})
		if err != nil {
			c.stopContexts()
			return fmt.Errorf("failed to create consumer %s on stream %s: %w", item.durable, item.stream, err)
		}

		cCtx, err := natsx.ConsumeWithDLQ(ctx, c.js, consumer, c.Dispatch, 5)
		if err != nil {
			c.stopContexts()
			return fmt.Errorf("failed to consume from %s: %w", item.durable, err)
		}
		c.contexts = append(c.contexts, cCtx)
	}

	c.running = true
	c.logger.Info("usage consumers started successfully")
	return nil
}

func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stopContexts()
	c.running = false
	c.logger.Info("usage consumers stopped")
}

func (c *Consumer) stopContexts() {
	for _, ctx := range c.contexts {
		ctx.Stop()
	}
	c.contexts = nil
}

func (c *Consumer) Dispatch(ctx context.Context, msg jetstream.Msg) error {
	subject := msg.Subject()
	switch {
	case strings.HasPrefix(subject, "lgw.usage."):
		return c.handleUsageRecorded(ctx, msg)
	case strings.HasPrefix(subject, "lgw.provider."):
		return c.handleProviderFallback(ctx, msg)
	case strings.HasPrefix(subject, "lgw.budget."):
		return c.handleBudgetThreshold(ctx, msg)
	case strings.HasPrefix(subject, "lgw.config."):
		return c.handleConfigChanged(ctx, msg)
	default:
		return c.handleGenericEvent(ctx, msg)
	}
}

func (c *Consumer) handleUsageRecorded(ctx context.Context, msg jetstream.Msg) error {
	var evt eventsv1.UsageRecorded
	if err := unmarshalEvent(msg.Data(), &evt); err != nil {
		c.logger.Error("failed to unmarshal usage recorded event", slog.Any("error", err))
		return err
	}

	occurredAt := time.Now().UTC()
	if evt.OccurredAt != nil {
		occurredAt = evt.OccurredAt.AsTime().UTC()
	}

	source := evt.Source
	if source == "" {
		source = string(core.SourceGateway)
	}

	costKind := evt.CostKind
	if costKind == "" {
		costKind = string(core.CostKindCobrado)
	} else if norm, err := core.NormalizeCostKind(costKind); err == nil {
		costKind = string(norm)
	}

	rec := store.UsageRecord{
		RequestID:        evt.RequestId,
		KeyID:            evt.KeyId,
		Project:          evt.Project,
		ModelRequested:   evt.ModelRequested,
		ModelUsed:        evt.ModelUsed,
		Provider:         evt.Provider,
		Source:           source,
		CostKind:         costKind,
		InputTokens:      evt.InputTokens,
		OutputTokens:     evt.OutputTokens,
		CacheReadTokens:  evt.CacheReadTokens,
		CacheWriteTokens: evt.CacheWriteTokens,
		CostMicros:       evt.CostMicros,
		LatencyMs:        evt.LatencyMs,
		Status:           evt.Status,
		CacheHit:         evt.CacheHit,
		FallbackFrom:     evt.FallbackFrom,
		CreatedAt:        occurredAt,
	}

	inserted, err := c.store.RecordUsage(ctx, rec)
	if err != nil {
		return fmt.Errorf("failed to record usage in store: %w", err)
	}

	if !inserted {
		c.logger.Debug("ignoring duplicate usage recorded event", slog.String("request_id", evt.RequestId))
	}

	_ = c.store.RecordAudit(ctx, store.AuditEvent{
		ID:          "audit-" + uuid.NewString(),
		EventType:   "lgw.usage.recorded.v1",
		Subject:     msg.Subject(),
		PayloadJSON: string(msg.Data()),
		CreatedAt:   time.Now().UTC(),
	})

	return nil
}

func (c *Consumer) handleProviderFallback(ctx context.Context, msg jetstream.Msg) error {
	var evt eventsv1.ProviderFallback
	if err := unmarshalEvent(msg.Data(), &evt); err != nil {
		c.logger.Error("failed to unmarshal provider fallback event", slog.Any("error", err))
		return err
	}

	return c.store.RecordAudit(ctx, store.AuditEvent{
		ID:          "audit-" + uuid.NewString(),
		EventType:   "lgw.provider.fallback.v1",
		Subject:     msg.Subject(),
		PayloadJSON: string(msg.Data()),
		CreatedAt:   time.Now().UTC(),
	})
}

func (c *Consumer) handleBudgetThreshold(ctx context.Context, msg jetstream.Msg) error {
	var evt eventsv1.BudgetThreshold
	if err := unmarshalEvent(msg.Data(), &evt); err != nil {
		c.logger.Error("failed to unmarshal budget threshold event", slog.Any("error", err))
		return err
	}

	return c.store.RecordAudit(ctx, store.AuditEvent{
		ID:          "audit-" + uuid.NewString(),
		EventType:   "lgw.budget.threshold.v1",
		Subject:     msg.Subject(),
		PayloadJSON: string(msg.Data()),
		CreatedAt:   time.Now().UTC(),
	})
}

func (c *Consumer) handleConfigChanged(ctx context.Context, msg jetstream.Msg) error {
	var evt eventsv1.ConfigChanged
	if err := unmarshalEvent(msg.Data(), &evt); err != nil {
		c.logger.Error("failed to unmarshal config changed event", slog.Any("error", err))
		return err
	}

	return c.store.RecordAudit(ctx, store.AuditEvent{
		ID:          "audit-" + uuid.NewString(),
		EventType:   "lgw.config.changed.v1",
		Subject:     msg.Subject(),
		PayloadJSON: string(msg.Data()),
		CreatedAt:   time.Now().UTC(),
	})
}

func (c *Consumer) handleGenericEvent(ctx context.Context, msg jetstream.Msg) error {
	return c.store.RecordAudit(ctx, store.AuditEvent{
		ID:          "audit-" + uuid.NewString(),
		EventType:   msg.Subject(),
		Subject:     msg.Subject(),
		PayloadJSON: string(msg.Data()),
		CreatedAt:   time.Now().UTC(),
	})
}
