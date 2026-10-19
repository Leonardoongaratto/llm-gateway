package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	DefaultQueueSize = 2048
	SubjectUsage     = "lgw.usage.recorded.v1"
	SubjectFallback  = "lgw.provider.fallback.v1"
)

type eventTask struct {
	subject string
	msgID   string
	payload []byte
}

// Option configures the event Publisher.
type Option func(*Publisher)

// WithQueueSize sets the asynchronous event queue capacity.
func WithQueueSize(size int) Option {
	return func(p *Publisher) {
		if size > 0 {
			p.queueSize = size
		}
	}
}

// WithLogger sets the logger for the Publisher.
func WithLogger(l *slog.Logger) Option {
	return func(p *Publisher) {
		if l != nil {
			p.logger = l
		}
	}
}

// Publisher is an asynchronous event publisher for NATS JetStream.
type Publisher struct {
	js        jetstream.JetStream
	queueSize int
	queue     chan eventTask
	wg        sync.WaitGroup
	closeOnce sync.Once
	logger    *slog.Logger
}

// NewPublisher creates and starts an asynchronous event publisher.
func NewPublisher(js jetstream.JetStream, opts ...Option) *Publisher {
	p := &Publisher{
		js:        js,
		queueSize: DefaultQueueSize,
		logger:    slog.Default(),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.queue = make(chan eventTask, p.queueSize)
	p.wg.Add(1)
	go p.worker()

	return p
}

func (p *Publisher) worker() {
	defer p.wg.Done()
	jsonOpts := protojson.MarshalOptions{UseProtoNames: true}
	_ = jsonOpts

	for task := range p.queue {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := natsx.Publish(ctx, p.js, task.subject, task.payload, task.msgID); err != nil {
			p.logger.Error("failed to publish jetstream event",
				slog.String("subject", task.subject),
				slog.String("msg_id", task.msgID),
				slog.Any("error", err),
			)
		}
		cancel()
	}
}

// PublishUsage publishes a usage.recorded event without blocking the caller.
func (p *Publisher) PublishUsage(_ context.Context, evt *eventsv1.UsageRecorded) {
	if evt == nil || p.js == nil {
		return
	}

	opts := protojson.MarshalOptions{UseProtoNames: true}
	payload, err := opts.Marshal(evt)
	if err != nil {
		p.logger.Error("failed to marshal usage recorded event", slog.Any("error", err))
		return
	}

	msgID := fmt.Sprintf("usage-%s", evt.RequestId)
	select {
	case p.queue <- eventTask{subject: SubjectUsage, msgID: msgID, payload: payload}:
	default:
		p.logger.Warn("event queue full, dropping usage event", slog.String("request_id", evt.RequestId))
	}
}

// PublishFallback publishes a provider.fallback event without blocking the caller.
func (p *Publisher) PublishFallback(_ context.Context, evt *eventsv1.ProviderFallback) {
	if evt == nil || p.js == nil {
		return
	}

	opts := protojson.MarshalOptions{UseProtoNames: true}
	payload, err := opts.Marshal(evt)
	if err != nil {
		p.logger.Error("failed to marshal provider fallback event", slog.Any("error", err))
		return
	}

	msgID := fmt.Sprintf("fallback-%s-%s-%d", evt.RequestId, evt.ToProvider, time.Now().UnixNano())
	select {
	case p.queue <- eventTask{subject: SubjectFallback, msgID: msgID, payload: payload}:
	default:
		p.logger.Warn("event queue full, dropping fallback event", slog.String("request_id", evt.RequestId))
	}
}

// Close gracefully closes the event queue and waits for pending events to be published.
func (p *Publisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.queue)
		p.wg.Wait()
	})
	return nil
}
