package natsx

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	StreamUsage  = "LGW_USAGE"
	StreamBudget = "LGW_BUDGET"
	StreamConfig = "LGW_CONFIG"

	MaxDeliverDefault = 5
)

// StartEmbeddedServer starts an in-process NATS server with JetStream for unit/integration tests.
func StartEmbeddedServer(t testing.TB) (*server.Server, string) {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(t.TempDir(), "nats-jetstream"),
		NoLog:     true,
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create embedded nats server: %v", err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded nats server failed to become ready for connections")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return srv, srv.ClientURL()
}

// Connect establishes a connection to NATS and initializes JetStream.
func Connect(url string, opts ...nats.Option) (*nats.Conn, jetstream.JetStream, error) {
	defaultOpts := []nats.Option{
		nats.MaxReconnects(10),
		nats.ReconnectWait(500 * time.Millisecond),
	}
	allOpts := append(defaultOpts, opts...)

	nc, err := nats.Connect(url, allOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("nats connect failed: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("jetstream init failed: %w", err)
	}

	return nc, js, nil
}

// InitStreams creates the standard LGW streams idempotently.
func InitStreams(ctx context.Context, js jetstream.JetStream) error {
	streamConfigs := []jetstream.StreamConfig{
		{
			Name:        StreamUsage,
			Description: "LLM Gateway usage and provider fallback events",
			Subjects:    []string{"lgw.usage.>", "lgw.provider.>"},
			Storage:     jetstream.MemoryStorage,
			Duplicates:  2 * time.Minute,
		},
		{
			Name:        StreamBudget,
			Description: "LLM Gateway budget threshold events",
			Subjects:    []string{"lgw.budget.>"},
			Storage:     jetstream.MemoryStorage,
			Duplicates:  2 * time.Minute,
		},
		{
			Name:        StreamConfig,
			Description: "LLM Gateway configuration change events",
			Subjects:    []string{"lgw.config.>"},
			Storage:     jetstream.MemoryStorage,
			Duplicates:  2 * time.Minute,
		},
	}

	for _, cfg := range streamConfigs {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("failed to create/update stream %s: %w", cfg.Name, err)
		}
	}

	return nil
}

// Publish publishes a message with Nats-Msg-Id deduplication and W3C trace injection.
func Publish(ctx context.Context, js jetstream.JetStream, subject string, data []byte, msgID string) (*jetstream.PubAck, error) {
	msg := nats.NewMsg(subject)
	msg.Data = data

	if msgID != "" {
		msg.Header.Set("Nats-Msg-Id", msgID)
	}

	// Inject W3C Trace Context
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))

	ack, err := js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("publish failed on %s: %w", subject, err)
	}

	return ack, nil
}

// ExtractTraceContext extracts W3C trace context from a NATS message header.
func ExtractTraceContext(ctx context.Context, msg jetstream.Msg) context.Context {
	headers := msg.Headers()
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(http.Header(headers)))
}

// DLQSubject returns the dead-letter queue subject for a given subject.
func DLQSubject(subject string) string {
	return "lgw.dlq." + subject
}

type MessageHandler func(ctx context.Context, msg jetstream.Msg) error

// ConsumeWithDLQ consumes messages from a consumer, retrying with backoff up to maxDeliver,
// and moving to DLQ when maxDeliver is exceeded.
func ConsumeWithDLQ(ctx context.Context, js jetstream.JetStream, consumer jetstream.Consumer, handler MessageHandler, maxDeliver int) (jetstream.ConsumeContext, error) {
	if maxDeliver <= 0 {
		maxDeliver = MaxDeliverDefault
	}

	return consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			_ = msg.Nak()
			return
		}

		msgCtx := ExtractTraceContext(ctx, msg)

		handlerErr := handler(msgCtx, msg)
		if handlerErr == nil {
			_ = msg.Ack()
			return
		}

		// If attempts exceed or equal maxDeliver, forward to DLQ and terminate
		if int(meta.NumDelivered) >= maxDeliver {
			dlqSubj := DLQSubject(msg.Subject())
			// Publish directly to NATS on DLQ subject
			dlqMsg := nats.NewMsg(dlqSubj)
			dlqMsg.Data = msg.Data()
			dlqMsg.Header = msg.Headers()
			if dlqMsg.Header == nil {
				dlqMsg.Header = nats.Header{}
			}
			dlqMsg.Header.Set("X-DLQ-Original-Subject", msg.Subject())
			dlqMsg.Header.Set("X-DLQ-Reason", handlerErr.Error())
			dlqMsg.Header.Set("X-DLQ-Attempts", fmt.Sprintf("%d", meta.NumDelivered))

			_, _ = js.PublishMsg(ctx, dlqMsg)
			_ = msg.Term()
			return
		}

		// Otherwise, nack with backoff
		backoff := time.Duration(meta.NumDelivered) * 5 * time.Millisecond
		_ = msg.NakWithDelay(backoff)
	})
}
