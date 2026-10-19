package events_test

import (
	"context"
	"strings"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/events"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupTestNats(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	srv, natsURL := natsx.StartEmbeddedServer(t)
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := natsx.InitStreams(ctx, js); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}
	return srv, nc, js
}

func TestPublisher_AC1D12_PublishUsageRecorded_NoContentOrKeys(t *testing.T) {
	_, _, js := setupTestNats(t)

	pub := events.NewPublisher(js, events.WithQueueSize(100))
	defer func() { _ = pub.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	evt := &eventsv1.UsageRecorded{
		RequestId:      "req-evt-123",
		OccurredAt:     timestamppb.Now(),
		Source:         "gateway",
		CostKind:       "billed",
		KeyId:          "key-id-456",
		Project:        "test-project",
		ModelRequested: "rapido",
		ModelUsed:      "claude-3-5-sonnet",
		Provider:       "anthropic",
		InputTokens:    1000,
		OutputTokens:   500,
		CostMicros:     10500,
		LatencyMs:      45,
		Status:         "ok",
		CacheHit:       false,
		FallbackFrom:   "openai",
	}

	pub.PublishUsage(ctx, evt)

	// Flush / Close to ensure message is delivered
	if err := pub.Close(); err != nil {
		t.Fatalf("failed to close publisher: %v", err)
	}

	// Consume and verify payload from stream
	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamUsage, jetstream.ConsumerConfig{
		Durable:       "test-usage-verifier",
		FilterSubject: "lgw.usage.recorded.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	msg, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("failed to fetch usage message from jetstream: %v", err)
	}

	rawPayload := string(msg.Data())

	// AC-1D-12 Verification: NEVER leak prompt, message, completion, or raw key in event
	for _, forbidden := range []string{"prompt", "completion", "content", "message", "lgw_"} {
		if strings.Contains(strings.ToLower(rawPayload), `"`+forbidden+`"`) {
			t.Fatalf("AC-1D-12 violation: raw payload contains forbidden token %s: %s", forbidden, rawPayload)
		}
	}

	var parsed eventsv1.UsageRecorded
	if err := protojson.Unmarshal(msg.Data(), &parsed); err != nil {
		t.Fatalf("failed to unmarshal protojson event: %v", err)
	}

	if parsed.RequestId != "req-evt-123" || parsed.CostMicros != 10500 || parsed.Provider != "anthropic" {
		t.Errorf("unexpected event content: %+v", &parsed)
	}
}

func TestPublisher_PublishFallback(t *testing.T) {
	_, _, js := setupTestNats(t)

	pub := events.NewPublisher(js, events.WithQueueSize(100))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fbEvt := &eventsv1.ProviderFallback{
		RequestId:    "req-fb-999",
		OccurredAt:   timestamppb.Now(),
		FromProvider: "primary-prov",
		ToProvider:   "secondary-prov",
		Reason:       "upstream 503",
	}

	pub.PublishFallback(ctx, fbEvt)

	if err := pub.Close(); err != nil {
		t.Fatalf("failed to close publisher: %v", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamUsage, jetstream.ConsumerConfig{
		Durable:       "test-fallback-verifier",
		FilterSubject: "lgw.provider.fallback.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	msg, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("failed to fetch fallback message: %v", err)
	}

	var parsed eventsv1.ProviderFallback
	if err := protojson.Unmarshal(msg.Data(), &parsed); err != nil {
		t.Fatalf("failed to unmarshal fallback event: %v", err)
	}

	if parsed.RequestId != "req-fb-999" || parsed.FromProvider != "primary-prov" || parsed.ToProvider != "secondary-prov" {
		t.Errorf("unexpected fallback event: %+v", &parsed)
	}
}

func TestPublisher_OptionsAndNil(t *testing.T) {
	pub := events.NewPublisher(nil, events.WithLogger(nil), events.WithQueueSize(50))

	// Should not panic on nil events
	pub.PublishUsage(context.Background(), nil)
	pub.PublishFallback(context.Background(), nil)

	if err := pub.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
}
