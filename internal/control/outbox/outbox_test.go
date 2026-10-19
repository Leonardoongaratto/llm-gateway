package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/outbox"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

func setupTestEnv(t *testing.T) (*store.Store, jetstream.JetStream) {
	t.Helper()
	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-outbox-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	_, natsURL := natsx.StartEmbeddedServer(t)
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	if err := natsx.InitStreams(ctx, js, natsx.WithMemoryStorage()); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}

	return store.New(db), js
}

func TestRelay_EmptyOutbox(t *testing.T) {
	s, js := setupTestEnv(t)
	relay := outbox.NewRelay(s, js)

	ctx := context.Background()
	n, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error on empty relay: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 published messages, got %d", n)
	}
}

func verifyConsumedConfigEvent(ctx context.Context, t *testing.T, js jetstream.JetStream, expectedKeyID, expectedMsgID string) {
	t.Helper()

	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamConfig, jetstream.ConsumerConfig{
		Durable:       "test-config-reader",
		FilterSubject: "lgw.config.changed.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create test consumer: %v", err)
	}

	msgBatch, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch message from jetstream: %v", err)
	}

	var received jetstream.Msg
	for msg := range msgBatch.Messages() {
		received = msg
		_ = msg.Ack()
		break
	}
	if received == nil {
		t.Fatal("expected to receive config changed message from JetStream")
	}

	if expectedMsgID != "" && received.Headers().Get("Nats-Msg-Id") != expectedMsgID {
		t.Errorf("expected Nats-Msg-Id header %q, got %q", expectedMsgID, received.Headers().Get("Nats-Msg-Id"))
	}

	var event eventsv1.ConfigChanged
	if err := protojson.Unmarshal(received.Data(), &event); err != nil {
		t.Fatalf("failed to unmarshal consumed event: %v", err)
	}
	if event.Kind != eventsv1.ConfigChanged_KEY {
		t.Errorf("expected Kind KEY, got %v", event.Kind)
	}
	if len(event.KeyLimits) != 1 || event.KeyLimits[0].KeyId != expectedKeyID {
		t.Errorf("unexpected key limits in event: %+v", event.KeyLimits)
	}
}

func TestRelay_PublishesAndMarks(t *testing.T) {
	s, js := setupTestEnv(t)
	ctx := context.Background()

	key := core.VirtualKey{
		ID:      "k-relay-test",
		Name:    "Relay Test Key",
		KeyHash: "relay-hash",
	}
	if _, err := s.CreateKey(ctx, key); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	pendingBefore, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pendingBefore) != 1 {
		t.Fatalf("expected 1 pending outbox message, got %d (err: %v)", len(pendingBefore), err)
	}

	relay := outbox.NewRelay(s, js,
		outbox.WithRelayBatchSize(10),
		outbox.WithRelayInterval(50*time.Millisecond),
		outbox.WithRelayLogger(slog.Default()),
	)

	publishedCount, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay failed: %v", err)
	}
	if publishedCount != 1 {
		t.Fatalf("expected 1 published message, got %d", publishedCount)
	}

	pendingAfter, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pendingAfter) != 0 {
		t.Fatalf("expected 0 pending outbox messages after relay, got %d", len(pendingAfter))
	}

	verifyConsumedConfigEvent(ctx, t, js, key.ID, pendingBefore[0].ID)
}

func TestRelay_RunContextCancellation(t *testing.T) {
	s, js := setupTestEnv(t)
	relay := outbox.NewRelay(s, js, outbox.WithRelayInterval(10*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := relay.Run(ctx); err != nil {
		t.Errorf("expected clean exit on context cancellation, got %v", err)
	}
}

type failingOutboxStore struct {
	fetchErr bool
	markErr  bool
}

func (f *failingOutboxStore) FetchPendingOutbox(_ context.Context, _ int) ([]store.OutboxMessage, error) {
	if f.fetchErr {
		return nil, errors.New("fetch error")
	}
	return []store.OutboxMessage{
		{
			ID:        "msg-1",
			Subject:   "lgw.config.changed.v1",
			Payload:   `{"kind":1,"version":1}`,
			CreatedAt: time.Now(),
		},
	}, nil
}

func (f *failingOutboxStore) MarkOutboxPublished(_ context.Context, _ []string, _ time.Time) error {
	if f.markErr {
		return errors.New("mark error")
	}
	return nil
}

func TestRelay_StoreErrors(t *testing.T) {
	_, js := setupTestEnv(t)
	ctx := context.Background()

	fetchFailStore := &failingOutboxStore{fetchErr: true}
	relayFetch := outbox.NewRelay(fetchFailStore, js)
	if _, err := relayFetch.RelayOnce(ctx); err == nil {
		t.Error("expected error when fetch fails")
	}

	markFailStore := &failingOutboxStore{markErr: true}
	relayMark := outbox.NewRelay(markFailStore, js)
	if _, err := relayMark.RelayOnce(ctx); err == nil {
		t.Error("expected error when mark published fails")
	}
}
