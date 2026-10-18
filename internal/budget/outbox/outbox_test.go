package outbox_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/outbox"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

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

func TestAC1B07_OutboxRelay_PublishesAndMarks(t *testing.T) {
	s, js := setupTestEnv(t)
	ctx := context.Background()

	keyID := "k-outbox-test"
	limits := core.KeyLimits{
		KeyID:            keyID,
		DailyLimitMicros: 10000,
	}
	if err := s.SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	now := time.Now().UTC()
	res, err := s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 8500,
		RequestID: "req-relay-1",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if _, err := s.Settle(ctx, res.ReservationID, 8500, now); err != nil {
		t.Fatalf("settle failed: %v", err)
	}

	pendingBefore, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pendingBefore) != 1 {
		t.Fatalf("expected 1 pending outbox message, got %d (err: %v)", len(pendingBefore), err)
	}

	relay := outbox.NewRelay(s, js)
	publishedCount, err := relay.RelayOnce(ctx)
	if err != nil {
		t.Fatalf("relay failed: %v", err)
	}
	if publishedCount != 1 {
		t.Fatalf("expected 1 published message, got %d", publishedCount)
	}

	pendingAfter, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pendingAfter) != 0 {
		t.Errorf("expected 0 pending outbox messages after relay, got %d", len(pendingAfter))
	}
}

func TestAC1B09_ConfigConsumer_UpdatesLimits(t *testing.T) {
	s, js := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumer, err := outbox.NewConfigConsumer(s, js, outbox.WithConsumerLogger(nil))
	if err != nil {
		t.Fatalf("failed to create config consumer: %v", err)
	}

	consumeCtx, err := consumer.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start config consumer: %v", err)
	}
	defer consumeCtx.Stop()

	cfgEvent := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_KEY,
		Version: 1,
		KeyLimits: []*eventsv1.KeyLimit{
			{
				KeyId:              "k-new-limits",
				DailyLimitMicros:   25000,
				MonthlyLimitMicros: 250000,
				Rpm:                120,
			},
		},
	}
	payload, err := protojson.Marshal(cfgEvent)
	if err != nil {
		t.Fatalf("failed to marshal protojson: %v", err)
	}

	_, err = natsx.Publish(ctx, js, "lgw.config.changed.v1", payload, "cfg-msg-1")
	if err != nil {
		t.Fatalf("failed to publish config changed event: %v", err)
	}

	var updated bool
	for attempt := 0; attempt < 40; attempt++ {
		time.Sleep(50 * time.Millisecond)
		rem, err := s.Remaining(ctx, "k-new-limits", time.Now().UTC())
		if err == nil && rem.DailyLimitMicros == 25000 && rem.MonthlyLimitMicros == 250000 {
			updated = true
			break
		}
	}

	if !updated {
		t.Fatal("config consumer failed to update key limits within 2 seconds")
	}
}

func TestConfigConsumer_MalformedPayload_Terminated(t *testing.T) {
	s, js := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var logBuf syncBuffer
	testLogger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	consumer, err := outbox.NewConfigConsumer(s, js, outbox.WithConsumerLogger(testLogger))
	if err != nil {
		t.Fatalf("failed to create config consumer: %v", err)
	}

	consumeCtx, err := consumer.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start config consumer: %v", err)
	}
	defer consumeCtx.Stop()

	_, err = natsx.Publish(ctx, js, "lgw.config.changed.v1", []byte("invalid-non-json-payload"), "poison-1")
	if err != nil {
		t.Fatalf("failed to publish poison pill: %v", err)
	}

	logged := false
	for i := 0; i < 20; i++ {
		time.Sleep(20 * time.Millisecond)
		if strings.Contains(logBuf.String(), "malformed config event, terminating poison pill") {
			logged = true
			break
		}
	}
	if !logged {
		t.Fatal("expected poison pill termination log message")
	}

	time.Sleep(200 * time.Millisecond)
	count := strings.Count(logBuf.String(), "malformed config event, terminating poison pill")
	if count != 1 {
		t.Fatalf("expected exactly 1 termination log without redeliveries, got %d", count)
	}
}

func TestRelay_Run_GracefulShutdownAndOptions(t *testing.T) {
	s, js := setupTestEnv(t)
	r := outbox.NewRelay(s, js,
		outbox.WithRelayBatchSize(10),
		outbox.WithRelayInterval(10*time.Millisecond),
		outbox.WithRelayLogger(nil),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- r.Run(ctx)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error on cancel: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("relay failed to shutdown within timeout")
	}
}
