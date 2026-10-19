package consumer_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/consumer"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupTestEnvironment(t *testing.T) (jetstream.JetStream, *store.Store) {
	t.Helper()
	ctx := context.Background()

	_, natsURL := natsx.StartEmbeddedServer(t)
	_, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}

	if err := natsx.InitStreams(ctx, js, natsx.WithMemoryStorage()); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-consumer-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}

	return js, store.New(db)
}

func publishProtoEvent(ctx context.Context, js jetstream.JetStream, subject, msgID string, m any) error {
	var (
		data []byte
		err  error
	)
	if pm, ok := m.(proto.Message); ok {
		opts := protojson.MarshalOptions{UseProtoNames: true}
		data, err = opts.Marshal(pm)
	} else {
		data = []byte(fmt.Sprint(m))
	}
	if err != nil {
		return err
	}
	_, err = natsx.Publish(ctx, js, subject, data, msgID)
	return err
}

func testAC1E01AtomicConsumption(ctx context.Context, t *testing.T, js jetstream.JetStream, st *store.Store) {
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.UTC)
	evt := &eventsv1.UsageRecorded{
		RequestId:        "req-c1",
		OccurredAt:       timestamppb.New(now),
		Source:           "gateway",
		CostKind:         "cobrado",
		KeyId:            "key-user-1",
		ModelRequested:   "claude-3-5-sonnet",
		ModelUsed:        "claude-3-5-sonnet-20241022",
		Provider:         "anthropic",
		InputTokens:      1000,
		OutputTokens:     200,
		CacheReadTokens:  50,
		CacheWriteTokens: 10,
		CostMicros:       6000,
		Status:           "success",
	}

	if err := publishProtoEvent(ctx, js, "lgw.usage.recorded.v1", "msg-1", evt); err != nil {
		t.Fatalf("failed to publish usage event: %v", err)
	}

	// Poll briefly for consumption
	var records []store.UsageRecord
	var err error
	for i := 0; i < 50; i++ {
		records, _, err = st.QueryUsage(ctx, store.UsageFilter{KeyID: "key-user-1"})
		if err == nil && len(records) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 usage record consumed, got %d (err: %v)", len(records), err)
	}

	aggs, err := st.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs) != 1 {
		t.Fatalf("failed to query daily aggregates: %v", err)
	}
	if aggs[0].Requests != 1 || aggs[0].CostMicros != 6000 {
		t.Errorf("unexpected aggregate values: %+v", aggs[0])
	}
}

func testAC1E02DuplicateDelivery(ctx context.Context, t *testing.T, js jetstream.JetStream, st *store.Store) {
	now := time.Date(2026, 10, 18, 14, 0, 0, 0, time.UTC)
	evt := &eventsv1.UsageRecorded{
		RequestId:   "req-c1", // Same RequestID as AC-1E-01
		OccurredAt:  timestamppb.New(now),
		Source:      "gateway",
		CostKind:    "cobrado",
		KeyId:       "key-user-1",
		ModelUsed:   "claude-3-5-sonnet-20241022",
		Provider:    "anthropic",
		InputTokens: 1000,
		CostMicros:  6000,
		Status:      "success",
	}

	// Publish duplicate delivery with different Nats-Msg-Id to bypass stream dedup and test consumer-level idempotency
	if err := publishProtoEvent(ctx, js, "lgw.usage.recorded.v1", "msg-1-dup", evt); err != nil {
		t.Fatalf("failed to publish duplicate event: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	records, total, err := st.QueryUsage(ctx, store.UsageFilter{KeyID: "key-user-1"})
	if err != nil || total != 1 || len(records) != 1 {
		t.Fatalf("expected exactly 1 usage record after duplicate delivery: total=%d, len=%d, err=%v", total, len(records), err)
	}

	aggs, err := st.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs) != 1 {
		t.Fatalf("failed to query daily aggregates: %v", err)
	}
	if aggs[0].Requests != 1 || aggs[0].CostMicros != 6000 {
		t.Errorf("expected daily aggregates not to double count: %+v", aggs[0])
	}
}

func testOtherEventTypes(ctx context.Context, t *testing.T, js jetstream.JetStream, st *store.Store) {
	// Fallback event
	fbEvt := &eventsv1.ProviderFallback{
		RequestId:    "req-fb-1",
		OccurredAt:   timestamppb.Now(),
		FromProvider: "anthropic",
		ToProvider:   "openai",
		Reason:       "rate limit",
	}
	if err := publishProtoEvent(ctx, js, "lgw.provider.fallback.v1", "msg-fb-1", fbEvt); err != nil {
		t.Fatalf("failed to publish fallback: %v", err)
	}

	// Budget threshold event
	btEvt := &eventsv1.BudgetThreshold{
		KeyId:       "key-thresh",
		Period:      "2026-10",
		Level:       80,
		SpentMicros: 80000000,
		LimitMicros: 100000000,
		OccurredAt:  timestamppb.Now(),
	}
	if err := publishProtoEvent(ctx, js, "lgw.budget.threshold.v1", "msg-bt-1", btEvt); err != nil {
		t.Fatalf("failed to publish threshold: %v", err)
	}

	// Config changed event
	cfgEvt := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_KEY,
		Version: 42,
	}
	if err := publishProtoEvent(ctx, js, "lgw.config.changed.v1", "msg-cfg-1", cfgEvt); err != nil {
		t.Fatalf("failed to publish config: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	var count int
	err := st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM events_audit WHERE event_type LIKE 'lgw.%'").Scan(&count)
	if err != nil || count < 3 {
		t.Errorf("expected at least 3 audit events, got %d, err %v", count, err)
	}
}

func TestConsumer_AC1E01_And_AC1E02(t *testing.T) {
	ctx := context.Background()
	js, st := setupTestEnvironment(t)

	c := consumer.New(js, st, nil)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer c.Stop()

	// Calling Start again should be a safe no-op
	if err := c.Start(ctx); err != nil {
		t.Fatalf("expected second start to be no-op: %v", err)
	}

	t.Run("AC-1E-01: atomic consumption of usage.recorded", func(t *testing.T) {
		testAC1E01AtomicConsumption(ctx, t, js, st)
	})

	t.Run("AC-1E-02: duplicate delivery idempotency", func(t *testing.T) {
		testAC1E02DuplicateDelivery(ctx, t, js, st)
	})

	t.Run("Audit other event types", func(t *testing.T) {
		testOtherEventTypes(ctx, t, js, st)
	})
}

func TestConsumer_AC1E03_OfflineResilience(t *testing.T) {
	ctx := context.Background()
	js, st := setupTestEnvironment(t)

	// Publish 100 events while the usage consumer is completely offline
	const count = 100
	now := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)

	for i := 0; i < count; i++ {
		reqID := fmt.Sprintf("offline-req-%03d", i)
		evt := &eventsv1.UsageRecorded{
			RequestId:    reqID,
			OccurredAt:   timestamppb.New(now),
			Source:       "gateway",
			CostKind:     "cobrado",
			KeyId:        "key-offline-test",
			ModelUsed:    "claude-3-5-sonnet-20241022",
			Provider:     "anthropic",
			InputTokens:  100,
			OutputTokens: 20,
			CostMicros:   500,
			Status:       "success",
		}
		if err := publishProtoEvent(ctx, js, "lgw.usage.recorded.v1", fmt.Sprintf("off-msg-%d", i), evt); err != nil {
			t.Fatalf("failed to publish offline event %d: %v", i, err)
		}
	}

	// Now start consumer - it should consume all 100 pending messages
	c := consumer.New(js, st, nil)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("failed to start consumer: %v", err)
	}
	defer c.Stop()

	// Poll until all 100 records are consumed
	var total int
	var err error
	for i := 0; i < 100; i++ {
		_, total, err = st.QueryUsage(ctx, store.UsageFilter{KeyID: "key-offline-test", Limit: 1000})
		if err == nil && total >= count {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if total != count {
		t.Fatalf("AC-1E-03 failed: expected %d records consumed after coming online, got %d (err: %v)", count, total, err)
	}

	// Verify aggregate
	aggs, err := st.QueryDailyAggregates(ctx, "2026-10-19", "2026-10-19")
	if err != nil || len(aggs) != 1 {
		t.Fatalf("failed to query aggregates: %v", err)
	}
	if aggs[0].Requests != count || aggs[0].CostMicros != count*500 {
		t.Errorf("unexpected aggregate: requests=%d, micros=%d", aggs[0].Requests, aggs[0].CostMicros)
	}
}

type mockMsg struct {
	subject string
	data    []byte
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *mockMsg) Data() []byte                              { return m.data }
func (m *mockMsg) Headers() nats.Header                      { return nil }
func (m *mockMsg) Subject() string                           { return m.subject }
func (m *mockMsg) Reply() string                             { return "" }
func (m *mockMsg) Ack() error                                { return nil }
func (m *mockMsg) DoubleAck(context.Context) error           { return nil }
func (m *mockMsg) Nak() error                                { return nil }
func (m *mockMsg) NakWithDelay(time.Duration) error          { return nil }
func (m *mockMsg) InProgress() error                         { return nil }
func (m *mockMsg) Term() error                               { return nil }
func (m *mockMsg) TermWithReason(string) error               { return nil }

func TestConsumer_Dispatch_ErrorsAndGeneric(t *testing.T) {
	ctx := context.Background()
	_, st := setupTestEnvironment(t)
	c := consumer.New(nil, st, nil)

	invalidJSON := []byte(`{invalid json`)

	// Invalid usage
	if err := c.Dispatch(ctx, &mockMsg{subject: "lgw.usage.recorded.v1", data: invalidJSON}); err == nil {
		t.Errorf("expected error for invalid usage payload")
	}

	// Invalid fallback
	if err := c.Dispatch(ctx, &mockMsg{subject: "lgw.provider.fallback.v1", data: invalidJSON}); err == nil {
		t.Errorf("expected error for invalid fallback payload")
	}

	// Invalid threshold
	if err := c.Dispatch(ctx, &mockMsg{subject: "lgw.budget.threshold.v1", data: invalidJSON}); err == nil {
		t.Errorf("expected error for invalid threshold payload")
	}

	// Invalid config
	if err := c.Dispatch(ctx, &mockMsg{subject: "lgw.config.changed.v1", data: invalidJSON}); err == nil {
		t.Errorf("expected error for invalid config payload")
	}

	// Generic subject
	if err := c.Dispatch(ctx, &mockMsg{subject: "lgw.other.custom.v1", data: []byte(`{"hello": "world"}`)}); err != nil {
		t.Errorf("unexpected error on generic dispatch: %v", err)
	}
}
