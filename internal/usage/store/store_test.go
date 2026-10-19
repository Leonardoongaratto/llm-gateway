package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func createTestDBs(t *testing.T) map[string]*sql.DB {
	t.Helper()
	dbs := make(map[string]*sql.DB)

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-usage-%d.db", time.Now().UnixNano()))
	sqliteDB, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	dbs["sqlite"] = sqliteDB

	if pgDSN := os.Getenv("LGW_TEST_PG_DSN"); pgDSN != "" {
		pgDB, err := sqlx.OpenPostgres(pgDSN)
		if err != nil {
			t.Fatalf("failed to open postgres: %v", err)
		}
		dbs["postgres"] = pgDB
	}

	for driver, db := range dbs {
		ctx := context.Background()
		if err := store.Migrate(ctx, db); err != nil {
			t.Fatalf("[%s] migrate 1 failed: %v", driver, err)
		}
		if err := store.Migrate(ctx, db); err != nil {
			t.Fatalf("[%s] migrate 2 (idempotency) failed: %v", driver, err)
		}
	}

	return dbs
}

func testRecordUsageIdempotency(ctx context.Context, t *testing.T, s *store.Store) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	rec := store.UsageRecord{
		RequestID:        "req-1",
		KeyID:            "key-dev",
		Project:          "proj-a",
		ModelRequested:   "claude-3-5-sonnet",
		ModelUsed:        "claude-3-5-sonnet-20241022",
		Provider:         "anthropic",
		Source:           "gateway",
		CostKind:         "cobrado",
		InputTokens:      1000,
		OutputTokens:     200,
		CacheReadTokens:  50,
		CacheWriteTokens: 10,
		CostMicros:       5000,
		LatencyMs:        320,
		Status:           "success",
		CacheHit:         false,
		FallbackFrom:     "",
		CreatedAt:        now,
	}

	inserted, err := s.RecordUsage(ctx, rec)
	if err != nil || !inserted {
		t.Fatalf("unexpected record usage failure: inserted=%v, err=%v", inserted, err)
	}

	insertedSecond, err := s.RecordUsage(ctx, rec)
	if err != nil || insertedSecond {
		t.Fatalf("expected duplicate record to be ignored: inserted=%v, err=%v", insertedSecond, err)
	}

	aggs, err := s.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs) != 1 {
		t.Fatalf("failed to query daily aggregates: %v", err)
	}
	if aggs[0].Requests != 1 || aggs[0].InputTokens != 1000 || aggs[0].CostMicros != 5000 {
		t.Errorf("unexpected aggregate values: %+v", aggs[0])
	}

	rec2 := rec
	rec2.RequestID = "req-2"
	rec2.InputTokens = 500
	rec2.CostMicros = 2500

	inserted2, err := s.RecordUsage(ctx, rec2)
	if err != nil || !inserted2 {
		t.Fatalf("failed to record second request: %v", err)
	}

	aggs2, err := s.QueryDailyAggregates(ctx, "2026-10-18", "2026-10-18")
	if err != nil || len(aggs2) != 1 || aggs2[0].Requests != 2 || aggs2[0].CostMicros != 7500 {
		t.Fatalf("unexpected aggregate after second request: %+v", aggs2)
	}
}

func testRecordUsageBatch(ctx context.Context, t *testing.T, s *store.Store) {
	now := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	records := []store.UsageRecord{
		{
			RequestID:  "batch-1",
			KeyID:      "key-prod",
			ModelUsed:  "gpt-4o",
			Provider:   "openai",
			Source:     "gateway",
			CostKind:   "cobrado",
			CostMicros: 10000,
			CreatedAt:  now,
		},
		{
			RequestID:  "batch-2",
			KeyID:      "key-prod",
			ModelUsed:  "gpt-4o",
			Provider:   "openai",
			Source:     "gateway",
			CostKind:   "cobrado",
			CostMicros: 15000,
			CreatedAt:  now,
		},
		{
			// duplicate in batch
			RequestID:  "batch-1",
			KeyID:      "key-prod",
			ModelUsed:  "gpt-4o",
			Provider:   "openai",
			Source:     "gateway",
			CostKind:   "cobrado",
			CostMicros: 10000,
			CreatedAt:  now,
		},
	}

	insertedCount, err := s.RecordUsageBatch(ctx, records)
	if err != nil || insertedCount != 2 {
		t.Fatalf("batch record error: count=%d, err=%v", insertedCount, err)
	}

	zeroCount, err := s.RecordUsageBatch(ctx, nil)
	if err != nil || zeroCount != 0 {
		t.Errorf("expected 0 for empty batch, got %d, err %v", zeroCount, err)
	}
}

func testQueryUsage(ctx context.Context, t *testing.T, s *store.Store) {
	res, total, err := s.QueryUsage(ctx, store.UsageFilter{
		Provider: "openai",
		Limit:    10,
	})
	if err != nil || total != 2 || len(res) != 2 {
		t.Fatalf("query usage provider failed: total=%d, len=%d, err=%v", total, len(res), err)
	}

	res1, total1, err := s.QueryUsage(ctx, store.UsageFilter{
		Provider: "openai",
		Limit:    1,
		Offset:   0,
	})
	if err != nil || len(res1) != 1 || total1 != 2 {
		t.Fatalf("pagination error: len=%d, total=%d, err=%v", len(res1), total1, err)
	}

	res2, _, err := s.QueryUsage(ctx, store.UsageFilter{
		Provider: "openai",
		Limit:    1,
		Offset:   1,
	})
	if err != nil || len(res2) != 1 || res1[0].RequestID == res2[0].RequestID {
		t.Fatalf("offset error: len=%d, err=%v", len(res2), err)
	}

	resDates, totalDates, err := s.QueryUsage(ctx, store.UsageFilter{
		StartDate: "2026-10-19T00:00:00Z",
		EndDate:   "2026-10-19T23:59:59Z",
	})
	if err != nil || totalDates != 2 || len(resDates) != 2 {
		t.Errorf("date filter error: total=%d len=%d err=%v", totalDates, len(resDates), err)
	}

	resNone, totalNone, err := s.QueryUsage(ctx, store.UsageFilter{
		KeyID: "non-existent-key",
	})
	if err != nil || totalNone != 0 || len(resNone) != 0 {
		t.Errorf("expected 0 records for non-existent key: total=%d len=%d", totalNone, len(resNone))
	}

	resDefault, _, err := s.QueryUsage(ctx, store.UsageFilter{
		Limit:  -1,
		Offset: -1,
	})
	if err != nil || len(resDefault) == 0 {
		t.Fatalf("default query error: %v", err)
	}
}

func testQueryDailyAggregates(ctx context.Context, t *testing.T, s *store.Store) {
	aggsStart, err := s.QueryDailyAggregates(ctx, "2026-10-18", "")
	if err != nil || len(aggsStart) < 2 {
		t.Fatalf("query aggregates with start only error: %v", err)
	}

	aggsEnd, err := s.QueryDailyAggregates(ctx, "", "2026-10-18")
	if err != nil || len(aggsEnd) != 1 {
		t.Fatalf("query aggregates with end only error: %v", err)
	}

	aggsAll, err := s.QueryDailyAggregates(ctx, "", "")
	if err != nil || len(aggsAll) < 2 {
		t.Fatalf("query all aggregates error: %v", err)
	}
}

func testAuditEvents(ctx context.Context, t *testing.T, s *store.Store) {
	audit := store.AuditEvent{
		ID:          "audit-1",
		EventType:   "lgw.usage.recorded.v1",
		Subject:     "lgw.events.usage",
		PayloadJSON: `{"test": true}`,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.RecordAudit(ctx, audit); err != nil {
		t.Fatalf("failed to record audit: %v", err)
	}
}

func testProcessedMessages(ctx context.Context, t *testing.T, s *store.Store) {
	msgID := "msg-100"
	stream := "LGW_EVENTS"
	consumer := "usage_consumer"

	processed, err := s.IsMessageProcessed(ctx, stream, consumer, msgID)
	if err != nil || processed {
		t.Fatalf("expected message not processed yet: %v", processed)
	}

	firstTry, err := s.TryProcessMessage(ctx, stream, consumer, msgID)
	if err != nil || !firstTry {
		t.Fatalf("expected first try to succeed: %v", firstTry)
	}

	secondTry, err := s.TryProcessMessage(ctx, stream, consumer, msgID)
	if err != nil || secondTry {
		t.Fatalf("expected second try to return false: %v", secondTry)
	}

	processedAfter, err := s.IsMessageProcessed(ctx, stream, consumer, msgID)
	if err != nil || !processedAfter {
		t.Fatalf("expected message to be processed: %v", processedAfter)
	}
}

func testImportCursors(ctx context.Context, t *testing.T, s *store.Store) {
	cursor := store.ImportCursor{
		Source:     "claude-code-local",
		FilePath:   "/path/to/project/logs.jsonl",
		ByteOffset: 1024,
		LastLine:   42,
		UpdatedAt:  time.Now().UTC(),
	}

	_, err := s.GetCursor(ctx, cursor.Source, cursor.FilePath)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := s.UpsertCursor(ctx, cursor); err != nil {
		t.Fatalf("failed to upsert cursor: %v", err)
	}

	got, err := s.GetCursor(ctx, cursor.Source, cursor.FilePath)
	if err != nil || got.ByteOffset != 1024 || got.LastLine != 42 {
		t.Fatalf("unexpected cursor data: %+v, err=%v", got, err)
	}

	cursor.ByteOffset = 2048
	cursor.LastLine = 84
	if err := s.UpsertCursor(ctx, cursor); err != nil {
		t.Fatalf("failed to update cursor: %v", err)
	}

	gotUpdated, err := s.GetCursor(ctx, cursor.Source, cursor.FilePath)
	if err != nil || gotUpdated.ByteOffset != 2048 {
		t.Fatalf("cursor offset not updated: %+v", gotUpdated)
	}

	cursors, err := s.ListCursors(ctx, cursor.Source)
	if err != nil || len(cursors) != 1 {
		t.Fatalf("failed to list cursors: %v", err)
	}
}

func testImportRuns(ctx context.Context, t *testing.T, s *store.Store) {
	run := store.ImportRun{
		ID:              "run-1",
		Source:          "claude-code",
		Status:          "running",
		LinesRead:       100,
		RecordsImported: 95,
		ErrorsCount:     5,
		ErrorDetails:    "malformed line 23",
		StartedAt:       time.Now().UTC(),
	}

	if err := s.CreateImportRun(ctx, run); err != nil {
		t.Fatalf("failed to create import run: %v", err)
	}

	now := time.Now().UTC()
	run.Status = "success"
	run.FinishedAt = &now
	if err := s.UpdateImportRun(ctx, run); err != nil {
		t.Fatalf("failed to update import run: %v", err)
	}

	runs, err := s.ListImportRuns(ctx, "claude-code", 10)
	if err != nil || len(runs) != 1 || runs[0].Status != "success" || runs[0].FinishedAt == nil {
		t.Fatalf("unexpected import runs: %+v", runs)
	}

	allRuns, err := s.ListImportRuns(ctx, "", 10)
	if err != nil || len(allRuns) < 1 {
		t.Fatalf("failed to list all runs: %v", err)
	}

	fakeRun := run
	fakeRun.ID = "non-existent"
	if err := s.UpdateImportRun(ctx, fakeRun); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expected ErrNotFound for non existent run update, got %v", err)
	}
}

func testSubscriptionsCRUD(ctx context.Context, t *testing.T, s *store.Store) {
	sub := store.Subscription{
		ID:                "sub-1",
		Name:              "Claude Pro",
		Provider:          "anthropic",
		MonthlyCostMicros: 20000000,
		RenewalDay:        15,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}

	_, err := s.GetSubscription(ctx, sub.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := s.CreateSubscription(ctx, sub); err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}

	got, err := s.GetSubscription(ctx, sub.ID)
	if err != nil || got.MonthlyCostMicros != 20000000 || got.RenewalDay != 15 {
		t.Fatalf("unexpected subscription: %+v, err=%v", got, err)
	}

	subs, err := s.ListSubscriptions(ctx)
	if err != nil || len(subs) != 1 {
		t.Fatalf("failed to list subscriptions: %v", err)
	}

	if err := s.DeleteSubscription(ctx, sub.ID); err != nil {
		t.Fatalf("failed to delete subscription: %v", err)
	}

	if err := s.DeleteSubscription(ctx, sub.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on second delete, got %v", err)
	}
}

func runAllStoreContractTests(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	s := store.New(db)
	if s.DB() != db {
		t.Fatalf("expected s.DB() == db")
	}

	t.Run("AC-1E-01 and AC-1E-02: RecordUsage idempotency", func(t *testing.T) {
		testRecordUsageIdempotency(ctx, t, s)
	})
	t.Run("RecordUsageBatch", func(t *testing.T) {
		testRecordUsageBatch(ctx, t, s)
	})
	t.Run("QueryUsage pagination and filters", func(t *testing.T) {
		testQueryUsage(ctx, t, s)
	})
	t.Run("QueryDailyAggregates variations", func(t *testing.T) {
		testQueryDailyAggregates(ctx, t, s)
	})
	t.Run("Audit Events", func(t *testing.T) {
		testAuditEvents(ctx, t, s)
	})
	t.Run("Processed Messages Idempotency", func(t *testing.T) {
		testProcessedMessages(ctx, t, s)
	})
	t.Run("Import Cursors", func(t *testing.T) {
		testImportCursors(ctx, t, s)
	})
	t.Run("Import Runs", func(t *testing.T) {
		testImportRuns(ctx, t, s)
	})
	t.Run("Subscriptions CRUD", func(t *testing.T) {
		testSubscriptionsCRUD(ctx, t, s)
	})
}

func TestStore_Contract(t *testing.T) {
	dbs := createTestDBs(t)
	for driver, db := range dbs {
		db := db
		driver := driver
		t.Run(driver, func(t *testing.T) {
			defer db.Close()
			runAllStoreContractTests(t, db)
		})
	}
}
