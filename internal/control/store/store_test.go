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

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"google.golang.org/protobuf/encoding/protojson"
)

func createTestDBs(t *testing.T) map[string]*sql.DB {
	t.Helper()
	dbs := make(map[string]*sql.DB)

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-control-%d.db", time.Now().UnixNano()))
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

	for name, db := range dbs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := store.Migrate(ctx, db); err != nil {
			cancel()
			t.Fatalf("%s: failed to run migrations: %v", name, err)
		}
		cancel()
	}

	t.Cleanup(func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	})

	return dbs
}

func forEachStore(t *testing.T, fn func(t *testing.T, s *store.Store, name string)) {
	t.Helper()
	for name, db := range createTestDBs(t) {
		t.Run(name, func(t *testing.T) {
			if name == "postgres" {
				if _, err := db.Exec("TRUNCATE keys, pricing, logical_models, control_outbox CASCADE; UPDATE config_versions SET version = 1 WHERE id = 1;"); err != nil {
					t.Fatalf("failed to truncate tables: %v", err)
				}
			}
			s := store.New(db)
			fn(t, s, name)
		})
	}
}

func TestMigrationIdempotency(t *testing.T) {
	dbs := createTestDBs(t)
	for name, db := range dbs {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if err := store.Migrate(ctx, db); err != nil {
				t.Fatalf("second migration run failed: %v", err)
			}
		})
	}
}

func verifyCreatedKey(ctx context.Context, t *testing.T, s *store.Store, key core.VirtualKey, initVer int64) {
	t.Helper()

	newVer, err := s.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("failed to query new version: %v", err)
	}
	if newVer <= initVer {
		t.Errorf("expected new version > %d, got %d", initVer, newVer)
	}

	byID, err := s.GetKeyByID(ctx, key.ID)
	if err != nil {
		t.Fatalf("failed to get key by id: %v", err)
	}
	if byID.Name != key.Name || byID.KeyHash != key.KeyHash {
		t.Errorf("key mismatch: %+v", byID)
	}
	if len(byID.AllowedModels) != 2 {
		t.Errorf("expected 2 allowed models, got %d", len(byID.AllowedModels))
	}

	byHash, err := s.GetKeyByHash(ctx, key.KeyHash)
	if err != nil {
		t.Fatalf("failed to get key by hash: %v", err)
	}
	if byHash.ID != key.ID {
		t.Errorf("expected key ID %q, got %q", key.ID, byHash.ID)
	}
}

func verifyOutboxKeyLimitEvent(ctx context.Context, t *testing.T, s *store.Store, keyID string) {
	t.Helper()

	outbox, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("failed to fetch outbox: %v", err)
	}
	if len(outbox) == 0 {
		t.Fatal("expected pending outbox message")
	}

	var event eventsv1.ConfigChanged
	if err := protojson.Unmarshal([]byte(outbox[0].Payload), &event); err != nil {
		t.Fatalf("failed to unmarshal outbox event: %v", err)
	}
	if event.Kind != eventsv1.ConfigChanged_KEY {
		t.Errorf("expected KEY event, got %v", event.Kind)
	}
	if len(event.KeyLimits) != 1 || event.KeyLimits[0].KeyId != keyID {
		t.Errorf("unexpected key limits in event: %+v", event.KeyLimits)
	}
}

func TestStore_CreateAndGetKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()
		initVer, err := s.GetCurrentVersion(ctx)
		if err != nil {
			t.Fatalf("unexpected version error: %v", err)
		}

		key := core.VirtualKey{
			ID:                 "k-101",
			Name:               "Test App",
			KeyHash:            "hash-101",
			AllowedModels:      []string{"claude-3-5-sonnet", "gpt-4o"},
			DailyLimitMicros:   money.Micros(100_000),
			MonthlyLimitMicros: money.Micros(1_000_000),
			RPM:                120,
		}

		created, err := s.CreateKey(ctx, key)
		if err != nil {
			t.Fatalf("failed to create key: %v", err)
		}
		if created.ID != key.ID {
			t.Errorf("expected ID %q, got %q", key.ID, created.ID)
		}

		verifyCreatedKey(ctx, t, s, key, initVer)
		verifyOutboxKeyLimitEvent(ctx, t, s, key.ID)
	})
}

func TestStore_RevokeKey(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()
		key := core.VirtualKey{
			ID:      "k-to-revoke",
			Name:    "Temporary Key",
			KeyHash: "hash-to-revoke",
		}
		if _, err := s.CreateKey(ctx, key); err != nil {
			t.Fatalf("failed to create key: %v", err)
		}

		initVer, err := s.GetCurrentVersion(ctx)
		if err != nil {
			t.Fatalf("unexpected version error: %v", err)
		}

		revokedAt := time.Date(2026, 10, 18, 22, 30, 0, 0, time.UTC)
		if err := s.RevokeKey(ctx, "k-to-revoke", revokedAt); err != nil {
			t.Fatalf("failed to revoke key: %v", err)
		}

		verifyRevokedKeyOutboxAndVersion(ctx, t, s, "k-to-revoke", initVer)
		verifyRevocationStatuses(ctx, t, s, "k-to-revoke", revokedAt)
	})
}

func verifyRevocationStatuses(ctx context.Context, t *testing.T, s *store.Store, keyID string, revokedAt time.Time) {
	t.Helper()

	k, err := s.GetKeyByID(ctx, keyID)
	if err != nil {
		t.Fatalf("failed to get revoked key: %v", err)
	}
	if k.RevokedAt == nil {
		t.Fatal("expected revoked_at to be set")
	}
	if k.IsActive(time.Now()) {
		t.Error("expected key to be inactive")
	}

	if err := s.RevokeKey(ctx, keyID, revokedAt); !errors.Is(err, core.ErrKeyRevoked) {
		t.Errorf("expected ErrKeyRevoked on second call, got %v", err)
	}

	if err := s.RevokeKey(ctx, "non-existent", revokedAt); !errors.Is(err, core.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func verifyRevokedKeyOutboxAndVersion(ctx context.Context, t *testing.T, s *store.Store, keyID string, initVer int64) {
	t.Helper()

	newVer, err := s.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("failed to query version after revoke: %v", err)
	}
	if newVer <= initVer {
		t.Errorf("expected version after revoke > %d, got %d", initVer, newVer)
	}

	msgs, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("failed to fetch outbox: %v", err)
	}
	var found bool
	for _, m := range msgs {
		var event eventsv1.ConfigChanged
		if err := protojson.Unmarshal([]byte(m.Payload), &event); err != nil {
			continue
		}
		if event.Kind == eventsv1.ConfigChanged_KEY_REVOKED && len(event.KeyLimits) > 0 && event.KeyLimits[0].KeyId == keyID && event.KeyLimits[0].Revoked {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected outbox to contain KEY_REVOKED event with Revoked: true")
	}
}

func verifyPricingOutboxAndVersion(ctx context.Context, t *testing.T, s *store.Store, initVer int64) {
	t.Helper()

	newVer, err := s.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("failed to query version after upsert pricing: %v", err)
	}
	if newVer <= initVer {
		t.Errorf("expected version after upsert pricing > %d, got %d", initVer, newVer)
	}

	msgs, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("failed to fetch outbox: %v", err)
	}
	var found bool
	for _, m := range msgs {
		var event eventsv1.ConfigChanged
		if err := protojson.Unmarshal([]byte(m.Payload), &event); err != nil {
			continue
		}
		if event.Kind == eventsv1.ConfigChanged_PRICING {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected outbox to contain PRICING event")
	}
}

func TestStore_PricingUpsertAndList(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()
		items := []core.Pricing{
			{
				Model:             "claude-3-5-sonnet",
				Provider:          "anthropic",
				InputPerMTok:      money.Micros(3_000_000),
				OutputPerMTok:     money.Micros(15_000_000),
				CacheReadPerMTok:  money.Micros(300_000),
				CacheWritePerMTok: money.Micros(3_750_000),
			},
			{
				Model:             "gpt-4o",
				Provider:          "openai",
				InputPerMTok:      money.Micros(2_500_000),
				OutputPerMTok:     money.Micros(10_000_000),
				CacheReadPerMTok:  money.Micros(1_250_000),
				CacheWritePerMTok: money.Micros(2_500_000),
			},
		}

		initVer, err := s.GetCurrentVersion(ctx)
		if err != nil {
			t.Fatalf("unexpected version error: %v", err)
		}

		if err := s.UpsertPricing(ctx, items); err != nil {
			t.Fatalf("failed to upsert pricing: %v", err)
		}

		verifyPricingOutboxAndVersion(ctx, t, s, initVer)

		list, err := s.ListPricing(ctx)
		if err != nil {
			t.Fatalf("failed to list pricing: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 pricing items, got %d", len(list))
		}

		items[0].InputPerMTok = money.Micros(3_500_000)
		if err := s.UpsertPricing(ctx, items[:1]); err != nil {
			t.Fatalf("failed to update pricing: %v", err)
		}

		updatedList, err := s.ListPricing(ctx)
		if err != nil {
			t.Fatalf("failed to list updated pricing: %v", err)
		}
		if updatedList[0].InputPerMTok != money.Micros(3_500_000) {
			t.Errorf("expected updated input price 3_500_000, got %d", updatedList[0].InputPerMTok)
		}
	})
}

func TestStore_LogicalModelsUpsertAndList(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()
		models := []core.LogicalModel{
			{
				Name:  "claude-fast",
				Chain: []string{"claude-3-5-haiku-20241022"},
			},
			{
				Name:  "claude-smart",
				Chain: []string{"claude-3-5-sonnet-20241022", "anthropic/claude-3-5-sonnet"},
			},
		}

		if err := s.UpsertLogicalModels(ctx, models); err != nil {
			t.Fatalf("failed to upsert logical models: %v", err)
		}

		list, err := s.ListLogicalModels(ctx)
		if err != nil {
			t.Fatalf("failed to list logical models: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 logical models, got %d", len(list))
		}
		if list[1].Name != "claude-smart" || len(list[1].Chain) != 2 {
			t.Errorf("unexpected model data: %+v", list[1])
		}
	})
}

func seedSnapshotEntities(ctx context.Context, t *testing.T, s *store.Store) {
	t.Helper()

	key := core.VirtualKey{
		ID:      "k-snap",
		Name:    "Snapshot Key",
		KeyHash: "hash-snap",
	}
	if _, err := s.CreateKey(ctx, key); err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	pricing := []core.Pricing{
		{
			Model:        "model-snap",
			Provider:     "prov-snap",
			InputPerMTok: money.Micros(1_000_000),
		},
	}
	if err := s.UpsertPricing(ctx, pricing); err != nil {
		t.Fatalf("failed to upsert pricing: %v", err)
	}

	models := []core.LogicalModel{
		{
			Name:  "lm-snap",
			Chain: []string{"model-snap"},
		},
	}
	if err := s.UpsertLogicalModels(ctx, models); err != nil {
		t.Fatalf("failed to upsert logical models: %v", err)
	}
}

func verifyOutboxPublishCycle(ctx context.Context, t *testing.T, s *store.Store) {
	t.Helper()

	msgs, err := s.FetchPendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("failed to fetch outbox: %v", err)
	}
	if len(msgs) < 3 {
		t.Errorf("expected at least 3 pending messages, got %d", len(msgs))
	}

	msgIDs := []string{msgs[0].ID, msgs[1].ID}
	if err := s.MarkOutboxPublished(ctx, msgIDs, time.Now().UTC()); err != nil {
		t.Fatalf("failed to mark outbox published: %v", err)
	}

	remaining, err := s.FetchPendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("failed to fetch remaining outbox: %v", err)
	}
	if len(remaining) != len(msgs)-2 {
		t.Errorf("expected %d remaining outbox messages, got %d", len(msgs)-2, len(remaining))
	}
}

func TestStore_GetSnapshotAndOutboxMark(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()
		seedSnapshotEntities(ctx, t, s)

		snapshot, err := s.GetSnapshot(ctx)
		if err != nil {
			t.Fatalf("failed to get snapshot: %v", err)
		}
		if snapshot.Version < 3 {
			t.Errorf("expected version >= 3, got %d", snapshot.Version)
		}
		if len(snapshot.Keys) == 0 || len(snapshot.Pricing) == 0 || len(snapshot.LogicalModels) == 0 {
			t.Errorf("incomplete snapshot: %+v", snapshot)
		}

		verifyOutboxPublishCycle(ctx, t, s)
	})
}

func TestStore_EdgeCases_NotFoundAndDefaults(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()

		if s.DB() == nil {
			t.Error("expected DB() to be non-nil")
		}

		if err := s.MarkOutboxPublished(ctx, nil, time.Now()); err != nil {
			t.Errorf("expected nil error on empty mark published, got %v", err)
		}

		if _, err := s.FetchPendingOutbox(ctx, 0); err != nil {
			t.Errorf("failed to fetch outbox with limit 0: %v", err)
		}

		if _, err := s.GetKeyByID(ctx, "does-not-exist"); !errors.Is(err, core.ErrKeyNotFound) {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
		if _, err := s.GetKeyByHash(ctx, "non-existent-hash"); !errors.Is(err, core.ErrKeyNotFound) {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})
}

func TestStore_EdgeCases_ValidationAndAutoID(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, _ string) {
		ctx := context.Background()

		if _, err := s.CreateKey(ctx, core.VirtualKey{Name: ""}); err == nil {
			t.Error("expected error creating invalid key")
		}

		k, err := s.CreateKey(ctx, core.VirtualKey{
			Name:    "Auto ID Key",
			KeyHash: "auto-hash",
		})
		if err != nil {
			t.Fatalf("failed to create key with auto ID: %v", err)
		}
		if k.ID == "" {
			t.Error("expected auto-generated ID")
		}

		keys, err := s.ListKeys(ctx)
		if err != nil {
			t.Fatalf("failed to list keys: %v", err)
		}
		if len(keys) == 0 {
			t.Error("expected non-empty key list")
		}

		if err := s.UpsertPricing(ctx, []core.Pricing{{Model: ""}}); err == nil {
			t.Error("expected error upserting invalid pricing")
		}

		if err := s.UpsertLogicalModels(ctx, []core.LogicalModel{{Name: ""}}); err == nil {
			t.Error("expected error upserting invalid logical model")
		}
	})
}
