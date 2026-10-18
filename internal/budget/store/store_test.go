package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
)

func createTestDBs(t *testing.T) map[string]*sql.DB {
	t.Helper()
	dbs := make(map[string]*sql.DB)

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-budget-%d.db", time.Now().UnixNano()))
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
		name := name
		db := db
		t.Run(name, func(t *testing.T) {
			s := store.New(db)
			fn(t, s, name)
		})
	}
}

func TestAC1B01_Reserve_WithinLimits(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, name string) {
		ctx := context.Background()
		keyID := fmt.Sprintf("k-ac01-%s", name)
		limits := core.KeyLimits{
			KeyID:              keyID,
			DailyLimitMicros:   5000,
			MonthlyLimitMicros: 50000,
			RPM:                100,
		}
		if err := s.SetKeyLimits(ctx, limits); err != nil {
			t.Fatalf("failed to set limits: %v", err)
		}

		now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
		res, err := s.Reserve(ctx, store.ReserveParams{
			KeyID:     keyID,
			MaxMicros: 1000,
			RequestID: "req-1",
			Now:       now,
		})
		if err != nil {
			t.Fatalf("unexpected reserve error: %v", err)
		}

		if !res.Allowed || res.ReservationID == "" || res.RemainingDailyMicros != 4000 {
			t.Errorf("unexpected reserve result: %+v", res)
		}

		rem, err := s.Remaining(ctx, keyID, now)
		if err != nil {
			t.Fatalf("failed to get remaining: %v", err)
		}
		if rem.DailyReservedMicros != 1000 || rem.DailySpentMicros != 0 {
			t.Errorf("unexpected remaining balances: %+v", rem)
		}
	})
}

func TestAC1B02_Reserve_ExceedsBudget(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, name string) {
		ctx := context.Background()
		keyID := fmt.Sprintf("k-ac02-%s", name)
		limits := core.KeyLimits{
			KeyID:              keyID,
			DailyLimitMicros:   1000,
			MonthlyLimitMicros: 10000,
		}
		if err := s.SetKeyLimits(ctx, limits); err != nil {
			t.Fatalf("failed to set limits: %v", err)
		}

		now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
		_, err := s.Reserve(ctx, store.ReserveParams{
			KeyID:     keyID,
			MaxMicros: 1500,
			RequestID: "req-over",
			Now:       now,
		})
		if !errors.Is(err, core.ErrBudgetExceeded) {
			t.Fatalf("expected ErrBudgetExceeded, got: %v", err)
		}

		rem, err := s.Remaining(ctx, keyID, now)
		if err != nil {
			t.Fatalf("failed to query remaining: %v", err)
		}
		if rem.DailyReservedMicros != 0 || rem.DailySpentMicros != 0 {
			t.Errorf("expected 0 reserved and 0 spent, got reserved=%d spent=%d", rem.DailyReservedMicros, rem.DailySpentMicros)
		}
	})
}

func runConcurrentWorkers(ctx context.Context, s *store.Store, keyID string, now time.Time, count int) (allowed, rejected int64) {
	var wg sync.WaitGroup
	var allowedCount, rejectedCount atomic.Int64

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := s.Reserve(ctx, store.ReserveParams{
				KeyID:     keyID,
				MaxMicros: 1000,
				RequestID: fmt.Sprintf("conc-req-%d", idx),
				Now:       now,
			})
			if err == nil {
				allowedCount.Add(1)
			} else if errors.Is(err, core.ErrBudgetExceeded) {
				rejectedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()
	return allowedCount.Load(), rejectedCount.Load()
}

func TestAC1B04_Concurrency_50Goroutines_10Allowed(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, name string) {
		ctx := context.Background()
		keyID := fmt.Sprintf("k-conc-%s", name)
		limits := core.KeyLimits{
			KeyID:              keyID,
			DailyLimitMicros:   10000,
			MonthlyLimitMicros: 100000,
			RPM:                1000,
		}
		if err := s.SetKeyLimits(ctx, limits); err != nil {
			t.Fatalf("failed to set limits: %v", err)
		}

		now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
		allowed, rejected := runConcurrentWorkers(ctx, s, keyID, now, 50)

		if allowed != 10 {
			t.Errorf("expected exactly 10 allowed reservations, got %d", allowed)
		}
		if rejected != 40 {
			t.Errorf("expected exactly 40 rejected reservations, got %d", rejected)
		}

		rem, err := s.Remaining(ctx, keyID, now)
		if err != nil {
			t.Fatalf("failed to get remaining: %v", err)
		}
		if rem.DailyReservedMicros != 10000 {
			t.Errorf("expected exactly 10000 daily reserved micros, got %d", rem.DailyReservedMicros)
		}
	})
}

func runAC1B05Test(t *testing.T, s *store.Store, name string) {
	ctx := context.Background()
	keyID := fmt.Sprintf("k-ac05-%s", name)
	limits := core.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   10000,
		MonthlyLimitMicros: 50000,
	}
	if err := s.SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
	res, err := s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 2000,
		RequestID: "req-s1",
		Now:       now,
	})
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	settleRes, err := s.Settle(ctx, res.ReservationID, 1500, now)
	if err != nil || !settleRes.Settled {
		t.Fatalf("settle failed: %v", err)
	}

	rem, _ := s.Remaining(ctx, keyID, now)
	if rem.DailySpentMicros != 1500 || rem.DailyReservedMicros != 0 {
		t.Errorf("unexpected remaining balance after settle: %+v", rem)
	}

	settleRes2, err := s.Settle(ctx, res.ReservationID, 1500, now)
	if err != nil || !settleRes2.Settled {
		t.Fatalf("second settle failed: %v", err)
	}

	rem2, _ := s.Remaining(ctx, keyID, now)
	if rem2.DailySpentMicros != 1500 || rem2.DailyReservedMicros != 0 {
		t.Errorf("expected idempotent balance, got: %+v", rem2)
	}
}

func TestAC1B05_Settle_Idempotent(t *testing.T) {
	forEachStore(t, runAC1B05Test)
}

func runAC1B06Test(t *testing.T, s *store.Store, name string) {
	ctx := context.Background()
	keyID := fmt.Sprintf("k-ac06-%s", name)
	limits := core.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   10000,
		MonthlyLimitMicros: 100000,
	}
	if err := s.SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
	res1, _ := s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 8500,
		RequestID: "req-t1",
		Now:       now,
	})
	if _, err := s.Settle(ctx, res1.ReservationID, 8500, now); err != nil {
		t.Fatalf("settle 1 failed: %v", err)
	}

	pending, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 outbox message for 80%% threshold, got %d (err: %v)", len(pending), err)
	}

	res2, _ := s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 1500,
		RequestID: "req-t2",
		Now:       now,
	})
	if _, err := s.Settle(ctx, res2.ReservationID, 1500, now); err != nil {
		t.Fatalf("settle 2 failed: %v", err)
	}

	pending2, err := s.FetchPendingOutbox(ctx, 10)
	if err != nil || len(pending2) != 2 {
		t.Fatalf("expected 2 outbox messages (80%% and 100%%), got %d (err: %v)", len(pending2), err)
	}

	if err := s.MarkOutboxPublished(ctx, []string{pending2[0].ID, pending2[1].ID}, time.Now().UTC()); err != nil {
		t.Fatalf("mark published failed: %v", err)
	}
	pendingAfter, _ := s.FetchPendingOutbox(ctx, 10)
	if len(pendingAfter) != 0 {
		t.Errorf("expected 0 pending outbox messages after publish, got %d", len(pendingAfter))
	}
}

func TestAC1B06_Settle_OutboxThreshold80and100(t *testing.T) {
	forEachStore(t, runAC1B06Test)
}

func verifyExpirationAndClosedState(ctx context.Context, t *testing.T, s *store.Store, resID string, now time.Time) {
	t.Helper()
	expiredCount, err := s.ExpireReservations(ctx, now.Add(16*time.Minute))
	if err != nil || expiredCount != 1 {
		t.Fatalf("expected 1 expired reservation, got %d (err: %v)", expiredCount, err)
	}

	_, err = s.Settle(ctx, resID, 1000, now.Add(17*time.Minute))
	if !errors.Is(err, core.ErrReservationClosed) {
		t.Errorf("expected ErrReservationClosed, got: %v", err)
	}
}

func TestAC1B08_Release_And_SweeperExpiration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *store.Store, name string) {
		ctx := context.Background()
		keyID := fmt.Sprintf("k-ac08-%s", name)
		limits := core.KeyLimits{
			KeyID:              keyID,
			DailyLimitMicros:   5000,
			MonthlyLimitMicros: 50000,
		}
		if err := s.SetKeyLimits(ctx, limits); err != nil {
			t.Fatalf("failed to set limits: %v", err)
		}

		now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
		resRel, _ := s.Reserve(ctx, store.ReserveParams{
			KeyID:     keyID,
			MaxMicros: 1000,
			RequestID: "req-rel",
			Now:       now,
		})
		if err := s.Release(ctx, resRel.ReservationID, now); err != nil {
			t.Fatalf("release failed: %v", err)
		}
		if err := s.Release(ctx, resRel.ReservationID, now); err != nil {
			t.Fatalf("second release failed: %v", err)
		}

		resExp, _ := s.Reserve(ctx, store.ReserveParams{
			KeyID:     keyID,
			MaxMicros: 2000,
			RequestID: "req-exp",
			Now:       now,
			TTL:       15 * time.Minute,
		})

		verifyExpirationAndClosedState(ctx, t, s, resExp.ReservationID, now)
	})
}

func runRateLimitTest(t *testing.T, s *store.Store, name string) {
	ctx := context.Background()
	keyID := fmt.Sprintf("k-rl-%s", name)
	limits := core.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   50000,
		MonthlyLimitMicros: 500000,
		RPM:                2,
	}
	if err := s.SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		_, err := s.Reserve(ctx, store.ReserveParams{
			KeyID:     keyID,
			MaxMicros: 100,
			RequestID: fmt.Sprintf("rl-ok-%d", i),
			Now:       now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("call %d within RPM should succeed, got %v", i+1, err)
		}
	}

	// 3rd call within 60s exceeds RPM
	_, err := s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 100,
		RequestID: "rl-over",
		Now:       now.Add(5 * time.Second),
	})
	if !errors.Is(err, core.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got: %v", err)
	}
}

func TestAC1B03_Reserve_RateLimit(t *testing.T) {
	forEachStore(t, runRateLimitTest)
}

func runNotFoundAndClosedTest(t *testing.T, s *store.Store, name string) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)

	// Unknown key
	if _, err := s.Remaining(ctx, "non-existent", now); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound for Remaining with unknown key, got %v", err)
	}
	if _, err := s.Reserve(ctx, store.ReserveParams{KeyID: "non-existent", MaxMicros: 100, Now: now}); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound for Reserve with unknown key, got %v", err)
	}

	// Unknown reservation
	if _, err := s.Settle(ctx, "unknown-res", 100, now); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound for Settle with unknown res, got %v", err)
	}
	if err := s.Release(ctx, "unknown-res", now); !errors.Is(err, core.ErrNotFound) {
		t.Errorf("expected ErrNotFound for Release with unknown res, got %v", err)
	}

	// Release after Settle -> closed
	keyID := fmt.Sprintf("k-closed-%s", name)
	_ = s.SetKeyLimits(ctx, core.KeyLimits{KeyID: keyID, DailyLimitMicros: 5000})
	res, _ := s.Reserve(ctx, store.ReserveParams{KeyID: keyID, MaxMicros: 500, Now: now})
	_, _ = s.Settle(ctx, res.ReservationID, 500, now)

	if err := s.Release(ctx, res.ReservationID, now); !errors.Is(err, core.ErrReservationClosed) {
		t.Errorf("expected ErrReservationClosed for Release on settled res, got %v", err)
	}

	// Outbox edge cases
	if err := s.MarkOutboxPublished(ctx, nil, now); err != nil {
		t.Errorf("expected nil error on empty mark published, got %v", err)
	}
	if _, err := s.FetchPendingOutbox(ctx, 0); err != nil {
		t.Errorf("expected nil error on fetch with default limit, got %v", err)
	}
}

func TestStore_ErrorAndEdgeCases(t *testing.T) {
	forEachStore(t, runNotFoundAndClosedTest)
}
