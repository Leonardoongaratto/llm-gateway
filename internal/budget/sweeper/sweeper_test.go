package sweeper_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/sweeper"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
)

type mockExpirer struct {
	expireFunc func(ctx context.Context, now time.Time) (int, error)
	calls      atomic.Int32
}

func (m *mockExpirer) ExpireReservations(ctx context.Context, now time.Time) (int, error) {
	m.calls.Add(1)
	if m.expireFunc != nil {
		return m.expireFunc(ctx, now)
	}
	return 0, nil
}

func TestSweeper_SweepOnce(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mock := &mockExpirer{
			expireFunc: func(_ context.Context, _ time.Time) (int, error) {
				return 3, nil
			},
		}
		sw := sweeper.New(mock)
		count, err := sw.SweepOnce(context.Background(), time.Now())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3 expired reservations, got %d", count)
		}
	})

	t.Run("converts non-UTC timezone to UTC", func(t *testing.T) {
		var receivedLoc *time.Location
		mock := &mockExpirer{
			expireFunc: func(_ context.Context, now time.Time) (int, error) {
				receivedLoc = now.Location()
				return 1, nil
			},
		}
		sw := sweeper.New(mock)
		nonUTC := time.Now().In(time.FixedZone("UTC-3", -3*3600))
		_, err := sw.SweepOnce(context.Background(), nonUTC)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedLoc != time.UTC {
			t.Errorf("expected time passed to expirer to be in UTC, got: %v", receivedLoc)
		}
	})

	t.Run("error", func(t *testing.T) {
		mock := &mockExpirer{
			expireFunc: func(_ context.Context, _ time.Time) (int, error) {
				return 0, errors.New("db failure")
			},
		}
		sw := sweeper.New(mock)
		_, err := sw.SweepOnce(context.Background(), time.Now())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestSweeper_Run_GracefulShutdown(t *testing.T) {
	mock := &mockExpirer{
		expireFunc: func(_ context.Context, _ time.Time) (int, error) {
			return 1, nil
		},
	}
	sw := sweeper.New(mock, sweeper.WithInterval(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- sw.Run(ctx)
	}()

	time.Sleep(35 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on cancel, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("sweeper Run failed to stop within timeout")
	}

	if mock.calls.Load() == 0 {
		t.Error("expected at least 1 sweep call during run")
	}
}

func TestAC1B08_SweeperWithStore_EndToEnd(t *testing.T) {
	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-sweep-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}

	s := store.New(db)
	keyID := "k-sweep-e2e"
	limits := core.KeyLimits{
		KeyID:            keyID,
		DailyLimitMicros: 10000,
	}
	if err := s.SetKeyLimits(ctx, limits); err != nil {
		t.Fatalf("failed to set limits: %v", err)
	}

	now := time.Now().UTC()
	_, err = s.Reserve(ctx, store.ReserveParams{
		KeyID:     keyID,
		MaxMicros: 2500,
		RequestID: "req-sweep-1",
		Now:       now,
		TTL:       50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to reserve: %v", err)
	}

	remBefore, _ := s.Remaining(ctx, keyID, now)
	if remBefore.DailyReservedMicros != 2500 {
		t.Fatalf("expected 2500 reserved before expiry, got %d", remBefore.DailyReservedMicros)
	}

	sw := sweeper.New(s)
	count, err := sw.SweepOnce(ctx, now.Add(60*time.Millisecond))
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 expired reservation, got %d", count)
	}

	remAfter, _ := s.Remaining(ctx, keyID, now.Add(60*time.Millisecond))
	if remAfter.DailyReservedMicros != 0 {
		t.Errorf("expected 0 reserved after sweep, got %d", remAfter.DailyReservedMicros)
	}
}
