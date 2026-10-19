package grpcapi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	controlv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/grpcapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockSnapshotStore struct {
	snapshot *core.Snapshot
	err      error
}

func (m *mockSnapshotStore) GetSnapshot(_ context.Context) (*core.Snapshot, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.snapshot, nil
}

func TestGetSnapshot_Success(t *testing.T) {
	future := time.Now().Add(24 * time.Hour)
	snap := &core.Snapshot{
		Version: 42,
		Keys: []core.VirtualKey{
			{
				ID:                 "k-1",
				Name:               "Test Key",
				KeyHash:            "peppered-hash-1",
				AllowedModels:      []string{"claude-3-5-sonnet"},
				DailyLimitMicros:   money.Micros(100_000),
				MonthlyLimitMicros: money.Micros(1_000_000),
				RPM:                60,
				CreatedAt:          time.Now(),
				ExpiresAt:          &future,
			},
			{
				ID:        "k-revoked",
				Name:      "Revoked Key",
				KeyHash:   "peppered-hash-revoked",
				CreatedAt: time.Now().Add(-2 * time.Hour),
				RevokedAt: &future,
			},
			{
				ID:        "k-expired",
				Name:      "Expired Key",
				KeyHash:   "peppered-hash-expired",
				CreatedAt: time.Now().Add(-2 * time.Hour),
				ExpiresAt: func() *time.Time { t := time.Now().Add(-1 * time.Hour); return &t }(),
			},
		},
		Pricing: []core.Pricing{
			{
				Model:             "claude-3-5-sonnet",
				Provider:          "anthropic",
				InputPerMTok:      money.Micros(3_000_000),
				OutputPerMTok:     money.Micros(15_000_000),
				CacheReadPerMTok:  money.Micros(300_000),
				CacheWritePerMTok: money.Micros(3_750_000),
			},
		},
		LogicalModels: []core.LogicalModel{
			{
				Name:  "claude-fast",
				Chain: []string{"claude-3-5-haiku"},
			},
		},
	}

	server := grpcapi.New(&mockSnapshotStore{snapshot: snap})
	res, err := server.GetSnapshot(context.Background(), &controlv1.GetSnapshotRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Version != 42 {
		t.Errorf("expected version 42, got %d", res.Version)
	}
	if len(res.Keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(res.Keys))
	}
	k := res.Keys[0]
	if k.Id != "k-1" || k.KeyHash != "peppered-hash-1" || !k.Active {
		t.Errorf("unexpected key fields: %+v", k)
	}
	if k.DailyLimitMicros != 100_000 || k.MonthlyLimitMicros != 1_000_000 || k.Rpm != 60 {
		t.Errorf("unexpected key limits: %+v", k)
	}
	if k.ExpiresAt == nil {
		t.Error("expected expires_at timestamp to be populated")
	}

	if len(res.Pricing) != 1 {
		t.Fatalf("expected 1 pricing item, got %d", len(res.Pricing))
	}
	p := res.Pricing[0]
	if p.Model != "claude-3-5-sonnet" || p.InputPerMtokMicros != 3_000_000 {
		t.Errorf("unexpected pricing fields: %+v", p)
	}

	if len(res.LogicalModels) != 1 {
		t.Fatalf("expected 1 logical model, got %d", len(res.LogicalModels))
	}
	m := res.LogicalModels[0]
	if m.Name != "claude-fast" || len(m.Chain) != 1 {
		t.Errorf("unexpected logical model fields: %+v", m)
	}
}

func TestGetSnapshot_StoreError(t *testing.T) {
	server := grpcapi.New(&mockSnapshotStore{err: errors.New("db unavailable")})
	_, err := server.GetSnapshot(context.Background(), &controlv1.GetSnapshotRequest{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Errorf("expected codes.Internal, got %v", err)
	}
}
