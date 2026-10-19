package grpcapi

import (
	"context"
	"time"

	controlv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotStore defines the storage contract required to produce system snapshots.
type SnapshotStore interface {
	GetSnapshot(ctx context.Context) (*core.Snapshot, error)
}

// Server implements controlv1.ControlServiceServer.
type Server struct {
	controlv1.UnimplementedControlServiceServer
	store SnapshotStore
}

// New creates a new Control gRPC Server instance.
func New(store SnapshotStore) *Server {
	return &Server{store: store}
}

// GetSnapshot retrieves the entire current configuration snapshot for gateway initialization.
func (s *Server) GetSnapshot(ctx context.Context, _ *controlv1.GetSnapshotRequest) (*controlv1.Snapshot, error) {
	snap, err := s.store.GetSnapshot(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to load snapshot: %v", err)
	}

	now := time.Now().UTC()
	keys := make([]*controlv1.KeyInfo, 0, len(snap.Keys))
	for _, k := range snap.Keys {
		if !k.IsActive(now) {
			continue
		}
		var exp *timestamppb.Timestamp
		if k.ExpiresAt != nil {
			exp = timestamppb.New(*k.ExpiresAt)
		}
		keys = append(keys, &controlv1.KeyInfo{
			Id:                 k.ID,
			Name:               k.Name,
			KeyHash:            k.KeyHash,
			AllowedModels:      k.AllowedModels,
			Active:             true,
			ExpiresAt:          exp,
			DailyLimitMicros:   int64(k.DailyLimitMicros),
			MonthlyLimitMicros: int64(k.MonthlyLimitMicros),
			Rpm:                k.RPM,
		})
	}

	pricing := make([]*controlv1.PricingInfo, 0, len(snap.Pricing))
	for _, p := range snap.Pricing {
		pricing = append(pricing, &controlv1.PricingInfo{
			Model:                   p.Model,
			Provider:                p.Provider,
			InputPerMtokMicros:      int64(p.InputPerMTok),
			OutputPerMtokMicros:     int64(p.OutputPerMTok),
			CacheReadPerMtokMicros:  int64(p.CacheReadPerMTok),
			CacheWritePerMtokMicros: int64(p.CacheWritePerMTok),
		})
	}

	models := make([]*controlv1.LogicalModelInfo, 0, len(snap.LogicalModels))
	for _, m := range snap.LogicalModels {
		models = append(models, &controlv1.LogicalModelInfo{
			Name:  m.Name,
			Chain: m.Chain,
		})
	}

	return &controlv1.Snapshot{
		Version:       snap.Version,
		Keys:          keys,
		Pricing:       pricing,
		LogicalModels: models,
	}, nil
}
