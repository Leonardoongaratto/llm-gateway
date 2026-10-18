package grpcapi

import (
	"context"
	"errors"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServiceStore interface {
	Reserve(ctx context.Context, params store.ReserveParams) (*store.ReserveResult, error)
	Settle(ctx context.Context, reservationID string, actualMicros int64, now time.Time) (*store.SettleResult, error)
	Release(ctx context.Context, reservationID string, now time.Time) error
	Remaining(ctx context.Context, keyID string, now time.Time) (*store.RemainingInfo, error)
}

type Server struct {
	budgetv1.UnimplementedBudgetServiceServer
	store ServiceStore
}

func New(s ServiceStore) *Server {
	return &Server{store: s}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, core.ErrBudgetExceeded) {
		st := status.New(codes.FailedPrecondition, "budget exceeded")
		if d, e := st.WithDetails(&budgetv1.ReserveDenial{Reason: budgetv1.ReserveDenial_BUDGET_EXCEEDED}); e == nil {
			return d.Err()
		}
		return st.Err()
	}

	if errors.Is(err, core.ErrRateLimited) {
		st := status.New(codes.ResourceExhausted, "rate limited")
		if d, e := st.WithDetails(&budgetv1.ReserveDenial{Reason: budgetv1.ReserveDenial_RATE_LIMITED, RetryAfterSeconds: 60}); e == nil {
			return d.Err()
		}
		return st.Err()
	}

	if errors.Is(err, core.ErrNotFound) {
		st := status.New(codes.NotFound, "not found")
		if d, e := st.WithDetails(&budgetv1.ReserveDenial{Reason: budgetv1.ReserveDenial_UNKNOWN_KEY}); e == nil {
			return d.Err()
		}
		return st.Err()
	}

	if errors.Is(err, core.ErrReservationClosed) {
		return status.Error(codes.FailedPrecondition, "reservation already settled, released or expired")
	}

	if errors.Is(err, core.ErrNegativeAmount) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return status.Errorf(codes.Internal, "internal budget error: %v", err)
}

func (s *Server) Reserve(ctx context.Context, req *budgetv1.ReserveRequest) (*budgetv1.ReserveResponse, error) {
	if req.KeyId == "" {
		return nil, status.Error(codes.InvalidArgument, "key_id is required")
	}
	if req.MaxMicros <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max_micros must be greater than zero")
	}

	res, err := s.store.Reserve(ctx, store.ReserveParams{
		KeyID:     req.KeyId,
		MaxMicros: req.MaxMicros,
		RequestID: req.RequestId,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		return nil, mapError(err)
	}

	return &budgetv1.ReserveResponse{
		ReservationId:   res.ReservationID,
		RemainingMicros: res.RemainingDailyMicros,
	}, nil
}

func (s *Server) Settle(ctx context.Context, req *budgetv1.SettleRequest) (*budgetv1.SettleResponse, error) {
	if req.ReservationId == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	if req.ActualMicros < 0 {
		return nil, status.Error(codes.InvalidArgument, "actual_micros cannot be negative")
	}

	_, err := s.store.Settle(ctx, req.ReservationId, req.ActualMicros, time.Now().UTC())
	if err != nil {
		return nil, mapError(err)
	}

	return &budgetv1.SettleResponse{}, nil
}

func (s *Server) Release(ctx context.Context, req *budgetv1.ReleaseRequest) (*budgetv1.ReleaseResponse, error) {
	if req.ReservationId == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}

	err := s.store.Release(ctx, req.ReservationId, time.Now().UTC())
	if err != nil {
		return nil, mapError(err)
	}

	return &budgetv1.ReleaseResponse{}, nil
}

func (s *Server) Remaining(ctx context.Context, req *budgetv1.RemainingRequest) (*budgetv1.RemainingResponse, error) {
	if req.KeyId == "" {
		return nil, status.Error(codes.InvalidArgument, "key_id is required")
	}

	info, err := s.store.Remaining(ctx, req.KeyId, time.Now().UTC())
	if err != nil {
		return nil, mapError(err)
	}

	remDaily := info.DailyLimitMicros - (info.DailySpentMicros + info.DailyReservedMicros)
	if remDaily < 0 {
		remDaily = 0
	}
	remMonthly := info.MonthlyLimitMicros - (info.MonthlySpentMicros + info.MonthlyReservedMicros)
	if remMonthly < 0 {
		remMonthly = 0
	}

	return &budgetv1.RemainingResponse{
		DailyMicros:   remDaily,
		MonthlyMicros: remMonthly,
	}, nil
}
