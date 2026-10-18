package grpcapi_test

import (
	"context"
	"net"
	"testing"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/grpcapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockStore struct {
	reserveFunc   func(ctx context.Context, params store.ReserveParams) (*store.ReserveResult, error)
	settleFunc    func(ctx context.Context, reservationID string, actualMicros int64, now time.Time) (*store.SettleResult, error)
	releaseFunc   func(ctx context.Context, reservationID string, now time.Time) error
	remainingFunc func(ctx context.Context, keyID string, now time.Time) (*store.RemainingInfo, error)
}

func (m *mockStore) Reserve(ctx context.Context, params store.ReserveParams) (*store.ReserveResult, error) {
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, params)
	}
	return nil, nil
}

func (m *mockStore) Settle(ctx context.Context, reservationID string, actualMicros int64, now time.Time) (*store.SettleResult, error) {
	if m.settleFunc != nil {
		return m.settleFunc(ctx, reservationID, actualMicros, now)
	}
	return &store.SettleResult{Settled: true}, nil
}

func (m *mockStore) Release(ctx context.Context, reservationID string, now time.Time) error {
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, reservationID, now)
	}
	return nil
}

func (m *mockStore) Remaining(ctx context.Context, keyID string, now time.Time) (*store.RemainingInfo, error) {
	if m.remainingFunc != nil {
		return m.remainingFunc(ctx, keyID, now)
	}
	return &store.RemainingInfo{KeyID: keyID}, nil
}

func setupTestServer(t *testing.T, s *mockStore) budgetv1.BudgetServiceClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	srv := grpcx.NewServer()
	budgetv1.RegisterBudgetServiceServer(srv, grpcapi.New(s))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })

	conn, err := grpcx.Dial(context.Background(), lis.Addr().String(), grpcx.WithDefaultDeadline(500*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return budgetv1.NewBudgetServiceClient(conn)
}

func TestGRPC_Reserve_Success(t *testing.T) {
	mock := &mockStore{
		reserveFunc: func(_ context.Context, _ store.ReserveParams) (*store.ReserveResult, error) {
			return &store.ReserveResult{
				ReservationID:        "res-123",
				Allowed:              true,
				RemainingDailyMicros: 4000,
			}, nil
		},
	}
	client := setupTestServer(t, mock)

	resp, err := client.Reserve(context.Background(), &budgetv1.ReserveRequest{
		KeyId:     "key-1",
		MaxMicros: 1000,
		RequestId: "req-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ReservationId != "res-123" {
		t.Errorf("expected reservation-id res-123, got %s", resp.ReservationId)
	}
	if resp.RemainingMicros != 4000 {
		t.Errorf("expected remaining micros 4000, got %d", resp.RemainingMicros)
	}
}

func TestGRPC_Reserve_ErrorMappings(t *testing.T) {
	cases := []struct {
		name         string
		storeErr     error
		expectedCode codes.Code
	}{
		{"budget exceeded", core.ErrBudgetExceeded, codes.FailedPrecondition},
		{"rate limited", core.ErrRateLimited, codes.ResourceExhausted},
		{"not found", core.ErrNotFound, codes.NotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockStore{
				reserveFunc: func(_ context.Context, _ store.ReserveParams) (*store.ReserveResult, error) {
					return nil, tc.storeErr
				},
			}
			client := setupTestServer(t, mock)

			_, err := client.Reserve(context.Background(), &budgetv1.ReserveRequest{
				KeyId:     "k-1",
				MaxMicros: 100,
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != tc.expectedCode {
				t.Errorf("expected code %v, got %v (err: %v)", tc.expectedCode, st.Code(), err)
			}
		})
	}
}

func TestGRPC_Reserve_InvalidArguments(t *testing.T) {
	client := setupTestServer(t, &mockStore{})

	_, err := client.Reserve(context.Background(), &budgetv1.ReserveRequest{KeyId: "", MaxMicros: 100})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for empty KeyId, got %v", err)
	}

	_, err = client.Reserve(context.Background(), &budgetv1.ReserveRequest{KeyId: "k-1", MaxMicros: 0})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for zero MaxMicros, got %v", err)
	}
}

func TestGRPC_Settle_And_Release(t *testing.T) {
	mock := &mockStore{
		settleFunc: func(_ context.Context, resID string, _ int64, _ time.Time) (*store.SettleResult, error) {
			if resID == "not-found" {
				return nil, core.ErrNotFound
			}
			if resID == "closed" {
				return nil, core.ErrReservationClosed
			}
			return &store.SettleResult{Settled: true}, nil
		},
		releaseFunc: func(_ context.Context, resID string, _ time.Time) error {
			if resID == "not-found" {
				return core.ErrNotFound
			}
			return nil
		},
	}
	client := setupTestServer(t, mock)

	_, err := client.Settle(context.Background(), &budgetv1.SettleRequest{ReservationId: "res-1", ActualMicros: 100})
	if err != nil {
		t.Fatalf("expected settle success, got %v", err)
	}

	_, err = client.Settle(context.Background(), &budgetv1.SettleRequest{ReservationId: "not-found", ActualMicros: 100})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", err)
	}

	_, err = client.Settle(context.Background(), &budgetv1.SettleRequest{ReservationId: "closed", ActualMicros: 100})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition code, got %v", err)
	}

	_, err = client.Release(context.Background(), &budgetv1.ReleaseRequest{ReservationId: "res-1"})
	if err != nil {
		t.Fatalf("expected release success, got %v", err)
	}

	_, err = client.Release(context.Background(), &budgetv1.ReleaseRequest{ReservationId: "not-found"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound code for release, got %v", err)
	}
}

func TestGRPC_Remaining(t *testing.T) {
	mock := &mockStore{
		remainingFunc: func(_ context.Context, keyID string, _ time.Time) (*store.RemainingInfo, error) {
			if keyID == "not-found" {
				return nil, core.ErrNotFound
			}
			return &store.RemainingInfo{
				KeyID:               keyID,
				DailyLimitMicros:    5000,
				MonthlyLimitMicros:  50000,
				DailySpentMicros:    1200,
				DailyReservedMicros: 300,
				MonthlySpentMicros:  5000,
			}, nil
		},
	}
	client := setupTestServer(t, mock)

	resp, err := client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "k-1"})
	if err != nil {
		t.Fatalf("expected remaining success, got %v", err)
	}
	if resp.DailyMicros != 3500 {
		t.Errorf("expected daily micros 3500, got %d", resp.DailyMicros)
	}
	if resp.MonthlyMicros != 45000 {
		t.Errorf("expected monthly micros 45000, got %d", resp.MonthlyMicros)
	}

	_, err = client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: ""})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for empty key, got %v", err)
	}

	_, err = client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "not-found"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound for unknown key, got %v", err)
	}
}
