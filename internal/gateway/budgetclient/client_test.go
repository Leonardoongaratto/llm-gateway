package budgetclient_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/budgetclient"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockBudgetServiceServer struct {
	budgetv1.UnimplementedBudgetServiceServer
	mu            sync.Mutex
	reserveDelay  time.Duration
	reserveResp   *budgetv1.ReserveResponse
	reserveErr    error
	settleCalls   int
	settleFailNum int
	settleErr     error
	releaseCalls  int
	releaseErr    error
}

func (m *mockBudgetServiceServer) Reserve(ctx context.Context, _ *budgetv1.ReserveRequest) (*budgetv1.ReserveResponse, error) {
	m.mu.Lock()
	delay := m.reserveDelay
	resp := m.reserveResp
	err := m.reserveErr
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *mockBudgetServiceServer) Settle(_ context.Context, _ *budgetv1.SettleRequest) (*budgetv1.SettleResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settleCalls++
	if m.settleFailNum > 0 {
		m.settleFailNum--
		return nil, status.Error(codes.Unavailable, "transient unavailable")
	}
	if m.settleErr != nil {
		return nil, m.settleErr
	}
	return &budgetv1.SettleResponse{}, nil
}

func (m *mockBudgetServiceServer) Release(_ context.Context, _ *budgetv1.ReleaseRequest) (*budgetv1.ReleaseResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls++
	if m.releaseErr != nil {
		return nil, m.releaseErr
	}
	return &budgetv1.ReleaseResponse{}, nil
}

type mockBudgetServiceClient struct {
	server *mockBudgetServiceServer
}

func (m *mockBudgetServiceClient) Reserve(ctx context.Context, in *budgetv1.ReserveRequest, _ ...grpc.CallOption) (*budgetv1.ReserveResponse, error) {
	return m.server.Reserve(ctx, in)
}

func (m *mockBudgetServiceClient) Settle(ctx context.Context, in *budgetv1.SettleRequest, _ ...grpc.CallOption) (*budgetv1.SettleResponse, error) {
	return m.server.Settle(ctx, in)
}

func (m *mockBudgetServiceClient) Release(ctx context.Context, in *budgetv1.ReleaseRequest, _ ...grpc.CallOption) (*budgetv1.ReleaseResponse, error) {
	return m.server.Release(ctx, in)
}

func (m *mockBudgetServiceClient) Remaining(_ context.Context, _ *budgetv1.RemainingRequest, _ ...grpc.CallOption) (*budgetv1.RemainingResponse, error) {
	return &budgetv1.RemainingResponse{}, nil
}

func TestClient_Reserve_Success(t *testing.T) {
	mockSrv := &mockBudgetServiceServer{
		reserveResp: &budgetv1.ReserveResponse{
			ReservationId:   "res-123",
			RemainingMicros: 5000000,
		},
	}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	resID, rem, err := client.Reserve(context.Background(), "key-1", 10000, "req-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resID != "res-123" || rem != 5000000 {
		t.Fatalf("expected (res-123, 5000000), got (%s, %d)", resID, rem)
	}
}

func TestClient_Reserve_BudgetExceeded(t *testing.T) {
	st := status.New(codes.FailedPrecondition, "budget exceeded")
	stWithDetails, err := st.WithDetails(&budgetv1.ReserveDenial{
		Reason: budgetv1.ReserveDenial_BUDGET_EXCEEDED,
	})
	if err != nil {
		t.Fatalf("failed to add details: %v", err)
	}

	mockSrv := &mockBudgetServiceServer{reserveErr: stWithDetails.Err()}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	_, _, err = client.Reserve(context.Background(), "key-1", 10000, "req-1")
	if !errors.Is(err, llm.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}

func TestClient_Reserve_RateLimited(t *testing.T) {
	st := status.New(codes.ResourceExhausted, "rate limited")
	stWithDetails, err := st.WithDetails(&budgetv1.ReserveDenial{
		Reason:            budgetv1.ReserveDenial_RATE_LIMITED,
		RetryAfterSeconds: 45,
	})
	if err != nil {
		t.Fatalf("failed to add details: %v", err)
	}

	mockSrv := &mockBudgetServiceServer{reserveErr: stWithDetails.Err()}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	_, _, err = client.Reserve(context.Background(), "key-1", 10000, "req-1")
	if !errors.Is(err, llm.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}

	var rateErr *budgetclient.RateLimitError
	if !errors.As(err, &rateErr) {
		t.Fatalf("expected *RateLimitError, got %T", err)
	}
	if rateErr.RetryAfterSeconds != 45 {
		t.Fatalf("expected RetryAfterSeconds=45, got %d", rateErr.RetryAfterSeconds)
	}
}

func TestClient_Reserve_FailClosed_OnTimeout(t *testing.T) {
	// Delay longer than default 200ms timeout
	mockSrv := &mockBudgetServiceServer{
		reserveDelay: 350 * time.Millisecond,
		reserveResp:  &budgetv1.ReserveResponse{ReservationId: "res-late"},
	}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv},
		budgetclient.WithTimeout(100*time.Millisecond),
	)
	defer func() { _ = client.Close() }()

	_, _, err := client.Reserve(context.Background(), "key-1", 10000, "req-1")
	if !errors.Is(err, llm.ErrBudgetUnavailable) {
		t.Fatalf("expected fail-closed ErrBudgetUnavailable, got %v", err)
	}
}

func TestClient_Reserve_FailClosed_OnUnavailable(t *testing.T) {
	mockSrv := &mockBudgetServiceServer{
		reserveErr: status.Error(codes.Unavailable, "budget service down"),
	}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	_, _, err := client.Reserve(context.Background(), "key-1", 10000, "req-1")
	if !errors.Is(err, llm.ErrBudgetUnavailable) {
		t.Fatalf("expected fail-closed ErrBudgetUnavailable, got %v", err)
	}
}

func TestClient_Settle_Success(t *testing.T) {
	mockSrv := &mockBudgetServiceServer{}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	if err := client.Settle(context.Background(), "res-123", 4500); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockSrv.settleCalls != 1 {
		t.Fatalf("expected 1 settle call, got %d", mockSrv.settleCalls)
	}
}

func TestClient_SettleAsync_BackgroundRetry(t *testing.T) {
	// Fail first call with transient error, second call succeeds
	mockSrv := &mockBudgetServiceServer{
		settleFailNum: 1,
	}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv},
		budgetclient.WithRetryBackoff(10*time.Millisecond),
	)

	client.SettleAsync("res-retry-1", 5000)

	// Close cleanly waits for background worker to flush queue
	if err := client.Close(); err != nil {
		t.Fatalf("unexpected error closing client: %v", err)
	}

	mockSrv.mu.Lock()
	calls := mockSrv.settleCalls
	mockSrv.mu.Unlock()

	if calls < 2 {
		t.Fatalf("expected at least 2 settle calls with retry, got %d", calls)
	}
}

func TestClient_Release_Success(t *testing.T) {
	mockSrv := &mockBudgetServiceServer{}
	client := budgetclient.New(&mockBudgetServiceClient{server: mockSrv})
	defer func() { _ = client.Close() }()

	if err := client.Release(context.Background(), "res-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockSrv.releaseCalls != 1 {
		t.Fatalf("expected 1 release call, got %d", mockSrv.releaseCalls)
	}
}
