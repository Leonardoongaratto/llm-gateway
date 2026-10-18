package grpcx_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type mockBudgetServer struct {
	budgetv1.UnimplementedBudgetServiceServer
	remainingCalls atomic.Int32
	sleepDuration  time.Duration
}

func (m *mockBudgetServer) Remaining(ctx context.Context, req *budgetv1.RemainingRequest) (*budgetv1.RemainingResponse, error) {
	call := m.remainingCalls.Add(1)

	if m.sleepDuration > 0 {
		select {
		case <-time.After(m.sleepDuration):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if req.KeyId == "fail-invalid" {
		return nil, status.Error(codes.InvalidArgument, "invalid key id")
	}

	if req.KeyId == "retry-twice" {
		if call <= 2 {
			return nil, status.Error(codes.Unavailable, "temporarily unavailable")
		}
		return &budgetv1.RemainingResponse{DailyMicros: 5000, MonthlyMicros: 100000}, nil
	}

	return &budgetv1.RemainingResponse{DailyMicros: 1000, MonthlyMicros: 50000}, nil
}

// AC-1A-08: Given um servidor gRPC que responde Unavailable duas vezes e depois OK,
// when o cliente grpcx chama, then há sucesso com 2 retentativas; com InvalidArgument, não há retentativa;
// com o servidor lento além do deadline, retorna DeadlineExceeded dentro do prazo + 10 ms.
func TestAC1A08_RetryAndDeadline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on random port: %v", err)
	}
	defer lis.Close()

	srv := grpcx.NewServer()
	mock := &mockBudgetServer{}
	budgetv1.RegisterBudgetServiceServer(srv, mock)

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.GracefulStop()

	clientConn, err := grpcx.Dial(context.Background(), lis.Addr().String(),
		grpcx.WithDefaultDeadline(50*time.Millisecond),
		grpcx.WithMaxRetries(2),
	)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer clientConn.Close()

	client := budgetv1.NewBudgetServiceClient(clientConn)

	// Case 1: Unavailable twice, then success on 3rd attempt
	mock.remainingCalls.Store(0)
	resp, err := client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "retry-twice"})
	if err != nil {
		t.Fatalf("expected success after 2 retries, got: %v", err)
	}
	if resp.DailyMicros != 5000 {
		t.Errorf("expected 5000 micros, got %d", resp.DailyMicros)
	}
	if calls := mock.remainingCalls.Load(); calls != 3 {
		t.Errorf("expected exactly 3 calls (1 initial + 2 retries), got %d", calls)
	}

	// Case 2: InvalidArgument, must NOT retry
	mock.remainingCalls.Store(0)
	_, err = client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "fail-invalid"})
	if err == nil {
		t.Fatal("expected InvalidArgument error, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected status InvalidArgument, got: %v", err)
	}
	if calls := mock.remainingCalls.Load(); calls != 1 {
		t.Errorf("expected exactly 1 call (no retries), got %d", calls)
	}

	// Case 3: Slow server beyond deadline, returns DeadlineExceeded within deadline + 15ms
	mock.remainingCalls.Store(0)
	mock.sleepDuration = 100 * time.Millisecond

	start := time.Now()
	_, err = client.Remaining(context.Background(), &budgetv1.RemainingRequest{KeyId: "normal"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected DeadlineExceeded error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded code, got: %v", err)
	}

	if elapsed > 60*time.Millisecond {
		t.Errorf("expected call to fail within deadline + 10ms (<= 60ms), took %v", elapsed)
	}
}

func TestHealthCheck(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	srv := grpcx.NewServer()
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	conn, err := grpcx.Dial(context.Background(), lis.Addr().String(), grpcx.WithDefaultDeadline(500*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	healthClient := grpc_health_v1.NewHealthClient(conn)
	res, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if res.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING status, got %v", res.Status)
	}
}
