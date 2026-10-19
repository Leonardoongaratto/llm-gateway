package decorate_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/decorate"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestWithTimeout(t *testing.T) {
	fp := fake.New(fake.WithDelay(200 * time.Millisecond))
	decorated := decorate.WithTimeout(fp, 50*time.Millisecond)

	_, err := decorated.Do(context.Background(), &llm.Request{ID: "req-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	if decorated.Name() != fp.Name() {
		t.Errorf("expected decorated name %s, got %s", fp.Name(), decorated.Name())
	}

	// Test timeout <= 0 returns inner
	if decorate.WithTimeout(fp, 0) != fp {
		t.Error("expected original provider when timeout <= 0")
	}

	// Test DoStream under timeout
	streamDec := decorate.WithTimeout(fake.New(
		fake.WithStreamChunks(llm.Chunk{Content: "chunk1"}),
	), 1*time.Second)
	ch, err := streamDec.DoStream(context.Background(), &llm.Request{ID: "req-stream"})
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	var count int
	for range ch {
		count++
	}
	if count != 1 {
		t.Fatalf("expected 1 chunk, got %d", count)
	}
}

func TestWithBreaker_AC1D07_OpensAfter5ConsecutiveFailures(t *testing.T) {
	err503 := &provider.UpstreamError{StatusCode: 503, Message: "service unavailable", Retryable: true}
	fp := fake.New(fake.WithError(err503))

	cb := decorate.WithBreaker(fp,
		decorate.WithFailureThreshold(5),
		decorate.WithCooldown(500*time.Millisecond),
	)

	req := &llm.Request{ID: "req-cb"}

	// 5 consecutive 5xx failures
	for i := 1; i <= 5; i++ {
		_, err := cb.Do(context.Background(), req)
		if !errors.Is(err, err503) {
			t.Fatalf("call %d: expected err503, got %v", i, err)
		}
	}

	if fp.CallCount() != 5 {
		t.Fatalf("expected 5 calls, got %d", fp.CallCount())
	}

	// 6th call: circuit breaker is open!
	// Must return ErrCircuitOpen without executing the network call on the underlying provider.
	_, err := cb.Do(context.Background(), req)
	if !errors.Is(err, decorate.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen on 6th call, got %v", err)
	}
	if !provider.IsRetryable(err) {
		t.Errorf("ErrCircuitOpen must be retryable for fallback routing")
	}

	// Ensure provider was NOT called on the 6th attempt
	if fp.CallCount() != 5 {
		t.Fatalf("circuit breaker did not short-circuit: call count is %d, expected 5", fp.CallCount())
	}
}

func TestWithBreaker_400DoesNotTripBreaker(t *testing.T) {
	err400 := &provider.UpstreamError{StatusCode: 400, Message: "invalid prompt", Retryable: false}
	fp := fake.New(fake.WithError(err400))

	cb := decorate.WithBreaker(fp,
		decorate.WithFailureThreshold(5),
	)

	req := &llm.Request{ID: "req-400"}

	// 10 consecutive 400 errors
	for i := 0; i < 10; i++ {
		_, err := cb.Do(context.Background(), req)
		if !errors.Is(err, err400) {
			t.Fatalf("expected err400, got %v", err)
		}
	}

	// Breaker should NOT be open because 400 errors are client/validation errors
	if fp.CallCount() != 10 {
		t.Fatalf("expected 10 calls, got %d", fp.CallCount())
	}
}

func TestWithBreaker_HalfOpenAndRecovery(t *testing.T) {
	err500 := &provider.UpstreamError{StatusCode: 500, Message: "error", Retryable: true}
	fp := fake.New(
		fake.WithErrorSequence(err500, err500, err500, nil), // fail 3 times then recover
	)

	cb := decorate.WithBreaker(fp,
		decorate.WithFailureThreshold(3),
		decorate.WithCooldown(50*time.Millisecond),
	)

	req := &llm.Request{ID: "req-rec"}

	// 3 failures open breaker
	for i := 0; i < 3; i++ {
		_, _ = cb.Do(context.Background(), req)
	}

	// Breaker is open
	_, err := cb.Do(context.Background(), req)
	if !errors.Is(err, decorate.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	// DoStream should also be blocked when open
	_, err = cb.DoStream(context.Background(), req)
	if !errors.Is(err, decorate.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen on DoStream, got %v", err)
	}

	// Wait for cooldown to transition to Half-Open
	time.Sleep(70 * time.Millisecond)

	// Canary call succeeds
	resp, err := cb.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("canary call should succeed, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// Next call also succeeds (circuit closed again)
	resp2, err := cb.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("circuit should be closed and call succeed, got %v", err)
	}
	if resp2 == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestWithMetrics(t *testing.T) {
	fp := fake.New()
	decorated := decorate.WithMetrics(fp, nil)

	resp, err := decorated.Do(context.Background(), &llm.Request{ID: "req-m"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	ch, err := decorated.DoStream(context.Background(), &llm.Request{ID: "req-ms"})
	if err != nil {
		t.Fatalf("unexpected stream error: %v", err)
	}
	for range ch {
	}
}

func TestWithBreaker_StreamHalfOpenCancelDoesNotDeadlock(t *testing.T) {
	err500 := &provider.UpstreamError{StatusCode: 500, Message: "error", Retryable: true}
	fp := fake.New(
		fake.WithErrorSequence(err500, err500, err500, nil, nil),
	)
	fp.SetStreamChunks([]llm.Chunk{{ID: "c1", Content: "1"}, {ID: "c2", Content: "2"}})
	fp.SetDelay(30 * time.Millisecond)

	cb := decorate.WithBreaker(fp,
		decorate.WithFailureThreshold(3),
		decorate.WithCooldown(50*time.Millisecond),
	)

	req := &llm.Request{ID: "req-halfopen"}
	for i := 0; i < 3; i++ {
		_, _ = cb.Do(context.Background(), req)
	}

	time.Sleep(60 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cb.DoStream(ctx, req)
	if err != nil {
		t.Fatalf("expected stream to be allowed in half-open, got %v", err)
	}

	<-stream
	cancel()

	time.Sleep(50 * time.Millisecond)
	resp, err := cb.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("expected next call to execute after stream cancel in half-open, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}
