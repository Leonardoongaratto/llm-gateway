package budgetclient

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultTimeout      = 200 * time.Millisecond
	DefaultRetryBackoff = 50 * time.Millisecond
	DefaultMaxRetries   = 5
	QueueBufferSize     = 1024
)

// RateLimitError indicates the key exceeded its allowed RPM.
type RateLimitError struct {
	RetryAfterSeconds int64
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limit exceeded (retry after %ds)", e.RetryAfterSeconds)
}

func (e *RateLimitError) Is(target error) bool {
	return target == llm.ErrRateLimited
}

type settleTask struct {
	ctx           context.Context
	reservationID string
	actualMicros  int64
	attempts      int
}

// Option configures the Client.
type Option func(*Client)

// WithTimeout configures the gRPC call timeout (default 200ms).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithRetryBackoff configures the initial retry backoff for asynchronous Settle.
func WithRetryBackoff(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.retryBackoff = d
		}
	}
}

// WithMaxRetries configures the maximum retry attempts for asynchronous Settle.
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxRetries = n
		}
	}
}

// WithLogger sets the logger for the Client.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		if l != nil {
			c.logger = l
		}
	}
}

// Client is a resilient gRPC client for BudgetService with fail-closed semantics and background Settle retry.
type Client struct {
	grpcClient   budgetv1.BudgetServiceClient
	timeout      time.Duration
	retryBackoff time.Duration
	maxRetries   int
	queue        chan settleTask
	wg           sync.WaitGroup
	closeOnce    sync.Once
	logger       *slog.Logger
}

// New creates and starts a new budget Client.
func New(grpcClient budgetv1.BudgetServiceClient, opts ...Option) *Client {
	c := &Client{
		grpcClient:   grpcClient,
		timeout:      DefaultTimeout,
		retryBackoff: DefaultRetryBackoff,
		maxRetries:   DefaultMaxRetries,
		queue:        make(chan settleTask, QueueBufferSize),
		logger:       slog.Default(),
	}

	for _, opt := range opts {
		opt(c)
	}

	c.wg.Add(1)
	go c.worker()

	return c
}

func mapDenialDetails(st *status.Status) error {
	for _, d := range st.Details() {
		if denial, ok := d.(*budgetv1.ReserveDenial); ok {
			switch denial.Reason {
			case budgetv1.ReserveDenial_BUDGET_EXCEEDED:
				return llm.ErrBudgetExceeded
			case budgetv1.ReserveDenial_RATE_LIMITED:
				sec := denial.RetryAfterSeconds
				if sec <= 0 {
					sec = 60
				}
				return &RateLimitError{RetryAfterSeconds: sec}
			case budgetv1.ReserveDenial_UNKNOWN_KEY:
				return llm.ErrBudgetUnavailable
			}
		}
	}
	return nil
}

func (c *Client) mapGrpcError(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return llm.ErrBudgetUnavailable
	}

	if errFromDetails := mapDenialDetails(st); errFromDetails != nil {
		return errFromDetails
	}

	switch st.Code() {
	case codes.FailedPrecondition:
		if strings.Contains(st.Message(), "budget exceeded") {
			return llm.ErrBudgetExceeded
		}
	case codes.ResourceExhausted:
		return &RateLimitError{RetryAfterSeconds: 60}
	}

	return llm.ErrBudgetUnavailable
}

// Reserve requests budget reservation for a virtual key.
// Enforces a strict timeout and fail-closed behavior.
func (c *Client) Reserve(ctx context.Context, keyID string, maxMicros int64, requestID string) (string, int64, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := c.grpcClient.Reserve(reqCtx, &budgetv1.ReserveRequest{
		KeyId:     keyID,
		MaxMicros: maxMicros,
		RequestId: requestID,
	})
	if err != nil {
		mappedErr := c.mapGrpcError(err)
		c.logger.WarnContext(ctx, "budget reserve failed",
			slog.String("key_id", keyID),
			slog.Int64("max_micros", maxMicros),
			slog.String("request_id", requestID),
			slog.Any("error", err),
			slog.Any("mapped_error", mappedErr),
		)
		return "", 0, mappedErr
	}

	return resp.ReservationId, resp.RemainingMicros, nil
}

// Settle updates a reservation to settled status with the actual measured cost.
func (c *Client) Settle(ctx context.Context, reservationID string, actualMicros int64) error {
	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
	defer cancel()

	_, err := c.grpcClient.Settle(reqCtx, &budgetv1.SettleRequest{
		ReservationId: reservationID,
		ActualMicros:  actualMicros,
	})
	if err != nil {
		return c.mapGrpcError(err)
	}
	return nil
}

// SettleAsync enqueues Settle execution in the background with automatic retries, propagating trace context.
func (c *Client) SettleAsync(ctx context.Context, reservationID string, actualMicros int64) {
	taskCtx := context.Background()
	if ctx != nil {
		taskCtx = context.WithoutCancel(ctx)
	}
	select {
	case c.queue <- settleTask{ctx: taskCtx, reservationID: reservationID, actualMicros: actualMicros}:
	default:
		c.logger.Error("settle queue is full, dropping async settle",
			slog.String("reservation_id", reservationID),
		)
	}
}

// Release releases a reservation so reserved funds return to the available balance.
func (c *Client) Release(ctx context.Context, reservationID string) error {
	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.timeout)
	defer cancel()

	_, err := c.grpcClient.Release(reqCtx, &budgetv1.ReleaseRequest{
		ReservationId: reservationID,
	})
	if err != nil {
		return c.mapGrpcError(err)
	}
	return nil
}

func isRetryable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal:
		return true
	default:
		return false
	}
}

func (c *Client) executeSettleTask(task *settleTask) bool {
	baseCtx := task.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, c.timeout)
	defer cancel()

	_, err := c.grpcClient.Settle(ctx, &budgetv1.SettleRequest{
		ReservationId: task.reservationID,
		ActualMicros:  task.actualMicros,
	})
	if err == nil {
		return true
	}

	st, ok := status.FromError(err)
	if !ok || isRetryable(st.Code()) {
		task.attempts++
		if task.attempts < c.maxRetries {
			return false
		}
	}

	c.logger.Error("background settle failed permanently",
		slog.String("reservation_id", task.reservationID),
		slog.Int("attempts", task.attempts),
		slog.Any("error", err),
	)
	return true
}

func (c *Client) worker() {
	defer c.wg.Done()
	for task := range c.queue {
		t := task
		for !c.executeSettleTask(&t) {
			time.Sleep(c.retryBackoff * time.Duration(1<<t.attempts))
		}
	}
}

// Close gracefully closes the background queue and waits for pending Settle tasks.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.queue)
		c.wg.Wait()
	})
	return nil
}
