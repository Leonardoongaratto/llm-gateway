package grpcx

import (
	"context"
	"log/slog"
	rand "math/rand/v2"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type ServerOption func(*serverConfig)

type serverConfig struct {
	logger *slog.Logger
}

// WithServerLogger sets the logger used for gRPC server interceptor logging.
func WithServerLogger(logger *slog.Logger) ServerOption {
	return func(c *serverConfig) {
		c.logger = logger
	}
}

// NewServer creates a new gRPC server with recovery, logging, and health service registered.
func NewServer(opts ...ServerOption) *grpc.Server {
	cfg := &serverConfig{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		recoveryServerInterceptor(cfg.logger),
		loggingServerInterceptor(cfg.logger),
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
	)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthServer)

	return srv
}

func recoveryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "grpc server panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = status.Errorf(codes.Internal, "panic recovered: %v", r)
			}
		}()
		return handler(ctx, req)
	}
}

func loggingServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		st, _ := status.FromError(err)
		logger.DebugContext(ctx, "grpc call finished",
			slog.String("method", info.FullMethod),
			slog.String("code", st.Code().String()),
			slog.Duration("duration", duration),
		)
		return resp, err
	}
}

type ClientOption func(*clientConfig)

type clientConfig struct {
	defaultDeadline time.Duration
	maxRetries      int
}

// WithDefaultDeadline sets the fallback call timeout if the caller's context does not have a deadline.
func WithDefaultDeadline(d time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.defaultDeadline = d
	}
}

// WithMaxRetries sets the maximum number of retries on codes.Unavailable.
func WithMaxRetries(n int) ClientOption {
	return func(c *clientConfig) {
		c.maxRetries = n
	}
}

// Dial connects to a gRPC target with default credentials, deadline enforcement, and retry on Unavailable.
func Dial(_ context.Context, target string, opts ...ClientOption) (*grpc.ClientConn, error) {
	cfg := &clientConfig{
		defaultDeadline: 50 * time.Millisecond,
		maxRetries:      2,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithUnaryInterceptor(clientUnaryInterceptor(cfg)),
	}

	return grpc.NewClient(target, dialOpts...)
}

func waitWithJitter(ctx context.Context, attempt int) error {
	if attempt == 0 {
		return nil
	}
	jitter := time.Duration(rand.IntN(3)+2) * time.Millisecond * time.Duration(attempt)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
		return nil
	}
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

func executeAttempt(callCtx context.Context, attempt int, invoker grpc.UnaryInvoker, method string, req, reply any, cc *grpc.ClientConn, opts []grpc.CallOption) (bool, error) {
	if err := waitWithJitter(callCtx, attempt); err != nil {
		return true, status.FromContextError(err).Err()
	}

	err := invoker(callCtx, method, req, reply, cc, opts...)
	if err == nil {
		return true, nil
	}

	if !shouldRetry(err) || callCtx.Err() != nil {
		if callCtx.Err() != nil {
			return true, status.FromContextError(callCtx.Err()).Err()
		}
		return true, err
	}

	return false, err
}

func clientUnaryInterceptor(cfg *clientConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		callCtx := ctx
		if _, hasDeadline := ctx.Deadline(); !hasDeadline && cfg.defaultDeadline > 0 {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, cfg.defaultDeadline)
			defer cancel()
		}

		var lastErr error
		for attempt := 0; attempt <= cfg.maxRetries; attempt++ {
			done, err := executeAttempt(callCtx, attempt, invoker, method, req, reply, cc, opts)
			if done {
				return err
			}
			lastErr = err
		}

		return lastErr
	}
}
