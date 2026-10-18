package grpcx

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"time"

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
func Dial(ctx context.Context, target string, opts ...ClientOption) (*grpc.ClientConn, error) {
	cfg := &clientConfig{
		defaultDeadline: 50 * time.Millisecond,
		maxRetries:      2,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(clientUnaryInterceptor(cfg)),
	}

	return grpc.NewClient(target, dialOpts...)
}

func clientUnaryInterceptor(cfg *clientConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		callCtx := ctx
		var cancel context.CancelFunc

		if _, hasDeadline := ctx.Deadline(); !hasDeadline && cfg.defaultDeadline > 0 {
			callCtx, cancel = context.WithTimeout(ctx, cfg.defaultDeadline)
			defer cancel()
		}

		var lastErr error
		for attempt := 0; attempt <= cfg.maxRetries; attempt++ {
			if attempt > 0 {
				// Jittered backoff: 2ms..5ms * attempt
				jitter := time.Duration(rand.IntN(3)+2) * time.Millisecond * time.Duration(attempt)
				select {
				case <-callCtx.Done():
					return callCtx.Err()
				case <-time.After(jitter):
				}
			}

			err := invoker(callCtx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			lastErr = err
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.Unavailable {
				return err
			}

			if callCtx.Err() != nil {
				return callCtx.Err()
			}
		}

		return lastErr
	}
}
