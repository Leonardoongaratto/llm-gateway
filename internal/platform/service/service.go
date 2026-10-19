package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
	"google.golang.org/grpc"
)

// Config defines common service server configuration.
type Config struct {
	ServiceName  string
	HTTPPort     int
	GRPCPort     int
	LogLevel     string
	OTelEndpoint string
}

// Options allows configuring service runtime options.
type Options struct {
	LogWriter    io.Writer
	Checker      *health.Checker
	RegisterGRPC func(srv *grpc.Server)
	RegisterHTTP func(mux *http.ServeMux)
}

func startGRPC(cfg Config, logger *slog.Logger, register func(*grpc.Server)) (*grpc.Server, error) {
	if cfg.GRPCPort <= 0 {
		return nil, nil
	}
	grpcLis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.GRPCPort))
	if err != nil {
		return nil, fmt.Errorf("failed to bind grpc port: %w", err)
	}
	srv := grpcx.NewServer(grpcx.WithServerLogger(logger))
	if register != nil {
		register(srv)
	}
	go func() {
		logger.Info(fmt.Sprintf("starting %s grpc server", cfg.ServiceName), slog.Int("port", cfg.GRPCPort))
		if err := srv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc server error", slog.Any("error", err))
		}
	}()
	return srv, nil
}

func buildHTTPMux(checker *health.Checker, otelProvider *otel.Provider, register func(*http.ServeMux)) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProvider.MetricsHandler())
	if register != nil {
		register(mux)
	}
	return mux
}

func startHTTP(cfg Config, logger *slog.Logger, mux *http.ServeMux) (*http.Server, <-chan error) {
	httpServer := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort),
		Handler:           otel.HTTPHandler(mux, cfg.ServiceName),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info(fmt.Sprintf("starting %s http server", cfg.ServiceName), slog.Int("port", cfg.HTTPPort))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	return httpServer, serverErr
}

func shutdownServers(cfg Config, logger *slog.Logger, grpcServer *grpc.Server, httpServer *http.Server) error {
	logger.Info(fmt.Sprintf("shutting down %s service", cfg.ServiceName))
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}

	httpServer.SetKeepAlivesEnabled(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown gracefully", slog.Any("error", err))
		return err
	}

	logger.Info("shutdown completed successfully")
	return nil
}

// Run starts the HTTP and optional gRPC servers, ensures clean shutdown,
// and guarantees that gRPC is stopped even if the HTTP server fails during startup.
func Run(ctx context.Context, cfg Config, opts Options) error {
	if opts.LogWriter == nil {
		opts.LogWriter = os.Stdout
	}
	logger := logging.New(opts.LogWriter, cfg.LogLevel)
	slog.SetDefault(logger)

	otelProvider, err := otel.Init(ctx, otel.Config{
		ServiceName:  cfg.ServiceName,
		OTelEndpoint: cfg.OTelEndpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to init otel: %w", err)
	}
	defer func() { _ = otelProvider.Shutdown(context.Background()) }()

	checker := opts.Checker
	if checker == nil {
		checker = health.New()
	}

	grpcServer, err := startGRPC(cfg, logger, opts.RegisterGRPC)
	if err != nil {
		return err
	}

	mux := buildHTTPMux(checker, otelProvider, opts.RegisterHTTP)
	httpServer, serverErr := startHTTP(cfg, logger, mux)

	var runErr error
	select {
	case runErr = <-serverErr:
		logger.Error("http server failed", slog.Any("error", runErr))
	case <-ctx.Done():
	}

	shutdownErr := shutdownServers(cfg, logger, grpcServer, httpServer)
	if runErr != nil {
		return runErr
	}
	return shutdownErr
}
