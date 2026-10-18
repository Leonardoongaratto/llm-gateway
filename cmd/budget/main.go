package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
)

type Config struct {
	ServiceName  string `env:"SERVICE_NAME" default:"budget"`
	HTTPPort     int    `env:"HTTP_PORT" default:"8791"`
	GRPCPort     int    `env:"GRPC_PORT" default:"9102"`
	LogLevel     string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint string `env:"OTEL_ENDPOINT" default:""`
}

type placeholderBudgetServer struct {
	budgetv1.UnimplementedBudgetServiceServer
}

func (s *placeholderBudgetServer) Remaining(ctx context.Context, req *budgetv1.RemainingRequest) (*budgetv1.RemainingResponse, error) {
	return &budgetv1.RemainingResponse{DailyMicros: 1000000, MonthlyMicros: 10000000}, nil
}

func run(ctx context.Context, cfg Config) error {
	logger := logging.New(os.Stdout, cfg.LogLevel)
	slog.SetDefault(logger)

	otelProvider, err := otel.Init(ctx, otel.Config{
		ServiceName:  cfg.ServiceName,
		OTelEndpoint: cfg.OTelEndpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to init otel: %w", err)
	}
	defer func() { _ = otelProvider.Shutdown(context.Background()) }()

	checker := health.New()

	grpcLis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to bind grpc port: %w", err)
	}

	grpcServer := grpcx.NewServer(grpcx.WithServerLogger(logger))
	budgetv1.RegisterBudgetServiceServer(grpcServer, &placeholderBudgetServer{})

	go func() {
		logger.Info("starting budget grpc server", slog.Int("port", cfg.GRPCPort))
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error("grpc server error", slog.Any("error", err))
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProvider.MetricsHandler())

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting budget http server", slog.Int("port", cfg.HTTPPort))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down budget service")
	grpcServer.GracefulStop()

	server.SetKeepAlivesEnabled(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shutdown gracefully", slog.Any("error", err))
		return err
	}
	logger.Info("shutdown completed successfully")
	return nil
}

func main() {
	cfg, err := config.LoadFromEnv[Config]("LGW_")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *cfg); err != nil {
		os.Exit(1)
	}
}
