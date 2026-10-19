package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/edge/proxy"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
)

type Config struct {
	ServiceName  string `env:"SERVICE_NAME" default:"edge"`
	HTTPPort     int    `env:"HTTP_PORT" default:"8787"`
	LogLevel     string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint string `env:"OTEL_ENDPOINT" default:""`
	GatewayURL   string `env:"GATEWAY_URL" default:"http://127.0.0.1:8788"`
	ControlURL   string `env:"CONTROL_URL" default:"http://127.0.0.1:8790"`
	UsageURL     string `env:"USAGE_URL" default:"http://127.0.0.1:8791"`
	BudgetURL    string `env:"BUDGET_URL" default:"http://127.0.0.1:8792"`
	WebDir       string `env:"WEB_DIR" default:""`
	MaxBodyBytes int64  `env:"MAX_BODY_BYTES" default:"10485760"`
}

type namedCheck struct {
	name  string
	check health.CheckFunc
}

type runOptions struct {
	logWriter   io.Writer
	extraChecks []namedCheck
}

type RunOption func(*runOptions)

func WithLogWriter(w io.Writer) RunOption {
	return func(o *runOptions) {
		o.logWriter = w
	}
}

func WithCheck(name string, check health.CheckFunc) RunOption {
	return func(o *runOptions) {
		o.extraChecks = append(o.extraChecks, namedCheck{name: name, check: check})
	}
}

func run(ctx context.Context, cfg Config, opts ...RunOption) error {
	options := runOptions{logWriter: os.Stdout}
	for _, opt := range opts {
		opt(&options)
	}

	logger := logging.New(options.logWriter, cfg.LogLevel)
	slog.SetDefault(logger)

	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "http://127.0.0.1:8788"
	}
	if cfg.ControlURL == "" {
		cfg.ControlURL = "http://127.0.0.1:8790"
	}
	if cfg.UsageURL == "" {
		cfg.UsageURL = "http://127.0.0.1:8791"
	}
	if cfg.BudgetURL == "" {
		cfg.BudgetURL = "http://127.0.0.1:8792"
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 10485760
	}

	otelProvider, err := otel.Init(ctx, otel.Config{
		ServiceName:  cfg.ServiceName,
		OTelEndpoint: cfg.OTelEndpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to init otel: %w", err)
	}
	defer func() { _ = otelProvider.Shutdown(context.Background()) }()

	checker := health.New()
	for _, ec := range options.extraChecks {
		checker.RegisterCheck(ec.name, ec.check)
	}

	proxyHandler, err := proxy.New(proxy.Config{
		GatewayURL:   cfg.GatewayURL,
		ControlURL:   cfg.ControlURL,
		UsageURL:     cfg.UsageURL,
		BudgetURL:    cfg.BudgetURL,
		WebDir:       cfg.WebDir,
		MaxBodyBytes: cfg.MaxBodyBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to create edge proxy handler: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProvider.MetricsHandler())
	mux.Handle("/", proxyHandler)

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort),
		Handler:           otel.HTTPHandler(mux, "edge"),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting edge service", slog.Int("port", cfg.HTTPPort))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down edge service")
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

func realMain() int {
	cfg, err := config.LoadFromEnv[Config]("LGW_")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *cfg); err != nil {
		fmt.Fprintf(os.Stderr, "service error: %v\n", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(realMain())
}
