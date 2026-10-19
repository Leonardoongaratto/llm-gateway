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

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	controlv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/api"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/budgetclient"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/events"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/anthropic"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/decorate"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/gemini"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/openai"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	ServiceName     string `env:"SERVICE_NAME" default:"gateway"`
	HTTPPort        int    `env:"HTTP_PORT" default:"8788"`
	LogLevel        string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint    string `env:"OTEL_ENDPOINT" default:""`
	KeyPepper       string `env:"KEY_PEPPER" default:"default-gateway-pepper-0123456789"`
	ControlAddr     string `env:"CONTROL_ADDR" default:""`
	BudgetAddr      string `env:"BUDGET_ADDR" default:""`
	NATSURL         string `env:"NATS_URL" default:""`
	AnthropicAPIKey string `env:"ANTHROPIC_API_KEY" default:""`
	OpenAIAPIKey    string `env:"OPENAI_API_KEY" default:""`
	GeminiAPIKey    string `env:"GEMINI_API_KEY" default:""`
}

type namedCheck struct {
	name  string
	check health.CheckFunc
}

type runOptions struct {
	logWriter   io.Writer
	extraChecks []namedCheck
	coreService *core.Service
	authManager *auth.Manager
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

func WithCoreService(svc *core.Service, authMgr *auth.Manager) RunOption {
	return func(o *runOptions) {
		o.coreService = svc
		o.authManager = authMgr
	}
}

func setupAuthAndNATS(ctx context.Context, cfg Config, logger *slog.Logger) (*auth.Manager, *events.Publisher, func()) {
	var controlClient controlv1.ControlServiceClient
	var cleanups []func()

	if cfg.ControlAddr != "" {
		if conn, err := grpcx.Dial(ctx, cfg.ControlAddr); err == nil {
			controlClient = controlv1.NewControlServiceClient(conn)
			cleanups = append(cleanups, func() { _ = conn.Close() })
		}
	}

	authMgr, _ := auth.NewManager(cfg.KeyPepper, controlClient, auth.WithLogger(logger))
	if controlClient != nil {
		_ = authMgr.LoadSnapshot(ctx)
	}

	var publisher *events.Publisher
	if cfg.NATSURL != "" {
		if nc, js, err := natsx.Connect(cfg.NATSURL); err == nil {
			_, _ = authMgr.StartConfigWatcher(ctx, js)
			publisher = events.NewPublisher(js, events.WithLogger(logger))
			cleanups = append(cleanups, func() {
				_ = publisher.Close()
				nc.Close()
			})
		}
	}

	return authMgr, publisher, func() {
		for _, c := range cleanups {
			c()
		}
	}
}

func setupBudget(ctx context.Context, cfg Config, logger *slog.Logger) (*budgetclient.Client, func()) {
	if cfg.BudgetAddr == "" {
		return nil, func() {}
	}
	conn, err := grpcx.Dial(ctx, cfg.BudgetAddr)
	if err != nil {
		return nil, func() {}
	}
	bc := budgetclient.New(budgetv1.NewBudgetServiceClient(conn), budgetclient.WithLogger(logger))
	return bc, func() {
		_ = bc.Close()
		_ = conn.Close()
	}
}

func decorateProvider(p provider.Provider) provider.Provider {
	withTimeout := decorate.WithTimeout(p, 30*time.Second)
	withBreaker := decorate.WithBreaker(withTimeout, decorate.WithFailureThreshold(5), decorate.WithCooldown(30*time.Second))
	return decorate.WithMetrics(withBreaker, nil)
}

func setupRouter(cfg Config, pub *events.Publisher) *router.Router {
	r := router.New()
	anth := decorateProvider(anthropic.New(cfg.AnthropicAPIKey))
	oai := decorateProvider(openai.New(cfg.OpenAIAPIKey))
	gem := decorateProvider(gemini.New(cfg.GeminiAPIKey))

	r.Register("anthropic", anth)
	r.Register("openai", oai)
	r.Register("gemini", gem)

	if pub != nil {
		r.SetFallbackHook(func(ctx context.Context, requestID, fromProvider, toProvider, reason string) {
			pub.PublishFallback(ctx, &eventsv1.ProviderFallback{
				RequestId:    requestID,
				OccurredAt:   timestamppb.Now(),
				FromProvider: fromProvider,
				ToProvider:   toProvider,
				Reason:       reason,
			})
		})
	}
	return r
}

func run(ctx context.Context, cfg Config, opts ...RunOption) error {
	options := runOptions{logWriter: os.Stdout}
	for _, opt := range opts {
		opt(&options)
	}

	logger := logging.New(options.logWriter, cfg.LogLevel)
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
	for _, ec := range options.extraChecks {
		checker.RegisterCheck(ec.name, ec.check)
	}

	authMgr, publisher, cleanupAuth := setupAuthAndNATS(ctx, cfg, logger)
	defer cleanupAuth()

	budgetCli, cleanupBudget := setupBudget(ctx, cfg, logger)
	defer cleanupBudget()

	svc := options.coreService
	activeAuthMgr := options.authManager
	if activeAuthMgr == nil {
		activeAuthMgr = authMgr
	}
	if svc == nil {
		r := setupRouter(cfg, publisher)
		localCache := cache.New(cache.WithCapacity(10000), cache.WithTTL(5*time.Minute))
		svc = core.NewService(core.ServiceConfig{
			AuthManager:    activeAuthMgr,
			BudgetClient:   budgetCli,
			EventPublisher: publisher,
			LocalCache:     localCache,
			Router:         r,
			Logger:         logger,
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProvider.MetricsHandler())

	apiHandler := api.NewHandler(svc, activeAuthMgr)
	apiHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", cfg.HTTPPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting gateway service", slog.Int("port", cfg.HTTPPort))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down gateway service")
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
