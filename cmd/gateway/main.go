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
	ServiceName      string `env:"SERVICE_NAME" default:"gateway"`
	HTTPPort         int    `env:"HTTP_PORT" default:"8788"`
	LogLevel         string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint     string `env:"OTEL_ENDPOINT" default:""`
	KeyPepper        string `env:"KEY_PEPPER,required"`
	ControlAddr      string `env:"CONTROL_ADDR" default:""`
	BudgetAddr       string `env:"BUDGET_ADDR" default:""`
	NATSURL          string `env:"NATS_URL" default:""`
	AnthropicAPIKey  string `env:"ANTHROPIC_API_KEY" default:""`
	AnthropicBaseURL string `env:"ANTHROPIC_BASE_URL" default:""`
	OpenAIAPIKey     string `env:"OPENAI_API_KEY" default:""`
	OpenAIBaseURL    string `env:"OPENAI_BASE_URL" default:""`
	GeminiAPIKey     string `env:"GEMINI_API_KEY" default:""`
	GeminiBaseURL    string `env:"GEMINI_BASE_URL" default:""`
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

func setupControl(ctx context.Context, controlAddr, keyPepper string, logger *slog.Logger) (*auth.Manager, func(), error) {
	var controlClient controlv1.ControlServiceClient
	cleanup := func() {}

	if controlAddr != "" {
		conn, err := grpcx.Dial(ctx, controlAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to dial control service at %s: %w", controlAddr, err)
		}
		controlClient = controlv1.NewControlServiceClient(conn)
		cleanup = func() { _ = conn.Close() }
	}

	authMgr, err := auth.NewManager(keyPepper, controlClient, auth.WithLogger(logger))
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to create auth manager: %w", err)
	}
	if controlClient != nil {
		if err := authMgr.LoadSnapshot(ctx); err != nil {
			logger.Warn("initial snapshot load failed, will retry on event", slog.Any("error", err))
		}
	}
	return authMgr, cleanup, nil
}

func setupNATS(ctx context.Context, natsURL string, authMgr *auth.Manager, logger *slog.Logger) (core.EventPublisher, func(), error) {
	if natsURL == "" {
		return nil, func() {}, nil
	}
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to nats at %s: %w", natsURL, err)
	}
	_, _ = authMgr.StartConfigWatcher(ctx, js)
	publisher := events.NewPublisher(js, events.WithLogger(logger))
	cleanup := func() {
		_ = publisher.Close()
		nc.Close()
	}
	return publisher, cleanup, nil
}

func setupAuthAndNATS(ctx context.Context, cfg Config, logger *slog.Logger) (*auth.Manager, core.EventPublisher, func(), error) {
	authMgr, cleanupCtrl, err := setupControl(ctx, cfg.ControlAddr, cfg.KeyPepper, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	pub, cleanupNATS, err := setupNATS(ctx, cfg.NATSURL, authMgr, logger)
	if err != nil {
		cleanupCtrl()
		return nil, nil, nil, err
	}

	return authMgr, pub, func() {
		cleanupNATS()
		cleanupCtrl()
	}, nil
}

func setupBudget(ctx context.Context, cfg Config, logger *slog.Logger) (core.BudgetClient, func(), error) {
	if cfg.BudgetAddr == "" {
		return nil, func() {}, nil
	}
	conn, err := grpcx.Dial(ctx, cfg.BudgetAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to dial budget service at %s: %w", cfg.BudgetAddr, err)
	}
	bc := budgetclient.New(budgetv1.NewBudgetServiceClient(conn), budgetclient.WithLogger(logger))
	return bc, func() {
		_ = bc.Close()
		_ = conn.Close()
	}, nil
}

func decorateProvider(p provider.Provider) provider.Provider {
	withTimeout := decorate.WithTimeout(p, 30*time.Second)
	withBreaker := decorate.WithBreaker(withTimeout, decorate.WithFailureThreshold(5), decorate.WithCooldown(30*time.Second))
	return decorate.WithMetrics(withBreaker, nil)
}

func setupRouter(cfg Config, pub core.EventPublisher) *router.Router {
	r := router.New()
	var anthOpts []anthropic.Option
	if cfg.AnthropicBaseURL != "" {
		anthOpts = append(anthOpts, anthropic.WithBaseURL(cfg.AnthropicBaseURL))
	}
	var oaiOpts []openai.Option
	if cfg.OpenAIBaseURL != "" {
		oaiOpts = append(oaiOpts, openai.WithBaseURL(cfg.OpenAIBaseURL))
	}
	var gemOpts []gemini.Option
	if cfg.GeminiBaseURL != "" {
		gemOpts = append(gemOpts, gemini.WithBaseURL(cfg.GeminiBaseURL))
	}

	anth := decorateProvider(anthropic.New(cfg.AnthropicAPIKey, anthOpts...))
	oai := decorateProvider(openai.New(cfg.OpenAIAPIKey, oaiOpts...))
	gem := decorateProvider(gemini.New(cfg.GeminiAPIKey, gemOpts...))

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

func buildCoreService(cfg Config, authMgr *auth.Manager, budgetCli core.BudgetClient, pub core.EventPublisher, logger *slog.Logger) *core.Service {
	r := setupRouter(cfg, pub)
	localCache := cache.New(cache.WithCapacity(10000), cache.WithTTL(5*time.Minute))
	return core.NewService(core.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   budgetCli,
		EventPublisher: pub,
		LocalCache:     localCache,
		Router:         r,
		Logger:         logger,
	})
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

	authMgr, publisher, cleanupAuth, err := setupAuthAndNATS(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer cleanupAuth()

	budgetCli, cleanupBudget, err := setupBudget(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer cleanupBudget()

	activeAuthMgr := options.authManager
	if activeAuthMgr == nil {
		activeAuthMgr = authMgr
	}

	checker.RegisterCheck("snapshot", func(_ context.Context) error {
		if activeAuthMgr == nil || activeAuthMgr.GetSnapshot() == nil {
			return errors.New("auth snapshot not loaded")
		}
		return nil
	})
	checker.RegisterCheck("budget", func(_ context.Context) error {
		if budgetCli == nil && options.coreService == nil {
			return errors.New("budget client not configured")
		}
		return nil
	})

	svc := options.coreService
	if svc == nil {
		svc = buildCoreService(cfg, activeAuthMgr, budgetCli, publisher, logger)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProvider.MetricsHandler())

	apiHandler := api.NewHandler(svc, activeAuthMgr)
	apiHandler.RegisterRoutes(mux)

	return startHTTPServer(ctx, cfg.HTTPPort, mux, logger)
}

func startHTTPServer(ctx context.Context, port int, handler http.Handler, logger *slog.Logger) error {
	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting gateway service", slog.Int("port", port))
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
