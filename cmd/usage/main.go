package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/service"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/consumer"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/httpapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/seed"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

type Config struct {
	ServiceName       string `env:"SERVICE_NAME" default:"usage"`
	HTTPPort          int    `env:"HTTP_PORT" default:"8791"`
	GRPCPort          int    `env:"GRPC_PORT" default:"0"`
	LogLevel          string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint      string `env:"OTEL_ENDPOINT" default:""`
	DatabaseDriver    string `env:"DATABASE_DRIVER" default:"sqlite"`
	DatabaseDSN       string `env:"DATABASE_DSN" default:""`
	NATSURL           string `env:"NATS_URL" default:""`
	AdminToken        string `env:"ADMIN_TOKEN,required"`
	ClaudeCodeDir     string `env:"CLAUDE_CODE_DIR" default:""`
	AnthropicAdminKey string `env:"ANTHROPIC_ADMIN_KEY" default:""`
	OpenAIAdminKey    string `env:"OPENAI_ADMIN_KEY" default:""`
}

type namedCheck struct {
	name  string
	check health.CheckFunc
}

type runOptions struct {
	logWriter   io.Writer
	extraChecks []namedCheck
	seedStore   func(*store.Store) error
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

func WithStoreSeeder(seeder func(*store.Store) error) RunOption {
	return func(o *runOptions) {
		o.seedStore = seeder
	}
}

func initDatabase(ctx context.Context, driver, dsn string) (*sql.DB, *store.Store, error) {
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := store.Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed to run usage migrations: %w", err)
	}

	return db, store.New(db), nil
}

func startNATSServices(ctx context.Context, natsURL string, st *store.Store, logger *slog.Logger) func() {
	if natsURL == "" {
		return func() {}
	}

	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		logger.ErrorContext(ctx, "failed to connect to nats", slog.Any("error", err))
		return func() {}
	}

	if err := natsx.InitStreams(ctx, js); err != nil {
		logger.ErrorContext(ctx, "failed to init nats streams", slog.Any("error", err))
	}

	c := consumer.New(js, st, logger)
	if err := c.Start(ctx); err != nil {
		logger.ErrorContext(ctx, "failed to start usage consumer", slog.Any("error", err))
	}

	return func() {
		c.Stop()
		nc.Close()
	}
}

func triggerClaudeCodeImport(ctx context.Context, ccImp *claudecode.Importer, logger *slog.Logger) {
	if ccImp == nil {
		return
	}
	if _, err := ccImp.Import(ctx); err != nil {
		logger.ErrorContext(ctx, "periodic claude code import failed", slog.Any("error", err))
	}
}

func triggerOrgImport(ctx context.Context, orgImp *org.Importer, source string, logger *slog.Logger) {
	if orgImp == nil || !orgImp.IsEnabled(source) {
		return
	}
	if _, err := orgImp.Import(ctx, source); err != nil {
		logger.ErrorContext(ctx, "periodic org import failed", slog.String("source", source), slog.Any("error", err))
	}
}

func runPeriodicImports(ctx context.Context, interval time.Duration, ccImp *claudecode.Importer, orgImp *org.Importer, logger *slog.Logger) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			triggerClaudeCodeImport(ctx, ccImp, logger)
			triggerOrgImport(ctx, orgImp, "anthropic-org", logger)
			triggerOrgImport(ctx, orgImp, "openai-org", logger)
		}
	}
}

func run(ctx context.Context, cfg Config, opts ...RunOption) error {
	options := runOptions{logWriter: os.Stdout}
	for _, opt := range opts {
		opt(&options)
	}

	logger := logging.New(options.logWriter, cfg.LogLevel)

	if cfg.DatabaseDriver == "" {
		cfg.DatabaseDriver = "sqlite"
	}

	db, usageStore, err := initDatabase(ctx, cfg.DatabaseDriver, cfg.DatabaseDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if options.seedStore != nil {
		if err := options.seedStore(usageStore); err != nil {
			return fmt.Errorf("failed to seed store: %w", err)
		}
	}

	cleanupNATS := startNATSServices(ctx, cfg.NATSURL, usageStore, logger)
	defer cleanupNATS()

	claudeImporter := claudecode.New(usageStore, cfg.ClaudeCodeDir, logger)
	orgImporter := org.New(usageStore, org.Config{
		AnthropicAdminKey: cfg.AnthropicAdminKey,
		OpenAIAdminKey:    cfg.OpenAIAdminKey,
	}, logger)

	if cfg.ClaudeCodeDir != "" || cfg.AnthropicAdminKey != "" || cfg.OpenAIAdminKey != "" {
		go runPeriodicImports(ctx, 15*time.Minute, claudeImporter, orgImporter, logger)
	}

	repEngine := report.NewEngine(usageStore)
	apiHandler := httpapi.NewHandler(usageStore, repEngine, claudeImporter, orgImporter, httpapi.Config{
		AdminToken: cfg.AdminToken,
	})

	checker := health.New()
	checker.RegisterCheck("usage_db", db.PingContext)
	for _, ec := range options.extraChecks {
		checker.RegisterCheck(ec.name, ec.check)
	}

	srvCfg := service.Config{
		ServiceName:  cfg.ServiceName,
		HTTPPort:     cfg.HTTPPort,
		GRPCPort:     cfg.GRPCPort,
		LogLevel:     cfg.LogLevel,
		OTelEndpoint: cfg.OTelEndpoint,
	}

	srvOpts := service.Options{
		LogWriter: options.logWriter,
		Checker:   checker,
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.Handle("/admin/", apiHandler)
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func runSeedDemo() int {
	driver := os.Getenv("LGW_DATABASE_DRIVER")
	if driver == "" {
		driver = "sqlite"
	}
	dsn := os.Getenv("LGW_DATABASE_DSN")
	if dsn == "" && driver == "sqlite" {
		dsn = ":memory:"
	}

	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database for seed: %v\n", err)
		return 1
	}
	defer db.Close()

	ctx := context.Background()
	if err := store.Migrate(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "failed to migrate db for seed: %v\n", err)
		return 1
	}

	st := store.New(db)
	outputPath := "docs/summary.example.json"
	if _, err := os.Stat("go.mod"); os.IsNotExist(err) {
		if _, err := os.Stat("../../go.mod"); err == nil {
			outputPath = "../../docs/summary.example.json"
		}
	}
	asOf := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	if _, err := seed.Demo(ctx, st, outputPath, asOf); err != nil {
		fmt.Fprintf(os.Stderr, "failed to seed demo: %v\n", err)
		return 1
	}
	fmt.Printf("Seed demo completed successfully -> %s\n", outputPath)
	return 0
}

func realMain(args []string) int {
	for _, arg := range args {
		if arg == "--seed-demo" {
			return runSeedDemo()
		}
	}

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
	os.Exit(realMain(os.Args[1:]))
}
