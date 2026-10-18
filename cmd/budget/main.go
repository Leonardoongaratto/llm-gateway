package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/grpcapi"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/outbox"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/sweeper"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/service"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
)

type Config struct {
	ServiceName    string `env:"SERVICE_NAME" default:"budget"`
	HTTPPort       int    `env:"HTTP_PORT" default:"8792"`
	GRPCPort       int    `env:"GRPC_PORT" default:"9101"`
	LogLevel       string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint   string `env:"OTEL_ENDPOINT" default:""`
	DatabaseDriver string `env:"DATABASE_DRIVER" default:"sqlite"`
	DatabaseDSN    string `env:"DATABASE_DSN" default:""`
	NATSURL        string `env:"NATS_URL" default:""`
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
		return nil, nil, fmt.Errorf("failed to run budget migrations: %w", err)
	}

	return db, store.New(db), nil
}

func startNATSServices(ctx context.Context, natsURL string, s *store.Store, logger *slog.Logger) func() {
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

	relay := outbox.NewRelay(s, js, outbox.WithRelayLogger(logger))
	go func() {
		_ = relay.Run(ctx)
	}()

	var consumeCtx jetstream.ConsumeContext
	consumer, err := outbox.NewConfigConsumer(s, js, outbox.WithConsumerLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "failed to create config consumer", slog.Any("error", err))
	} else {
		cCtx, cErr := consumer.Start(ctx)
		if cErr != nil {
			logger.ErrorContext(ctx, "failed to start config consumer", slog.Any("error", cErr))
		} else {
			consumeCtx = cCtx
		}
	}

	return func() {
		if consumeCtx != nil {
			consumeCtx.Stop()
		}
		nc.Close()
	}
}

func run(ctx context.Context, cfg Config, opts ...RunOption) error {
	options := runOptions{logWriter: os.Stdout}
	for _, opt := range opts {
		opt(&options)
	}

	logger := slog.New(slog.NewJSONHandler(options.logWriter, nil))

	if cfg.DatabaseDriver == "" {
		cfg.DatabaseDriver = "sqlite"
	}

	db, budgetStore, err := initDatabase(ctx, cfg.DatabaseDriver, cfg.DatabaseDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if options.seedStore != nil {
		if err := options.seedStore(budgetStore); err != nil {
			return fmt.Errorf("failed to seed store: %w", err)
		}
	}

	cleanupNATS := startNATSServices(ctx, cfg.NATSURL, budgetStore, logger)
	defer cleanupNATS()

	sw := sweeper.New(budgetStore, sweeper.WithInterval(30*time.Second), sweeper.WithLogger(logger))
	go func() {
		_ = sw.Run(ctx)
	}()

	checker := health.New()
	checker.RegisterCheck("budget_db", db.PingContext)
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
		RegisterGRPC: func(srv *grpc.Server) {
			budgetv1.RegisterBudgetServiceServer(srv, grpcapi.New(budgetStore))
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
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
