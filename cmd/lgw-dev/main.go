package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	controlv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
	budgetcore "github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	budgetgrpc "github.com/Leonardoongaratto/llm-gateway/internal/budget/grpcapi"
	budgetoutbox "github.com/Leonardoongaratto/llm-gateway/internal/budget/outbox"
	budgetstore "github.com/Leonardoongaratto/llm-gateway/internal/budget/store"
	budgetsweeper "github.com/Leonardoongaratto/llm-gateway/internal/budget/sweeper"
	controlcore "github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	controlgrpc "github.com/Leonardoongaratto/llm-gateway/internal/control/grpcapi"
	controlhttp "github.com/Leonardoongaratto/llm-gateway/internal/control/httpapi"
	controloutbox "github.com/Leonardoongaratto/llm-gateway/internal/control/outbox"
	controlstore "github.com/Leonardoongaratto/llm-gateway/internal/control/store"
	edgeproxy "github.com/Leonardoongaratto/llm-gateway/internal/edge/proxy"
	gatewayapi "github.com/Leonardoongaratto/llm-gateway/internal/gateway/api"
	gatewayauth "github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	gatewaybudget "github.com/Leonardoongaratto/llm-gateway/internal/gateway/budgetclient"
	gatewaycache "github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	gatewaycore "github.com/Leonardoongaratto/llm-gateway/internal/gateway/core"
	gatewayevents "github.com/Leonardoongaratto/llm-gateway/internal/gateway/events"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	gatewayrouter "github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/health"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/service"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	usageconsumer "github.com/Leonardoongaratto/llm-gateway/internal/usage/consumer"
	usagehttp "github.com/Leonardoongaratto/llm-gateway/internal/usage/httpapi"
	usageclaude "github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	usageorg "github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/org"
	usagereport "github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	usageseed "github.com/Leonardoongaratto/llm-gateway/internal/usage/seed"
	usagestore "github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
)

type DevConfig struct {
	EdgePort         int
	GatewayPort      int
	Gateway2Port     int
	ControlPort      int
	UsagePort        int
	BudgetPort       int
	BudgetGRPCPort   int
	ControlGRPCPort  int
	MockProvider     bool
	SeedDemo         bool
	ImportClaudeCode bool
	WebDir           string
	AdminToken       string
	KeyPepper        string
	LogLevel         string
	TempDir          string
}

func defaultDevConfig() DevConfig {
	return DevConfig{
		EdgePort:         8787,
		GatewayPort:      8788,
		Gateway2Port:     8789,
		ControlPort:      8790,
		UsagePort:        8791,
		BudgetPort:       8792,
		BudgetGRPCPort:   9101,
		ControlGRPCPort:  9102,
		MockProvider:     true,
		SeedDemo:         false,
		ImportClaudeCode: false,
		WebDir:           "",
		AdminToken:       "dev-admin-token",
		KeyPepper:        "dev-pepper-secret-32-chars-long!",
		LogLevel:         "info",
	}
}

type devServer struct {
	cfg          DevConfig
	logger       *slog.Logger
	logWriter    io.Writer
	tempDir      string
	natsSrv      *server.Server
	natsConn     *nats.Conn
	js           jetstream.JetStream
	budgetDB     *sql.DB
	controlDB    *sql.DB
	usageDB      *sql.DB
	budgetStore  *budgetstore.Store
	controlStore *controlstore.Store
	usageStore   *usagestore.Store
}

func (s *devServer) initNATS() error {
	natsStore := filepath.Join(s.tempDir, "nats")
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  natsStore,
		NoLog:     true,
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return fmt.Errorf("embedded nats server creation: %w", err)
	}
	s.natsSrv = srv

	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		return errors.New("embedded nats failed to become ready")
	}

	nc, js, err := natsx.Connect(srv.ClientURL())
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	s.natsConn = nc
	s.js = js

	if err := natsx.InitStreams(context.Background(), js); err != nil {
		return fmt.Errorf("nats init streams: %w", err)
	}
	return nil
}

func (s *devServer) initDatabases(ctx context.Context) error {
	bDB, err := sqlx.Open("sqlite", filepath.Join(s.tempDir, "budget.db"))
	if err != nil {
		return fmt.Errorf("budget db open: %w", err)
	}
	s.budgetDB = bDB
	if err := budgetstore.Migrate(ctx, bDB); err != nil {
		return fmt.Errorf("budget db migrate: %w", err)
	}
	s.budgetStore = budgetstore.New(bDB)

	cDB, err := sqlx.Open("sqlite", filepath.Join(s.tempDir, "control.db"))
	if err != nil {
		return fmt.Errorf("control db open: %w", err)
	}
	s.controlDB = cDB
	if err := controlstore.Migrate(ctx, cDB); err != nil {
		return fmt.Errorf("control db migrate: %w", err)
	}
	s.controlStore = controlstore.New(cDB)

	uDB, err := sqlx.Open("sqlite", filepath.Join(s.tempDir, "usage.db"))
	if err != nil {
		return fmt.Errorf("usage db open: %w", err)
	}
	s.usageDB = uDB
	if err := usagestore.Migrate(ctx, uDB); err != nil {
		return fmt.Errorf("usage db migrate: %w", err)
	}
	s.usageStore = usagestore.New(uDB)

	return nil
}

func (s *devServer) seedData(ctx context.Context) error {
	devKey := "lgw_0123456789abcdef0123456789abcdef"
	keyHash, err := controlcore.HashKey(devKey, s.cfg.KeyPepper)
	if err != nil {
		return fmt.Errorf("dev key hashing: %w", err)
	}
	keyID := "key-dev-default"

	vk := controlcore.VirtualKey{
		ID:        keyID,
		Name:      "Default Dev Key",
		KeyHash:   keyHash,
		CreatedAt: time.Now(),
	}
	if _, err := s.controlStore.CreateKey(ctx, vk); err != nil {
		s.logger.Warn("could not create dev virtual key", slog.Any("error", err))
	}

	limit := budgetcore.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   1000000000,
		MonthlyLimitMicros: 5000000000,
		RPM:                10000,
	}
	if err := s.budgetStore.SetKeyLimits(ctx, limit); err != nil {
		s.logger.Warn("could not upsert dev limit", slog.Any("error", err))
	}

	prices := []controlcore.Pricing{
		{
			Model:         "gpt-4o",
			Provider:      "openai",
			InputPerMTok:  5000000,
			OutputPerMTok: 15000000,
		},
		{
			Model:         "claude-3-5-sonnet",
			Provider:      "anthropic",
			InputPerMTok:  3000000,
			OutputPerMTok: 15000000,
		},
		{
			Model:         "gemini-1.5-pro",
			Provider:      "gemini",
			InputPerMTok:  3500000,
			OutputPerMTok: 10500000,
		},
	}
	if err := s.controlStore.UpsertPricing(ctx, prices); err != nil {
		s.logger.Warn("could not seed dev pricing", slog.Any("error", err))
	}

	if s.cfg.SeedDemo {
		s.logger.Info("seeding 90 days of demo usage data")
		if _, err := usageseed.Demo(ctx, s.usageStore, "", time.Now()); err != nil {
			return fmt.Errorf("failed to seed demo usage: %w", err)
		}
	}

	if s.cfg.ImportClaudeCode {
		s.logger.Info("importing local Claude Code usage logs")
		imp := usageclaude.New(s.usageStore, "", s.logger)
		if imp.IsEnabled() {
			if _, err := imp.Import(ctx); err != nil {
				s.logger.Warn("claude code import warning", slog.Any("error", err))
			}
		}
	}

	return nil
}

func (s *devServer) startBudget(ctx context.Context) error {
	sw := budgetsweeper.New(s.budgetStore, budgetsweeper.WithInterval(5*time.Second), budgetsweeper.WithLogger(s.logger))
	go func() {
		_ = sw.Run(ctx)
	}()

	relay := budgetoutbox.NewRelay(s.budgetStore, s.js, budgetoutbox.WithRelayLogger(s.logger))
	go func() {
		_ = relay.Run(ctx)
	}()

	consumer, err := budgetoutbox.NewConfigConsumer(s.budgetStore, s.js, budgetoutbox.WithConsumerLogger(s.logger))
	if err == nil {
		_, _ = consumer.Start(ctx)
	}

	checker := health.New()
	checker.RegisterCheck("budget_db", s.budgetDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "budget",
		HTTPPort:    s.cfg.BudgetPort,
		GRPCPort:    s.cfg.BudgetGRPCPort,
		LogLevel:    s.cfg.LogLevel,
	}

	srvOpts := service.Options{
		LogWriter: s.logWriter,
		Checker:   checker,
		RegisterGRPC: func(srv *grpc.Server) {
			budgetv1.RegisterBudgetServiceServer(srv, budgetgrpc.New(s.budgetStore))
		},
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.HandleFunc("POST /admin/limits", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			})
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func (s *devServer) startControl(ctx context.Context) error {
	relay := controloutbox.NewRelay(s.controlStore, s.js, controloutbox.WithRelayLogger(s.logger))
	go func() {
		_ = relay.Run(ctx)
	}()

	checker := health.New()
	checker.RegisterCheck("control_db", s.controlDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "control",
		HTTPPort:    s.cfg.ControlPort,
		GRPCPort:    s.cfg.ControlGRPCPort,
		LogLevel:    s.cfg.LogLevel,
	}

	adminHandler := controlhttp.NewHandler(s.controlStore, controlhttp.Config{
		AdminToken: s.cfg.AdminToken,
		KeyPepper:  s.cfg.KeyPepper,
	})

	srvOpts := service.Options{
		LogWriter: s.logWriter,
		Checker:   checker,
		RegisterGRPC: func(srv *grpc.Server) {
			controlv1.RegisterControlServiceServer(srv, controlgrpc.New(s.controlStore))
		},
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.Handle("/admin/", adminHandler)
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func (s *devServer) startUsage(ctx context.Context) error {
	uc := usageconsumer.New(s.js, s.usageStore, s.logger)
	if err := uc.Start(ctx); err != nil {
		s.logger.Warn("usage consumer failed to start", slog.Any("error", err))
	}

	checker := health.New()
	checker.RegisterCheck("usage_db", s.usageDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "usage",
		HTTPPort:    s.cfg.UsagePort,
		LogLevel:    s.cfg.LogLevel,
	}

	claudeImporter := usageclaude.New(s.usageStore, "", s.logger)
	orgImporter := usageorg.New(s.usageStore, usageorg.Config{}, s.logger)
	repEngine := usagereport.NewEngine(s.usageStore)
	apiHandler := usagehttp.NewHandler(s.usageStore, repEngine, claudeImporter, orgImporter, usagehttp.Config{
		AdminToken: s.cfg.AdminToken,
	})

	srvOpts := service.Options{
		LogWriter: s.logWriter,
		Checker:   checker,
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.Handle("/admin/", apiHandler)
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func (s *devServer) buildGatewayCoreService(ctx context.Context) (*gatewaycore.Service, *gatewayauth.Manager, error) {
	connCtrl, err := grpcx.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", s.cfg.ControlGRPCPort))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway dial control: %w", err)
	}

	authMgr, err := gatewayauth.NewManager(s.cfg.KeyPepper, controlv1.NewControlServiceClient(connCtrl), gatewayauth.WithLogger(s.logger))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway auth manager: %w", err)
	}
	_ = authMgr.LoadSnapshot(ctx)

	connBdg, err := grpcx.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", s.cfg.BudgetGRPCPort))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway dial budget: %w", err)
	}
	budgetCli := gatewaybudget.New(budgetv1.NewBudgetServiceClient(connBdg), gatewaybudget.WithLogger(s.logger))

	pub := gatewayevents.NewPublisher(s.js, gatewayevents.WithLogger(s.logger))
	r := gatewayrouter.New()

	if s.cfg.MockProvider {
		r.Register("openai", fake.New(fake.WithName("openai"), fake.WithContent("Fake OpenAI response from dev server")))
		r.Register("anthropic", fake.New(fake.WithName("anthropic"), fake.WithContent("Fake Anthropic response from dev server")))
		r.Register("gemini", fake.New(fake.WithName("gemini"), fake.WithContent("Fake Gemini response from dev server")))
	}

	localCache := gatewaycache.New(gatewaycache.WithCapacity(10000), gatewaycache.WithTTL(5*time.Minute))
	svc := gatewaycore.NewService(gatewaycore.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   budgetCli,
		EventPublisher: pub,
		LocalCache:     localCache,
		Router:         r,
		Logger:         s.logger,
	})

	return svc, authMgr, nil
}

func (s *devServer) startGatewayInstance(ctx context.Context, port int, name string) error {
	svc, authMgr, err := s.buildGatewayCoreService(ctx)
	if err != nil {
		return err
	}

	checker := health.New()
	checker.RegisterCheck("snapshot", func(_ context.Context) error {
		if authMgr == nil || !authMgr.IsLoaded() {
			return errors.New("auth snapshot not loaded")
		}
		return nil
	})

	otelProv, err := otel.Init(ctx, otel.Config{
		ServiceName: name,
	})
	if err != nil {
		return fmt.Errorf("gateway otel init: %w", err)
	}
	defer func() { _ = otelProv.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProv.MetricsHandler())

	apiHandler := gatewayapi.NewHandler(svc, authMgr)
	apiHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		s.logger.Info("starting gateway instance", slog.String("name", name), slog.Int("port", port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	server.SetKeepAlivesEnabled(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func (s *devServer) startEdge(ctx context.Context) error {
	gwURLs := []string{fmt.Sprintf("http://127.0.0.1:%d", s.cfg.GatewayPort)}
	if s.cfg.Gateway2Port > 0 {
		gwURLs = append(gwURLs, fmt.Sprintf("http://127.0.0.1:%d", s.cfg.Gateway2Port))
	}

	proxyHandler, err := edgeproxy.New(edgeproxy.Config{
		GatewayURLs:  gwURLs,
		ControlURL:   fmt.Sprintf("http://127.0.0.1:%d", s.cfg.ControlPort),
		UsageURL:     fmt.Sprintf("http://127.0.0.1:%d", s.cfg.UsagePort),
		BudgetURL:    fmt.Sprintf("http://127.0.0.1:%d", s.cfg.BudgetPort),
		WebDir:       s.cfg.WebDir,
		MaxBodyBytes: 10 * 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("edge proxy init: %w", err)
	}

	checker := health.New()
	otelProv, err := otel.Init(ctx, otel.Config{ServiceName: "edge"})
	if err != nil {
		return fmt.Errorf("edge otel init: %w", err)
	}
	defer func() { _ = otelProv.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", checker.LivezHandler())
	mux.HandleFunc("/readyz", checker.ReadyzHandler())
	mux.Handle("/metrics", otelProv.MetricsHandler())
	mux.Handle("/", proxyHandler)

	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", s.cfg.EdgePort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		s.logger.Info("starting edge proxy", slog.Int("port", s.cfg.EdgePort))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
	}

	server.SetKeepAlivesEnabled(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func (s *devServer) printBanner() {
	fmt.Fprintf(s.logWriter, "\n"+
		"=================================================================\n"+
		"  LLM Gateway Local Dev Server (lgw-dev)\n"+
		"=================================================================\n"+
		"  Edge Entrypoint:  http://127.0.0.1:%d\n"+
		"  Gateway 1:        http://127.0.0.1:%d\n"+
		"  Gateway 2:        http://127.0.0.1:%d\n"+
		"  Control API:      http://127.0.0.1:%d\n"+
		"  Usage API:        http://127.0.0.1:%d\n"+
		"  Budget API:       http://127.0.0.1:%d\n"+
		"  Health Status:    http://127.0.0.1:%d/health\n"+
		"  Dashboard SPA:    http://127.0.0.1:%d/dashboard\n"+
		"  Dev Key:          lgw_0123456789abcdef0123456789abcdef\n"+
		"  Admin Token:      %s\n"+
		"=================================================================\n\n",
		s.cfg.EdgePort, s.cfg.GatewayPort, s.cfg.Gateway2Port,
		s.cfg.ControlPort, s.cfg.UsagePort, s.cfg.BudgetPort,
		s.cfg.EdgePort, s.cfg.EdgePort, s.cfg.AdminToken,
	)
}

func (s *devServer) closeResources() {
	if s.budgetDB != nil {
		_ = s.budgetDB.Close()
	}
	if s.controlDB != nil {
		_ = s.controlDB.Close()
	}
	if s.usageDB != nil {
		_ = s.usageDB.Close()
	}
	if s.natsConn != nil {
		s.natsConn.Close()
	}
	if s.natsSrv != nil {
		s.natsSrv.Shutdown()
		s.natsSrv.WaitForShutdown()
	}
	if s.tempDir != "" {
		_ = os.RemoveAll(s.tempDir)
	}
}

type DevServerOption func(*devServer)

func WithLogWriter(w io.Writer) DevServerOption {
	return func(s *devServer) {
		s.logWriter = w
	}
}

func (s *devServer) prepareDevServer(ctx context.Context) error {
	var err error
	if s.tempDir == "" {
		s.tempDir, err = os.MkdirTemp("", "lgw-dev-*")
		if err != nil {
			return fmt.Errorf("failed to create temp dir: %w", err)
		}
	}
	if err := s.initNATS(); err != nil {
		return err
	}
	if err := s.initDatabases(ctx); err != nil {
		return err
	}
	return s.seedData(ctx)
}

func (s *devServer) launchServices(ctx context.Context, errCh chan<- error) *sync.WaitGroup {
	var wg sync.WaitGroup
	startGoroutine := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("service terminated with error", slog.String("name", name), slog.Any("error", err))
				errCh <- fmt.Errorf("%s service error: %w", name, err)
			}
		}()
	}

	startGoroutine("budget", s.startBudget)
	startGoroutine("control", s.startControl)
	startGoroutine("usage", s.startUsage)

	time.Sleep(100 * time.Millisecond)

	startGoroutine("gateway-1", func(c context.Context) error {
		return s.startGatewayInstance(c, s.cfg.GatewayPort, "gateway-1")
	})
	if s.cfg.Gateway2Port > 0 {
		startGoroutine("gateway-2", func(c context.Context) error {
			return s.startGatewayInstance(c, s.cfg.Gateway2Port, "gateway-2")
		})
	}

	time.Sleep(100 * time.Millisecond)

	startGoroutine("edge", s.startEdge)
	return &wg
}

func runDevServer(ctx context.Context, cfg DevConfig, opts ...DevServerOption) error {
	server := &devServer{
		cfg:       cfg,
		logWriter: io.Writer(os.Stdout),
		tempDir:   cfg.TempDir,
	}
	for _, opt := range opts {
		opt(server)
	}

	server.logger = logging.New(server.logWriter, cfg.LogLevel)
	defer server.closeResources()

	if err := server.prepareDevServer(ctx); err != nil {
		return err
	}

	errCh := make(chan error, 6)
	wg := server.launchServices(ctx, errCh)

	server.printBanner()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	wg.Wait()
	server.logger.Info("all lgw-dev services shut down successfully")
	return nil
}

func parseFlags(args []string) (DevConfig, error) {
	cfg := defaultDevConfig()
	fs := flag.NewFlagSet("lgw-dev", flag.ContinueOnError)

	fs.BoolVar(&cfg.MockProvider, "mock-provider", true, "Initialize gateway using fake providers without consuming live APIs")
	fs.BoolVar(&cfg.SeedDemo, "seed-demo", false, "Seed deterministic demo records covering 90 days")
	fs.BoolVar(&cfg.ImportClaudeCode, "import-claude-code", false, "Automatically import local Claude Code usage logs on startup")
	fs.StringVar(&cfg.WebDir, "web-dir", "", "Static assets directory to serve at /dashboard")
	fs.IntVar(&cfg.EdgePort, "edge-port", 8787, "Edge proxy HTTP port")
	fs.IntVar(&cfg.GatewayPort, "gw-port", 8788, "Gateway instance 1 HTTP port")
	fs.IntVar(&cfg.Gateway2Port, "gw2-port", 8789, "Gateway instance 2 HTTP port (0 to disable)")
	fs.StringVar(&cfg.AdminToken, "admin-token", "dev-admin-token", "Admin API token")
	fs.StringVar(&cfg.LogLevel, "log-level", "info", "Logging level (debug, info, warn, error)")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func realMain(args []string) int {
	cfg, err := parseFlags(args)
	if err != nil {
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runDevServer(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "lgw-dev error: %v\n", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(realMain(os.Args[1:]))
}
