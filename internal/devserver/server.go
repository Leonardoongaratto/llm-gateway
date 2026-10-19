package devserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
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

type Config struct {
	EdgePort         int
	GatewayPort      int
	Gateway2Port     int
	ControlPort      int
	UsagePort        int
	BudgetPort       int
	BudgetGRPCPort   int
	ControlGRPCPort  int
	MockProvider     bool
	Providers        map[string]provider.Provider
	SeedDemo         bool
	ImportClaudeCode bool
	WebDir           string
	AdminToken       string
	KeyPepper        string
	LogLevel         string
	TempDir          string
}

func DefaultConfig() Config {
	return Config{
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

type Cluster struct {
	cfg          Config
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
	authMgrs     []*gatewayauth.Manager

	mu         sync.Mutex
	cancelBdg  context.CancelFunc
	cancelUsg  context.CancelFunc
	cancelGw1  context.CancelFunc
	cancelGw2  context.CancelFunc
	cancelEdge context.CancelFunc
	cancelCtrl context.CancelFunc

	wg sync.WaitGroup
}

type Option func(*Cluster)

func WithLogWriter(w io.Writer) Option {
	return func(c *Cluster) {
		c.logWriter = w
	}
}

func NewCluster(cfg Config, opts ...Option) (*Cluster, error) {
	c := &Cluster{
		cfg:       cfg,
		logWriter: io.Writer(os.Stdout),
		tempDir:   cfg.TempDir,
	}
	for _, opt := range opts {
		opt(c)
	}

	c.logger = logging.New(c.logWriter, cfg.LogLevel)

	var err error
	if c.tempDir == "" {
		c.tempDir, err = os.MkdirTemp("", "lgw-dev-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
	}

	if err := c.initNATS(); err != nil {
		c.Close()
		return nil, err
	}
	if err := c.initDatabases(context.Background()); err != nil {
		c.Close()
		return nil, err
	}
	if err := c.seedData(context.Background()); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Cluster) EdgeURL() string     { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.EdgePort) }
func (c *Cluster) Gateway1URL() string { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.GatewayPort) }
func (c *Cluster) Gateway2URL() string {
	if c.cfg.Gateway2Port <= 0 {
		return ""
	}
	return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.Gateway2Port)
}
func (c *Cluster) ControlURL() string              { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.ControlPort) }
func (c *Cluster) UsageURL() string                { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.UsagePort) }
func (c *Cluster) BudgetURL() string               { return fmt.Sprintf("http://127.0.0.1:%d", c.cfg.BudgetPort) }
func (c *Cluster) BudgetStore() *budgetstore.Store { return c.budgetStore }
func (c *Cluster) ControlStore() *controlstore.Store {
	return c.controlStore
}
func (c *Cluster) UsageStore() *usagestore.Store { return c.usageStore }
func (c *Cluster) NATSConn() *nats.Conn          { return c.natsConn }
func (c *Cluster) JS() jetstream.JetStream       { return c.js }

func (c *Cluster) ReloadSnapshots(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.authMgrs {
		if err := m.LoadSnapshot(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) initNATS() error {
	natsStore := filepath.Join(c.tempDir, "nats")
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  natsStore,
		NoLog:     true,
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return fmt.Errorf("embedded nats server: %w", err)
	}
	c.natsSrv = srv

	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		return errors.New("embedded nats failed to become ready")
	}

	nc, js, err := natsx.Connect(srv.ClientURL())
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	c.natsConn = nc
	c.js = js

	if err := natsx.InitStreams(context.Background(), js); err != nil {
		return fmt.Errorf("nats init streams: %w", err)
	}
	return nil
}

func (c *Cluster) initDatabases(ctx context.Context) error {
	bDB, err := sqlx.Open("sqlite", filepath.Join(c.tempDir, "budget.db"))
	if err != nil {
		return fmt.Errorf("budget db open: %w", err)
	}
	c.budgetDB = bDB
	if err := budgetstore.Migrate(ctx, bDB); err != nil {
		return fmt.Errorf("budget db migrate: %w", err)
	}
	c.budgetStore = budgetstore.New(bDB)

	ctrlDB, err := sqlx.Open("sqlite", filepath.Join(c.tempDir, "control.db"))
	if err != nil {
		return fmt.Errorf("control db open: %w", err)
	}
	c.controlDB = ctrlDB
	if err := controlstore.Migrate(ctx, ctrlDB); err != nil {
		return fmt.Errorf("control db migrate: %w", err)
	}
	c.controlStore = controlstore.New(ctrlDB)

	uDB, err := sqlx.Open("sqlite", filepath.Join(c.tempDir, "usage.db"))
	if err != nil {
		return fmt.Errorf("usage db open: %w", err)
	}
	c.usageDB = uDB
	if err := usagestore.Migrate(ctx, uDB); err != nil {
		return fmt.Errorf("usage db migrate: %w", err)
	}
	c.usageStore = usagestore.New(uDB)

	return nil
}

func (c *Cluster) seedData(ctx context.Context) error {
	devKey := "lgw_0123456789abcdef0123456789abcdef"
	keyHash, err := controlcore.HashKey(devKey, c.cfg.KeyPepper)
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
	if _, err := c.controlStore.CreateKey(ctx, vk); err != nil {
		c.logger.Warn("could not create dev virtual key", slog.Any("error", err))
	}

	limit := budgetcore.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   1000000000,
		MonthlyLimitMicros: 5000000000,
		RPM:                10000,
	}
	if err := c.budgetStore.SetKeyLimits(ctx, limit); err != nil {
		c.logger.Warn("could not upsert dev limit", slog.Any("error", err))
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
	if err := c.controlStore.UpsertPricing(ctx, prices); err != nil {
		c.logger.Warn("could not seed dev pricing", slog.Any("error", err))
	}

	if c.cfg.SeedDemo {
		c.logger.Info("seeding 90 days of demo usage data")
		if _, err := usageseed.Demo(ctx, c.usageStore, "", time.Now()); err != nil {
			return fmt.Errorf("failed to seed demo usage: %w", err)
		}
	}

	if c.cfg.ImportClaudeCode {
		c.logger.Info("importing local Claude Code usage logs")
		imp := usageclaude.New(c.usageStore, "", c.logger)
		if imp.IsEnabled() {
			if _, err := imp.Import(ctx); err != nil {
				c.logger.Warn("claude code import warning", slog.Any("error", err))
			}
		}
	}

	return nil
}

func (c *Cluster) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	bCtx, bCancel := context.WithCancel(ctx)
	c.cancelBdg = bCancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_ = c.startBudget(bCtx)
	}()

	ctrlCtx, ctrlCancel := context.WithCancel(ctx)
	c.cancelCtrl = ctrlCancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_ = c.startControl(ctrlCtx)
	}()

	uCtx, uCancel := context.WithCancel(ctx)
	c.cancelUsg = uCancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_ = c.startUsage(uCtx)
	}()

	time.Sleep(100 * time.Millisecond)

	gw1Ctx, gw1Cancel := context.WithCancel(ctx)
	c.cancelGw1 = gw1Cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_ = c.startGatewayInstance(gw1Ctx, c.cfg.GatewayPort, "gateway-1")
	}()

	if c.cfg.Gateway2Port > 0 {
		gw2Ctx, gw2Cancel := context.WithCancel(ctx)
		c.cancelGw2 = gw2Cancel
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			_ = c.startGatewayInstance(gw2Ctx, c.cfg.Gateway2Port, "gateway-2")
		}()
	}

	time.Sleep(100 * time.Millisecond)

	edgeCtx, edgeCancel := context.WithCancel(ctx)
	c.cancelEdge = edgeCancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		_ = c.startEdge(edgeCtx)
	}()

	return nil
}

func (c *Cluster) StopBudget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancelBdg != nil {
		c.cancelBdg()
		c.cancelBdg = nil
	}
}

func (c *Cluster) StartBudget(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancelBdg == nil {
		bCtx, bCancel := context.WithCancel(ctx)
		c.cancelBdg = bCancel
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			_ = c.startBudget(bCtx)
		}()
	}
}

func (c *Cluster) StopUsage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancelUsg != nil {
		c.cancelUsg()
		c.cancelUsg = nil
	}
}

func (c *Cluster) StartUsage(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancelUsg == nil {
		uCtx, uCancel := context.WithCancel(ctx)
		c.cancelUsg = uCancel
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			_ = c.startUsage(uCtx)
		}()
	}
}

func (c *Cluster) startBudget(ctx context.Context) error {
	sw := budgetsweeper.New(c.budgetStore, budgetsweeper.WithInterval(5*time.Second), budgetsweeper.WithLogger(c.logger))
	go func() { _ = sw.Run(ctx) }()

	relay := budgetoutbox.NewRelay(c.budgetStore, c.js, budgetoutbox.WithRelayLogger(c.logger))
	go func() { _ = relay.Run(ctx) }()

	consumer, err := budgetoutbox.NewConfigConsumer(c.budgetStore, c.js, budgetoutbox.WithConsumerLogger(c.logger))
	if err == nil {
		_, _ = consumer.Start(ctx)
	}

	checker := health.New()
	checker.RegisterCheck("budget_db", c.budgetDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "budget",
		HTTPPort:    c.cfg.BudgetPort,
		GRPCPort:    c.cfg.BudgetGRPCPort,
		LogLevel:    c.cfg.LogLevel,
	}

	srvOpts := service.Options{
		LogWriter: c.logWriter,
		Checker:   checker,
		RegisterGRPC: func(srv *grpc.Server) {
			budgetv1.RegisterBudgetServiceServer(srv, budgetgrpc.New(c.budgetStore))
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

func (c *Cluster) startControl(ctx context.Context) error {
	relay := controloutbox.NewRelay(c.controlStore, c.js, controloutbox.WithRelayLogger(c.logger))
	go func() { _ = relay.Run(ctx) }()

	checker := health.New()
	checker.RegisterCheck("control_db", c.controlDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "control",
		HTTPPort:    c.cfg.ControlPort,
		GRPCPort:    c.cfg.ControlGRPCPort,
		LogLevel:    c.cfg.LogLevel,
	}

	adminHandler := controlhttp.NewHandler(c.controlStore, controlhttp.Config{
		AdminToken: c.cfg.AdminToken,
		KeyPepper:  c.cfg.KeyPepper,
	})

	srvOpts := service.Options{
		LogWriter: c.logWriter,
		Checker:   checker,
		RegisterGRPC: func(srv *grpc.Server) {
			controlv1.RegisterControlServiceServer(srv, controlgrpc.New(c.controlStore))
		},
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.Handle("/admin/", adminHandler)
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func (c *Cluster) startUsage(ctx context.Context) error {
	uc := usageconsumer.New(c.js, c.usageStore, c.logger)
	if err := uc.Start(ctx); err != nil {
		c.logger.Warn("usage consumer failed to start", slog.Any("error", err))
	}
	defer uc.Stop()
	go func() {
		<-ctx.Done()
		uc.Stop()
	}()

	checker := health.New()
	checker.RegisterCheck("usage_db", c.usageDB.PingContext)

	srvCfg := service.Config{
		ServiceName: "usage",
		HTTPPort:    c.cfg.UsagePort,
		LogLevel:    c.cfg.LogLevel,
	}

	claudeImporter := usageclaude.New(c.usageStore, "", c.logger)
	orgImporter := usageorg.New(c.usageStore, usageorg.Config{}, c.logger)
	repEngine := usagereport.NewEngine(c.usageStore)
	apiHandler := usagehttp.NewHandler(c.usageStore, repEngine, claudeImporter, orgImporter, usagehttp.Config{
		AdminToken: c.cfg.AdminToken,
	})

	srvOpts := service.Options{
		LogWriter: c.logWriter,
		Checker:   checker,
		RegisterHTTP: func(mux *http.ServeMux) {
			mux.Handle("/admin/", apiHandler)
		},
	}

	return service.Run(ctx, srvCfg, srvOpts)
}

func (c *Cluster) buildGatewayCoreService(ctx context.Context) (*gatewaycore.Service, *gatewayauth.Manager, error) {
	connCtrl, err := grpcx.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", c.cfg.ControlGRPCPort))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway dial control: %w", err)
	}

	authMgr, err := gatewayauth.NewManager(c.cfg.KeyPepper, controlv1.NewControlServiceClient(connCtrl), gatewayauth.WithLogger(c.logger))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway auth manager: %w", err)
	}
	_ = authMgr.LoadSnapshot(ctx)
	if _, err := authMgr.StartConfigWatcher(ctx, c.js); err != nil {
		c.logger.Warn("gateway auth config watcher failed to start", slog.Any("error", err))
	}

	c.mu.Lock()
	c.authMgrs = append(c.authMgrs, authMgr)
	c.mu.Unlock()

	connBdg, err := grpcx.Dial(ctx, fmt.Sprintf("127.0.0.1:%d", c.cfg.BudgetGRPCPort))
	if err != nil {
		return nil, nil, fmt.Errorf("gateway dial budget: %w", err)
	}
	budgetCli := gatewaybudget.New(budgetv1.NewBudgetServiceClient(connBdg), gatewaybudget.WithLogger(c.logger))

	pub := gatewayevents.NewPublisher(c.js, gatewayevents.WithLogger(c.logger))
	r := gatewayrouter.New()

	if c.cfg.MockProvider {
		r.Register("openai", fake.New(fake.WithName("openai"), fake.WithContent("Fake OpenAI response from dev server")))
		r.Register("anthropic", fake.New(fake.WithName("anthropic"), fake.WithContent("Fake Anthropic response from dev server")))
		r.Register("gemini", fake.New(fake.WithName("gemini"), fake.WithContent("Fake Gemini response from dev server")))
	}
	for name, prov := range c.cfg.Providers {
		r.Register(name, prov)
	}

	localCache := gatewaycache.New(gatewaycache.WithCapacity(10000), gatewaycache.WithTTL(5*time.Minute))
	svc := gatewaycore.NewService(gatewaycore.ServiceConfig{
		AuthManager:    authMgr,
		BudgetClient:   budgetCli,
		EventPublisher: pub,
		LocalCache:     localCache,
		Router:         r,
		Logger:         c.logger,
	})

	return svc, authMgr, nil
}

func (c *Cluster) startGatewayInstance(ctx context.Context, port int, name string) error {
	svc, authMgr, err := c.buildGatewayCoreService(ctx)
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

	otelProv, err := otel.Init(ctx, otel.Config{ServiceName: name})
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
		Handler:           otel.HTTPHandler(mux, name),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		c.logger.Info("starting gateway instance", slog.String("name", name), slog.Int("port", port))
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

func (c *Cluster) startEdge(ctx context.Context) error {
	gwURLs := []string{fmt.Sprintf("http://127.0.0.1:%d", c.cfg.GatewayPort)}
	if c.cfg.Gateway2Port > 0 {
		gwURLs = append(gwURLs, fmt.Sprintf("http://127.0.0.1:%d", c.cfg.Gateway2Port))
	}

	proxyHandler, err := edgeproxy.New(edgeproxy.Config{
		GatewayURLs:  gwURLs,
		ControlURL:   fmt.Sprintf("http://127.0.0.1:%d", c.cfg.ControlPort),
		UsageURL:     fmt.Sprintf("http://127.0.0.1:%d", c.cfg.UsagePort),
		BudgetURL:    fmt.Sprintf("http://127.0.0.1:%d", c.cfg.BudgetPort),
		WebDir:       c.cfg.WebDir,
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
		Addr:              fmt.Sprintf("127.0.0.1:%d", c.cfg.EdgePort),
		Handler:           otel.HTTPHandler(mux, "edge"),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		c.logger.Info("starting edge proxy", slog.Int("port", c.cfg.EdgePort))
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

func (c *Cluster) PrintBanner() {
	fmt.Fprintf(c.logWriter, "\n"+
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
		c.cfg.EdgePort, c.cfg.GatewayPort, c.cfg.Gateway2Port,
		c.cfg.ControlPort, c.cfg.UsagePort, c.cfg.BudgetPort,
		c.cfg.EdgePort, c.cfg.EdgePort, c.cfg.AdminToken,
	)
}

func (c *Cluster) Close() {
	c.mu.Lock()
	if c.cancelEdge != nil {
		c.cancelEdge()
	}
	if c.cancelGw1 != nil {
		c.cancelGw1()
	}
	if c.cancelGw2 != nil {
		c.cancelGw2()
	}
	if c.cancelUsg != nil {
		c.cancelUsg()
	}
	if c.cancelCtrl != nil {
		c.cancelCtrl()
	}
	if c.cancelBdg != nil {
		c.cancelBdg()
	}
	c.mu.Unlock()

	c.wg.Wait()

	if c.budgetDB != nil {
		_ = c.budgetDB.Close()
	}
	if c.controlDB != nil {
		_ = c.controlDB.Close()
	}
	if c.usageDB != nil {
		_ = c.usageDB.Close()
	}
	if c.natsConn != nil {
		c.natsConn.Close()
	}
	if c.natsSrv != nil {
		c.natsSrv.Shutdown()
		c.natsSrv.WaitForShutdown()
	}
	if c.tempDir != "" && c.cfg.TempDir == "" {
		_ = os.RemoveAll(c.tempDir)
	}
}

func Run(ctx context.Context, cfg Config, opts ...Option) error {
	cluster, err := NewCluster(cfg, opts...)
	if err != nil {
		return err
	}
	defer cluster.Close()

	if err := cluster.Start(ctx); err != nil {
		return err
	}

	cluster.PrintBanner()

	<-ctx.Done()
	return nil
}
