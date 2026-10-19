package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Leonardoongaratto/llm-gateway/internal/devserver"
)

func parseFlags(args []string) (devserver.Config, error) {
	cfg := devserver.DefaultConfig()
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

func run(ctx context.Context, args []string) int {
	cfg, err := parseFlags(args)
	if err != nil {
		return 1
	}

	if err := devserver.Run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "lgw-dev error: %v\n", err)
		return 1
	}
	return 0
}

func realMain(args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, args)
}

func main() {
	os.Exit(realMain(os.Args[1:]))
}
