package main

import (
	"context"
	"testing"
)

func TestParseFlags_CustomAndInvalid(t *testing.T) {
	cfg, err := parseFlags([]string{
		"--edge-port=9090",
		"--mock-provider=false",
		"--seed-demo=true",
		"--admin-token=custom-token",
		"--web-dir=/tmp/custom-web",
		"--import-claude-code=true",
		"--log-level=debug",
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if cfg.EdgePort != 9090 {
		t.Errorf("expected EdgePort 9090, got %d", cfg.EdgePort)
	}
	if cfg.MockProvider {
		t.Errorf("expected MockProvider to be false")
	}
	if !cfg.SeedDemo {
		t.Errorf("expected SeedDemo to be true")
	}
	if cfg.AdminToken != "custom-token" {
		t.Errorf("expected custom-token, got %s", cfg.AdminToken)
	}
	if cfg.WebDir != "/tmp/custom-web" {
		t.Errorf("expected /tmp/custom-web, got %s", cfg.WebDir)
	}
	if !cfg.ImportClaudeCode {
		t.Errorf("expected ImportClaudeCode to be true")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}

	_, errInvalid := parseFlags([]string{"--unsupported-flag"})
	if errInvalid == nil {
		t.Errorf("expected error for unsupported flag, got nil")
	}
}

func TestRealMain_InvalidFlagReturnsOne(t *testing.T) {
	code := realMain([]string{"--bad-flag"})
	if code != 1 {
		t.Errorf("expected exit code 1 for invalid flag, got %d", code)
	}
}

func TestRun_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	code := run(ctx, []string{
		"--edge-port=0",
		"--gw-port=0",
		"--gw2-port=0",
	})
	if code != 0 {
		t.Errorf("expected exit code 0 for cancelled context, got %d", code)
	}
}
