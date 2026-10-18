package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/config"
)

type sampleConfig struct {
	ServiceName string `env:"SERVICE_NAME,required"`
	DatabaseDSN string `env:"DB_DSN,required"`
	HTTPPort    int    `env:"HTTP_PORT" default:"8787"`
	Debug       bool   `env:"DEBUG" default:"false"`
}

// AC-1A-12: Given uma variável obrigatória ausente, when o serviço inicia,
// then sai com código ≠ 0 e uma mensagem listando todas as variáveis faltantes.
func TestConfig_MissingRequiredVariables_AC1A12(t *testing.T) {
	os.Unsetenv("LGW_SERVICE_NAME")
	os.Unsetenv("LGW_DB_DSN")

	_, err := config.LoadFromEnv[sampleConfig]("LGW_")
	if err == nil {
		t.Fatal("expected error when required environment variables are missing")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "LGW_SERVICE_NAME") {
		t.Errorf("expected error message to contain LGW_SERVICE_NAME, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "LGW_DB_DSN") {
		t.Errorf("expected error message to contain LGW_DB_DSN, got: %s", errMsg)
	}
}

func TestConfig_LoadSuccessWithDefaults(t *testing.T) {
	t.Setenv("LGW_SERVICE_NAME", "edge")
	t.Setenv("LGW_DB_DSN", "file:edge.db")

	cfg, err := config.LoadFromEnv[sampleConfig]("LGW_")
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.ServiceName != "edge" {
		t.Errorf("expected ServiceName=edge, got %s", cfg.ServiceName)
	}
	if cfg.DatabaseDSN != "file:edge.db" {
		t.Errorf("expected DatabaseDSN=file:edge.db, got %s", cfg.DatabaseDSN)
	}
	if cfg.HTTPPort != 8787 {
		t.Errorf("expected default HTTPPort=8787, got %d", cfg.HTTPPort)
	}
	if cfg.Debug != false {
		t.Errorf("expected default Debug=false, got %v", cfg.Debug)
	}
}

func TestConfig_OverridesDefaults(t *testing.T) {
	t.Setenv("LGW_SERVICE_NAME", "gateway")
	t.Setenv("LGW_DB_DSN", "postgres://localhost/test")
	t.Setenv("LGW_HTTP_PORT", "9999")
	t.Setenv("LGW_DEBUG", "true")

	cfg, err := config.LoadFromEnv[sampleConfig]("LGW_")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.HTTPPort != 9999 {
		t.Errorf("expected HTTPPort=9999, got %d", cfg.HTTPPort)
	}
	if cfg.Debug != true {
		t.Errorf("expected Debug=true, got %v", cfg.Debug)
	}
}
