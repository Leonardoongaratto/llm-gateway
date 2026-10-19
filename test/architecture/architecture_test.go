package architecture

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

type pkgInfo struct {
	ImportPath   string   `json:"ImportPath"`
	Imports      []string `json:"Imports"`
	TestImports  []string `json:"TestImports"`
	XTestImports []string `json:"XTestImports"`
}

func loadPackages(t *testing.T, pattern string) []pkgInfo {
	t.Helper()
	cmd := exec.Command("go", "list", "-json", pattern)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to run go list -json %s: %v\nOutput: %s", pattern, err, string(out))
	}

	var packages []pkgInfo
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		var p pkgInfo
		if err := dec.Decode(&p); err != nil {
			t.Fatalf("failed to decode package info: %v", err)
		}
		packages = append(packages, p)
	}
	return packages
}

// TestArchitecture_KernelPurity verifies that internal/kernel never imports transport or infrastructure.
func TestArchitecture_KernelPurity(t *testing.T) {
	packages := loadPackages(t, "./internal/kernel/...")

	forbiddenSubstrings := []string{
		"net/http",
		"google.golang.org/grpc",
		"database/sql",
		"github.com/nats-io/nats.go",
		"/internal/platform",
		"/internal/budget",
		"/internal/control",
		"/internal/gateway",
		"/internal/usage",
		"/internal/edge",
	}

	for _, p := range packages {
		for _, imp := range p.Imports {
			for _, forbidden := range forbiddenSubstrings {
				if strings.Contains(imp, forbidden) {
					t.Errorf("kernel package %s must not import transport/infra %s", p.ImportPath, imp)
				}
			}
		}
	}
}

func assertNoImports(t *testing.T, svc, otherSvc string, p pkgInfo) {
	t.Helper()
	forbiddenPrefix := "github.com/Leonardoongaratto/llm-gateway/internal/" + otherSvc
	for _, imp := range p.Imports {
		if strings.HasPrefix(imp, forbiddenPrefix) {
			t.Errorf("microservice %s (%s) illegally imports %s from service %s",
				svc, p.ImportPath, imp, otherSvc)
		}
	}
}

func verifyServiceIsolation(t *testing.T, svc string, allServices []string) {
	t.Helper()
	packages := loadPackages(t, "./internal/"+svc+"/...")
	for _, otherSvc := range allServices {
		if otherSvc == svc {
			continue
		}
		for _, p := range packages {
			assertNoImports(t, svc, otherSvc, p)
		}
	}
}

// TestArchitecture_MicroserviceIsolation verifies that no microservice imports code from another microservice.
func TestArchitecture_MicroserviceIsolation(t *testing.T) {
	services := []string{"budget", "control", "gateway", "usage", "edge"}
	for _, svc := range services {
		verifyServiceIsolation(t, svc, services)
	}
}

// TestArchitecture_CoreDomainPurity verifies that internal/*/core never imports database/sql or transport.
func TestArchitecture_CoreDomainPurity(t *testing.T) {
	packages := loadPackages(t, "./internal/...")

	forbiddenTransport := []string{
		"database/sql",
		"net/http",
		"google.golang.org/grpc",
		"github.com/nats-io/nats.go",
		"/internal/platform/sqlx",
		"/internal/platform/grpcx",
		"/internal/platform/natsx",
	}

	for _, p := range packages {
		if !strings.HasSuffix(p.ImportPath, "/core") {
			continue
		}
		for _, imp := range p.Imports {
			for _, forbidden := range forbiddenTransport {
				if strings.Contains(imp, forbidden) {
					t.Errorf("core domain %s must not import transport/db %s", p.ImportPath, imp)
				}
			}
		}
	}
}
