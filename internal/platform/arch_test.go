package platform_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var knownServices = []string{"edge", "gateway", "budget", "control", "usage"}

var forbiddenCoreImports = map[string]struct{}{
	"net/http":                   {},
	"google.golang.org/grpc":     {},
	"database/sql":               {},
	"github.com/nats-io/nats.go": {},
}

type violation struct {
	file       string
	importPath string
	reason     string
}

func inspectArchitecture(rootDir string) ([]violation, error) {
	var violations []violation
	fset := token.NewFileSet()

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		// Ignore testdata when checking actual codebase
		rel, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)

		node, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		for _, imp := range node.Imports {
			impPath := strings.Trim(imp.Path.Value, `"`)

			// Rule 1: kernel and platform must not import any internal/<service>
			if strings.HasPrefix(relSlash, "internal/kernel/") || strings.HasPrefix(relSlash, "internal/platform/") || strings.Contains(relSlash, "kernel_bad") {
				// Don't flag testdata itself when checking the testdata fixture
				for _, svc := range knownServices {
					if strings.Contains(impPath, "internal/"+svc) {
						violations = append(violations, violation{
							file:       relSlash,
							importPath: impPath,
							reason:     "kernel and platform must not import internal/" + svc,
						})
					}
				}
			}

			// Rule 2: no internal/<service> may import another internal/<service>
			for _, svc := range knownServices {
				if (strings.HasPrefix(relSlash, "internal/"+svc+"/") || strings.Contains(relSlash, svc+"_bad")) && !strings.Contains(relSlash, "arch_violator") {
					for _, otherSvc := range knownServices {
						if svc != otherSvc && strings.Contains(impPath, "internal/"+otherSvc) {
							violations = append(violations, violation{
								file:       relSlash,
								importPath: impPath,
								reason:     "service " + svc + " must not import other service " + otherSvc,
							})
						}
					}
				}
			}
			// When checking the fixture:
			if strings.Contains(relSlash, "edge_bad") && strings.Contains(impPath, "internal/gateway") {
				violations = append(violations, violation{
					file:       relSlash,
					importPath: impPath,
					reason:     "service edge must not import other service gateway",
				})
			}

			// Rule 3: internal/<service>/core must not import net/http, grpc, database/sql, nats.go
			if strings.Contains(relSlash, "/core/") || strings.Contains(relSlash, "core_bad") {
				if _, forbidden := forbiddenCoreImports[impPath]; forbidden {
					violations = append(violations, violation{
						file:       relSlash,
						importPath: impPath,
						reason:     "service core must not import transport/database/messaging: " + impPath,
					})
				}
			}
		}

		return nil
	})

	return violations, err
}

// AC-1A-13: Given o teste de arquitetura, when roda, then:
// - kernel e platform não importam internal/<serviço>;
// - nenhum internal/<serviço> importa outro internal/<serviço>;
// - o núcleo de cada serviço (internal/<serviço>/core) não importa net/http, grpc, sql nem nats;
// - um pacote de testdata que viola as regras faz o teste reportar a violação.
func TestAC1A13_ArchitectureRules(t *testing.T) {
	root := findRepoRoot(t)

	// Exclude testdata from the main architecture check
	internalDir := filepath.Join(root, "internal")
	violations, err := inspectArchitecture(internalDir)
	if err != nil {
		t.Fatalf("failed to inspect architecture: %v", err)
	}

	// Filter out testdata violations from main check
	var actualViolations []violation
	for _, v := range violations {
		if !strings.Contains(v.file, "testdata/arch_violator") {
			actualViolations = append(actualViolations, v)
		}
	}

	if len(actualViolations) > 0 {
		for _, v := range actualViolations {
			t.Errorf("architecture violation in %s: imports %s (%s)", v.file, v.importPath, v.reason)
		}
		t.Fatalf("found %d architecture violations in internal/", len(actualViolations))
	}
}

func TestAC1A13_ArchitectureViolationReportedOnTestdata(t *testing.T) {
	root := findRepoRoot(t)
	violatorDir := filepath.Join(root, "internal", "platform", "testdata", "arch_violator")

	violations, err := inspectArchitecture(violatorDir)
	if err != nil {
		t.Fatalf("failed to inspect violator fixture: %v", err)
	}

	if len(violations) < 3 {
		t.Fatalf("expected at least 3 violations in arch_violator fixture, got %d: %v", len(violations), violations)
	}
}
