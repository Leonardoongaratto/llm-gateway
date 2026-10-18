package platform_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var knownServices = []string{"edge", "gateway", "budget", "control", "usage"}

var forbiddenCoreImportPrefixes = []string{
	"net/http",
	"google.golang.org/grpc",
	"database/sql",
	"github.com/nats-io/nats.go",
}

type violation struct {
	file       string
	importPath string
	rule       string
}

func isForbiddenCoreImport(impPath string) bool {
	for _, prefix := range forbiddenCoreImportPrefixes {
		if impPath == prefix || strings.HasPrefix(impPath, prefix+"/") {
			return true
		}
	}
	return false
}

func checkRule1(relSlash, impPath string) *violation {
	if !strings.HasPrefix(relSlash, "kernel/") && !strings.HasPrefix(relSlash, "platform/") {
		return nil
	}
	for _, svc := range knownServices {
		if strings.Contains(impPath, "internal/"+svc) {
			return &violation{
				file:       relSlash,
				importPath: impPath,
				rule:       "rule-1: kernel and platform must not import internal/" + svc,
			}
		}
	}
	return nil
}

func checkRule2(relSlash, impPath string) *violation {
	for _, svc := range knownServices {
		if !strings.HasPrefix(relSlash, svc+"/") {
			continue
		}
		for _, otherSvc := range knownServices {
			if svc != otherSvc && strings.Contains(impPath, "internal/"+otherSvc) {
				return &violation{
					file:       relSlash,
					importPath: impPath,
					rule:       "rule-2: service " + svc + " must not import other service " + otherSvc,
				}
			}
		}
	}
	return nil
}

func checkRule3(relSlash, impPath string) *violation {
	if strings.Contains(relSlash, "/core/") && isForbiddenCoreImport(impPath) {
		return &violation{
			file:       relSlash,
			importPath: impPath,
			rule:       "rule-3: service core must not import transport/database/messaging: " + impPath,
		}
	}
	return nil
}

func checkFileImports(relSlash string, imports []*ast.ImportSpec) []violation {
	var violations []violation
	for _, imp := range imports {
		impPath := strings.Trim(imp.Path.Value, `"`)
		if v := checkRule1(relSlash, impPath); v != nil {
			violations = append(violations, *v)
		}
		if v := checkRule2(relSlash, impPath); v != nil {
			violations = append(violations, *v)
		}
		if v := checkRule3(relSlash, impPath); v != nil {
			violations = append(violations, *v)
		}
	}
	return violations
}

func inspectArchitecture(internalDir string) ([]violation, error) {
	var violations []violation
	fset := token.NewFileSet()

	err := filepath.Walk(internalDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		rel, err := filepath.Rel(internalDir, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)

		node, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		violations = append(violations, checkFileImports(relSlash, node.Imports)...)
		return nil
	})

	return violations, err
}

func TestAC1A13_ArchitectureRules(t *testing.T) {
	root := findRepoRoot(t)
	internalDir := filepath.Join(root, "internal")

	violations, err := inspectArchitecture(internalDir)
	if err != nil {
		t.Fatalf("failed to inspect architecture: %v", err)
	}

	var actualViolations []violation
	for _, v := range violations {
		if !strings.Contains(v.file, "testdata") {
			actualViolations = append(actualViolations, v)
		}
	}

	if len(actualViolations) > 0 {
		for _, v := range actualViolations {
			t.Errorf("architecture violation in %s: imports %s (%s)", v.file, v.importPath, v.rule)
		}
		t.Fatalf("found %d architecture violations in internal/", len(actualViolations))
	}
}

func TestAC1A13_ArchitectureViolationReportedOnTestdata(t *testing.T) {
	root := findRepoRoot(t)
	violatorInternalDir := filepath.Join(root, "internal", "platform", "testdata", "arch_violator", "internal")

	violations, err := inspectArchitecture(violatorInternalDir)
	if err != nil {
		t.Fatalf("failed to inspect violator fixture: %v", err)
	}

	if len(violations) != 3 {
		t.Fatalf("expected exactly 3 violations in arch_violator fixture, got %d: %v", len(violations), violations)
	}

	rulesTriggered := make(map[string]bool)
	for _, v := range violations {
		if strings.HasPrefix(v.rule, "rule-1") {
			rulesTriggered["rule-1"] = true
		}
		if strings.HasPrefix(v.rule, "rule-2") {
			rulesTriggered["rule-2"] = true
		}
		if strings.HasPrefix(v.rule, "rule-3") {
			rulesTriggered["rule-3"] = true
		}
	}

	for _, expectedRule := range []string{"rule-1", "rule-2", "rule-3"} {
		if !rulesTriggered[expectedRule] {
			t.Errorf("expected violation for %s to be triggered by testdata fixture", expectedRule)
		}
	}
}

func parsePortsFromConfigFields(fields []*ast.Field) (httpPort, grpcPort int, err error) {
	for _, field := range fields {
		if field.Tag == nil {
			continue
		}
		tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
		envName := tag.Get("env")
		defaultVal := tag.Get("default")
		switch envName {
		case "HTTP_PORT":
			p, err := strconv.Atoi(defaultVal)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid http port %q: %w", defaultVal, err)
			}
			httpPort = p
		case "GRPC_PORT":
			p, err := strconv.Atoi(defaultVal)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid grpc port %q: %w", defaultVal, err)
			}
			grpcPort = p
		}
	}
	return httpPort, grpcPort, nil
}

func extractConfigPorts(mainFile string) (httpPort, grpcPort int, err error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, mainFile, nil, 0)
	if err != nil {
		return 0, 0, err
	}
	for _, decl := range node.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "Config" {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			return parsePortsFromConfigFields(structType.Fields.List)
		}
	}
	return 0, 0, fmt.Errorf("Config struct not found in %s", mainFile)
}

func TestAC1A11_ServiceDefaultPortsContract(t *testing.T) {
	root := findRepoRoot(t)

	contract := map[string]struct {
		httpPort int
		grpcPort int
	}{
		"edge":    {httpPort: 8787, grpcPort: 0},
		"gateway": {httpPort: 8788, grpcPort: 0},
		"control": {httpPort: 8790, grpcPort: 9102},
		"budget":  {httpPort: 8792, grpcPort: 9101},
		"usage":   {httpPort: 8791, grpcPort: 0},
	}

	for svc, expected := range contract {
		svc := svc
		expected := expected
		t.Run(svc, func(t *testing.T) {
			mainFile := filepath.Join(root, "cmd", svc, "main.go")
			actualHTTP, actualGRPC, err := extractConfigPorts(mainFile)
			if err != nil {
				t.Fatalf("failed to extract ports from %s: %v", mainFile, err)
			}

			if actualHTTP != expected.httpPort {
				t.Errorf("%s: expected HTTPPort default %d, got %d", svc, expected.httpPort, actualHTTP)
			}
			if actualGRPC != expected.grpcPort {
				t.Errorf("%s: expected GRPCPort default %d, got %d", svc, expected.grpcPort, actualGRPC)
			}
		})
	}
}
