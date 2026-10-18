package platform_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current dir: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "buf.yaml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo root with buf.yaml starting from %s", dir)
		}
		dir = parent
	}
}

// AC-1A-01: Given os .proto, when buf lint e buf generate rodam, then o lint passa e o gen/ regenerado não difere do versionado.
func TestAC1A01_BufLintAndGenerate(t *testing.T) {
	root := findRepoRoot(t)

	lintCmd := exec.Command("buf", "lint")
	lintCmd.Dir = root
	out, err := lintCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("buf lint failed: %v\nOutput: %s", err, string(out))
	}

	genCmd := exec.Command("buf", "generate")
	genCmd.Dir = root
	out, err = genCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("buf generate failed: %v\nOutput: %s", err, string(out))
	}

	diffCmd := exec.Command("git", "diff", "--exit-code", "gen")
	diffCmd.Dir = root
	out, err = diffCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gen/ differs from versioned state: %v\nDiff: %s", err, string(out))
	}
}

// AC-1A-02: Given uma cópia temporária do proto com o tipo de um campo alterado, when buf breaking --against o estado atual, then o comando falha.
func TestAC1A02_BufBreakingDetection(t *testing.T) {
	root := findRepoRoot(t)
	tempDir := t.TempDir()

	protoDest := filepath.Join(tempDir, "proto")
	if err := copyDir(filepath.Join(root, "proto"), protoDest); err != nil {
		t.Fatalf("failed to copy proto dir: %v", err)
	}

	bufYamlContent, err := os.ReadFile(filepath.Join(root, "buf.yaml"))
	if err != nil {
		t.Fatalf("failed to read buf.yaml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, "buf.yaml"), bufYamlContent, 0644); err != nil {
		t.Fatalf("failed to copy buf.yaml: %v", err)
	}

	targetProto := filepath.Join(protoDest, "lgw", "budget", "v1", "budget.proto")
	content, err := os.ReadFile(targetProto)
	if err != nil {
		t.Fatalf("failed to read target proto: %v", err)
	}

	modified := strings.Replace(string(content), "int64 max_micros = 2;", "string max_micros = 2;", 1)
	if modified == string(content) {
		t.Fatalf("failed to replace target field type in %s", targetProto)
	}
	if err := os.WriteFile(targetProto, []byte(modified), 0644); err != nil {
		t.Fatalf("failed to write modified proto: %v", err)
	}

	cmd := exec.Command("buf", "breaking", tempDir, "--against", root)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected buf breaking to fail, but it succeeded. Output: %s", string(out))
	}

	if !strings.Contains(string(out), "changed type from \"int64\" to \"string\"") {
		t.Fatalf("expected output to mention type change, got: %s", string(out))
	}
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
}
