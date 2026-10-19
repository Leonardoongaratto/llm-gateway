package claudecode_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func setupTestStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()

	sqlitePath := filepath.Join(t.TempDir(), fmt.Sprintf("test-claude-imp-%d.db", time.Now().UnixNano()))
	db, err := sqlx.OpenSQLite("file:" + sqlitePath + "?cache=shared")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := store.Migrate(ctx, db); err != nil {
		t.Fatalf("failed to migrate db: %v", err)
	}

	return store.New(db)
}

func verifyImportedRecords(ctx context.Context, t *testing.T, st *store.Store) {
	records, total, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceClaudeCodeLocal)})
	if err != nil || total != 2 {
		t.Fatalf("expected 2 usage records, got %d (err: %v)", total, err)
	}

	for _, r := range records {
		if r.Project != "finance-app" {
			t.Errorf("expected project 'finance-app', got '%s'", r.Project)
		}
		if r.Source != string(core.SourceClaudeCodeLocal) {
			t.Errorf("expected source '%s', got '%s'", core.SourceClaudeCodeLocal, r.Source)
		}
		if r.CostKind != string(core.CostKindEquivalente) {
			t.Errorf("expected cost_kind '%s', got '%s'", core.CostKindEquivalente, r.CostKind)
		}
		if r.CostMicros <= 0 {
			t.Errorf("expected positive equivalent cost, got %d", r.CostMicros)
		}
	}
}

func assertNoSecretInRow(t *testing.T, vals []any, tbl, secretMarker string) {
	t.Helper()
	for _, v := range vals {
		if v == nil {
			continue
		}
		if strings.Contains(fmt.Sprintf("%v", v), secretMarker) {
			t.Fatalf("PRIVACY VIOLATION: secret marker %q found in table %s!", secretMarker, tbl)
		}
	}
}

func assertNoSecretInTable(ctx context.Context, t *testing.T, st *store.Store, tbl, secretMarker string) {
	t.Helper()
	rows, err := st.DB().QueryContext(ctx, "SELECT * FROM "+tbl)
	if err != nil {
		t.Fatalf("failed to query table %s: %v", tbl, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns for %s: %v", tbl, err)
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan error on %s: %v", tbl, err)
		}
		assertNoSecretInRow(t, vals, tbl, secretMarker)
	}
}

func assertNoSecretInStoreAndLogs(ctx context.Context, t *testing.T, st *store.Store, logBuf *bytes.Buffer, secretMarker string) {
	t.Helper()
	if logBuf != nil && strings.Contains(logBuf.String(), secretMarker) {
		t.Fatalf("PRIVACY VIOLATION: secret marker %q found in logger output!", secretMarker)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	promhttp.Handler().ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), secretMarker) {
		t.Fatalf("PRIVACY VIOLATION: secret marker %q found in /metrics output!", secretMarker)
	}

	tables := []string{
		"usage_requests",
		"import_runs",
		"import_cursors",
		"events_audit",
		"daily_aggregates",
		"subscriptions",
		"processed_messages",
	}

	for _, tbl := range tables {
		assertNoSecretInTable(ctx, t, st, tbl, secretMarker)
	}
}

func TestImporter_AC1E04_And_AC1E06_ImportAndPrivacy(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	tempDir := t.TempDir()
	projDir := filepath.Join(tempDir, "finance-app")
	if err := os.MkdirAll(projDir, 0755); err != nil {
		t.Fatalf("failed to create proj dir: %v", err)
	}

	secretMarker := "SEGREDO-LGW-TEST-CONFIDENCIAL-12345"

	jsonlContent := fmt.Sprintf(`{"timestamp":"2026-10-18T12:00:00Z","type":"message","message":{"id":"msg-001","model":"claude-3-5-sonnet-20241022","content":"%s: este é um prompt confidencial","usage":{"input_tokens":1200,"output_tokens":350,"cache_read_input_tokens":100,"cache_creation_input_tokens":50}}}
{"timestamp":"2026-10-18T12:05:00Z","type":"progress","status":"thinking"}
{"timestamp":"2026-10-18T12:07:00Z","malformed_with_secret":"%s"
{"timestamp":"2026-10-18T12:10:00Z","type":"message","message":{"id":"msg-002","model":"claude-3-5-sonnet-20241022","prompt":"%s: mais segredos em prompt","usage":{"input_tokens":800,"output_tokens":200,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}
`, secretMarker, secretMarker, secretMarker)

	filePath := filepath.Join(projDir, "session-1.jsonl")
	if err := os.WriteFile(filePath, []byte(jsonlContent), 0644); err != nil {
		t.Fatalf("failed to write jsonl file: %v", err)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	imp := claudecode.New(st, tempDir, logger)
	run, err := imp.Import(ctx)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if run.RecordsImported != 2 {
		t.Errorf("expected 2 records imported, got %d", run.RecordsImported)
	}

	verifyImportedRecords(ctx, t, st)
	assertNoSecretInStoreAndLogs(ctx, t, st, &logBuf, secretMarker)
}

func TestImporter_AC1E05_IncrementalCursor(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	tempDir := t.TempDir()
	projDir := filepath.Join(tempDir, "my-service")
	_ = os.MkdirAll(projDir, 0755)

	filePath := filepath.Join(projDir, "transcript.jsonl")

	line1 := `{"timestamp":"2026-10-18T10:00:00Z","type":"message","id":"l-1","model":"claude-3-5-sonnet","usage":{"input_tokens":500,"output_tokens":100}}` + "\n"
	if err := os.WriteFile(filePath, []byte(line1), 0644); err != nil {
		t.Fatalf("failed to write line 1: %v", err)
	}

	imp := claudecode.New(st, tempDir, nil)

	run1, err := imp.Import(ctx)
	if err != nil || run1.RecordsImported != 1 {
		t.Fatalf("first run error: imported=%d, err=%v", run1.RecordsImported, err)
	}

	run2, err := imp.Import(ctx)
	if err != nil || run2.RecordsImported != 0 {
		t.Fatalf("second run should import 0 new records: imported=%d, err=%v", run2.RecordsImported, err)
	}

	line2 := `{"timestamp":"2026-10-18T10:05:00Z","type":"message","id":"l-2","model":"claude-3-5-sonnet","usage":{"input_tokens":600,"output_tokens":150}}` + "\n"
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open for append: %v", err)
	}
	if _, err := f.WriteString(line2); err != nil {
		t.Fatalf("failed to append line 2: %v", err)
	}
	_ = f.Close()

	run3, err := imp.Import(ctx)
	if err != nil || run3.RecordsImported != 1 {
		t.Fatalf("third run should import exactly 1 appended record: imported=%d, err=%v", run3.RecordsImported, err)
	}

	_, total, err := st.QueryUsage(ctx, store.UsageFilter{Source: string(core.SourceClaudeCodeLocal)})
	if err != nil || total != 2 {
		t.Fatalf("expected total 2 records in DB, got %d", total)
	}
}

func TestImporter_AC1E07_MalformedLinesIgnored(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	tempDir := t.TempDir()
	projDir := filepath.Join(tempDir, "faulty-proj")
	_ = os.MkdirAll(projDir, 0755)

	filePath := filepath.Join(projDir, "corrupt.jsonl")

	content := `{"timestamp":"2026-10-18T11:00:00Z","type":"message","id":"good-1","model":"claude-3-5-sonnet","usage":{"input_tokens":300,"output_tokens":50}}
{this is corrupted json on line 2
{"timestamp":"2026-10-18T11:01:00Z","type":"message","id":"bad-type","model":"claude-3-5-sonnet","usage":{"input_tokens":"invalid-type-instead-of-int"}}
{"timestamp":"2026-10-18T11:02:00Z","type":"message","id":"good-2","model":"claude-3-5-sonnet","usage":{"input_tokens":400,"output_tokens":80}}
`
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	imp := claudecode.New(st, tempDir, nil)
	run, err := imp.Import(ctx)
	if err != nil {
		t.Fatalf("import returned unexpected error: %v", err)
	}

	if run.Status != "partial" {
		t.Errorf("expected status 'partial', got '%s'", run.Status)
	}
	if run.ErrorsCount != 2 {
		t.Errorf("expected 2 errors, got %d", run.ErrorsCount)
	}
	if !strings.Contains(run.ErrorDetails, "corrupt.jsonl:2") {
		t.Errorf("expected error details to mention corrupt.jsonl:2, got %s", run.ErrorDetails)
	}
	if run.RecordsImported != 2 {
		t.Errorf("expected 2 valid records imported despite malformed line, got %d", run.RecordsImported)
	}
}

func TestImporter_BUG06_PartialLineAtEOF(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	tempDir := t.TempDir()
	projDir := filepath.Join(tempDir, "active-project")
	_ = os.MkdirAll(projDir, 0755)

	filePath := filepath.Join(projDir, "transcript.jsonl")

	halfLine := `{"timestamp":"2026-10-18T10:00:00Z","type":"message","id":"msg-partial","model":"claude-3-5-sonnet"`
	if err := os.WriteFile(filePath, []byte(halfLine), 0644); err != nil {
		t.Fatalf("failed to write partial line: %v", err)
	}

	imp := claudecode.New(st, tempDir, nil)

	run1, err := imp.Import(ctx)
	if err != nil {
		t.Fatalf("first import failed: %v", err)
	}
	if run1.RecordsImported != 0 {
		t.Errorf("expected 0 records imported from partial line, got %d", run1.RecordsImported)
	}
	if run1.ErrorsCount != 0 {
		t.Errorf("expected 0 errors from partial line at EOF, got %d", run1.ErrorsCount)
	}

	restOfLine := `,"usage":{"input_tokens":500,"output_tokens":100}}` + "\n"
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file for append: %v", err)
	}
	if _, err := f.WriteString(restOfLine); err != nil {
		t.Fatalf("failed to write rest of line: %v", err)
	}
	_ = f.Close()

	run2, err := imp.Import(ctx)
	if err != nil {
		t.Fatalf("second import failed: %v", err)
	}
	if run2.RecordsImported != 1 {
		t.Errorf("expected 1 record imported after line completion, got %d", run2.RecordsImported)
	}
	if run2.ErrorsCount != 0 {
		t.Errorf("expected 0 errors on completed line, got %d", run2.ErrorsCount)
	}
}

func TestImporter_CursorResetOnTruncation(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	tempDir := t.TempDir()
	projDir := filepath.Join(tempDir, "rotated-project")
	_ = os.MkdirAll(projDir, 0755)

	filePath := filepath.Join(projDir, "transcript.jsonl")
	line := `{"timestamp":"2026-10-18T10:00:00Z","type":"message","id":"msg-rot-1","model":"claude-3-5-sonnet","usage":{"input_tokens":300,"output_tokens":50}}` + "\n" +
		`{"timestamp":"2026-10-18T10:05:00Z","type":"message","id":"msg-rot-2","model":"claude-3-5-sonnet","usage":{"input_tokens":350,"output_tokens":55}}` + "\n"
	if err := os.WriteFile(filePath, []byte(line), 0644); err != nil {
		t.Fatalf("failed to write line: %v", err)
	}

	imp := claudecode.New(st, tempDir, nil)
	run1, err := imp.Import(ctx)
	if err != nil || run1.RecordsImported != 2 {
		t.Fatalf("expected 2 records imported, got %d, err=%v", run1.RecordsImported, err)
	}

	newLine := `{"timestamp":"2026-10-18T10:10:00Z","type":"message","id":"msg-rot-3","model":"claude-3-5-sonnet","usage":{"input_tokens":400,"output_tokens":60}}` + "\n"
	if err := os.WriteFile(filePath, []byte(newLine), 0644); err != nil {
		t.Fatalf("failed to overwrite file: %v", err)
	}

	run2, err := imp.Import(ctx)
	if err != nil || run2.RecordsImported != 1 {
		t.Fatalf("expected 1 record imported after truncation, got %d, err=%v", run2.RecordsImported, err)
	}
}

func TestImporter_NonExistentDir(t *testing.T) {
	ctx := context.Background()
	st := setupTestStore(t)

	imp := claudecode.New(st, filepath.Join(t.TempDir(), "does-not-exist"), nil)
	run, err := imp.Import(ctx)
	if err != nil || run.Status != "success" || run.RecordsImported != 0 {
		t.Fatalf("expected non-existent dir to succeed with 0 imports: %+v, err=%v", run, err)
	}
}

func TestImporter_IsEnabled(t *testing.T) {
	st := setupTestStore(t)
	tempDir := t.TempDir()

	existingDir := filepath.Join(tempDir, "existing-dir")
	if err := os.MkdirAll(existingDir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	regularFile := filepath.Join(tempDir, "regular-file.txt")
	if err := os.WriteFile(regularFile, []byte("hello"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	nonExistent := filepath.Join(tempDir, "does-not-exist")

	tests := []struct {
		name     string
		dir      string
		expected bool
	}{
		{name: "existing directory", dir: existingDir, expected: true},
		{name: "non-existent path", dir: nonExistent, expected: false},
		{name: "regular file", dir: regularFile, expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			imp := claudecode.New(st, tt.dir, nil)
			if got := imp.IsEnabled(); got != tt.expected {
				t.Errorf("IsEnabled() for %s = %v, expected %v", tt.name, got, tt.expected)
			}
		})
	}
}

func TestImporter_IsEnabled_EnvAndDefault(t *testing.T) {
	st := setupTestStore(t)
	tempDir := t.TempDir()

	t.Setenv("LGW_CLAUDE_CODE_DIR", tempDir)
	imp := claudecode.New(st, "", nil)
	if !imp.IsEnabled() {
		t.Errorf("expected IsEnabled() = true when LGW_CLAUDE_CODE_DIR is valid dir")
	}

	t.Setenv("LGW_CLAUDE_CODE_DIR", filepath.Join(tempDir, "absent"))
	impAbsent := claudecode.New(st, "", nil)
	if impAbsent.IsEnabled() {
		t.Errorf("expected IsEnabled() = false when LGW_CLAUDE_CODE_DIR is absent")
	}

	t.Setenv("LGW_CLAUDE_CODE_DIR", "")
	mockHome := filepath.Join(tempDir, "mock-home")
	t.Setenv("HOME", mockHome)
	t.Setenv("USERPROFILE", mockHome)
	defaultClaudeProjects := filepath.Join(mockHome, ".claude", "projects")
	if err := os.MkdirAll(defaultClaudeProjects, 0755); err != nil {
		t.Fatalf("failed to create default projects dir: %v", err)
	}
	impDefault := claudecode.New(st, "", nil)
	if !impDefault.IsEnabled() {
		t.Errorf("expected IsEnabled() = true with default ~/.claude/projects")
	}
}
