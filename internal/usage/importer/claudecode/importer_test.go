package claudecode_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/importer/claudecode"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
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

func assertNoSecretInDB(ctx context.Context, t *testing.T, st *store.Store, secretMarker string) {
	var dbDump strings.Builder
	rows, err := st.DB().QueryContext(ctx, "SELECT request_id, project, model_used, source, cost_kind FROM usage_requests")
	if err != nil {
		t.Fatalf("failed to query usage_requests: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var reqID, proj, model, src, ck string
		if err := rows.Scan(&reqID, &proj, &model, &src, &ck); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		fmt.Fprintf(&dbDump, "%s %s %s %s %s\n", reqID, proj, model, src, ck)
	}

	if strings.Contains(dbDump.String(), secretMarker) {
		t.Fatalf("PRIVACY VIOLATION: secret marker %q was found in database dump!", secretMarker)
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
{"timestamp":"2026-10-18T12:10:00Z","type":"message","message":{"id":"msg-002","model":"claude-3-5-sonnet-20241022","prompt":"%s: mais segredos em prompt","usage":{"input_tokens":800,"output_tokens":200,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}
`, secretMarker, secretMarker)

	filePath := filepath.Join(projDir, "session-1.jsonl")
	if err := os.WriteFile(filePath, []byte(jsonlContent), 0644); err != nil {
		t.Fatalf("failed to write jsonl file: %v", err)
	}

	imp := claudecode.New(st, tempDir, nil)
	run, err := imp.Import(ctx)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	if run.Status != "success" || run.RecordsImported != 2 {
		t.Errorf("expected success with 2 imported, got status=%s, imported=%d", run.Status, run.RecordsImported)
	}

	verifyImportedRecords(ctx, t, st)
	assertNoSecretInDB(ctx, t, st, secretMarker)
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
	if run.ErrorsCount != 1 {
		t.Errorf("expected 1 error, got %d", run.ErrorsCount)
	}
	if !strings.Contains(run.ErrorDetails, "corrupt.jsonl:2") {
		t.Errorf("expected error details to mention corrupt.jsonl:2, got %s", run.ErrorDetails)
	}
	if run.RecordsImported != 2 {
		t.Errorf("expected 2 valid records imported despite malformed line, got %d", run.RecordsImported)
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
