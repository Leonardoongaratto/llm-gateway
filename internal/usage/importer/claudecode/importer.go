package claudecode

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/google/uuid"
)

const DefaultClaudeCodeModel = "claude-3-5-sonnet-20241022"

type rawClaudeCodeUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadTokens          int64 `json:"cache_read_tokens"`
	CacheCreationTokens      int64 `json:"cache_creation_tokens"`
}

type rawClaudeCodeMessage struct {
	ID    string              `json:"id"`
	Model string              `json:"model"`
	Usage *rawClaudeCodeUsage `json:"usage"`
}

type rawClaudeCodeLine struct {
	Timestamp string                `json:"timestamp"`
	Type      string                `json:"type"`
	ID        string                `json:"id"`
	Model     string                `json:"model"`
	Message   *rawClaudeCodeMessage `json:"message"`
	Usage     *rawClaudeCodeUsage   `json:"usage"`
}

type Importer struct {
	store  *store.Store
	logger *slog.Logger
	dir    string
}

func New(st *store.Store, dir string, logger *slog.Logger) *Importer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Importer{
		store:  st,
		logger: logger,
		dir:    dir,
	}
}

func resolveProjectsDir(dir string) string {
	if dir != "" {
		return dir
	}
	if envDir := os.Getenv("LGW_CLAUDE_CODE_DIR"); envDir != "" {
		return envDir
	}
	home, _ := os.UserHomeDir()
	if home != "" {
		return filepath.Join(home, ".claude", "projects")
	}
	return ""
}

func (imp *Importer) IsEnabled() bool {
	dir := resolveProjectsDir(imp.dir)
	if dir == "" {
		return false
	}
	info, err := os.Stat(dir)
	return err == nil && info.IsDir()
}

func finalizeImportRun(run *store.ImportRun, errorMsgs []string, walkErr error) {
	finishTime := time.Now().UTC()
	run.FinishedAt = &finishTime

	switch {
	case walkErr != nil:
		run.Status = "failed"
		errorMsgs = append(errorMsgs, walkErr.Error())
	case run.ErrorsCount > 0 && run.RecordsImported > 0:
		run.Status = "partial"
	case run.ErrorsCount > 0 && run.RecordsImported == 0 && run.LinesRead > 0:
		run.Status = "failed"
	default:
		run.Status = "success"
	}

	if len(errorMsgs) > 0 {
		run.ErrorDetails = strings.Join(errorMsgs, "; ")
		if len(run.ErrorDetails) > 2000 {
			run.ErrorDetails = run.ErrorDetails[:2000]
		}
	}
}

func (imp *Importer) Import(ctx context.Context) (*store.ImportRun, error) {
	run := store.ImportRun{
		ID:        "run-" + uuid.NewString(),
		Source:    "claude-code-local",
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}

	if err := imp.store.CreateImportRun(ctx, run); err != nil {
		return nil, fmt.Errorf("failed to create import run: %w", err)
	}

	dir := resolveProjectsDir(imp.dir)
	if dir == "" || !dirExists(dir) {
		run.Status = "success"
		now := time.Now().UTC()
		run.FinishedAt = &now
		_ = imp.store.UpdateImportRun(ctx, run)
		return &run, nil
	}

	var errorMsgs []string
	walkErr := imp.walkProjectFiles(ctx, dir, &run, &errorMsgs)
	finalizeImportRun(&run, errorMsgs, walkErr)

	_ = imp.store.UpdateImportRun(ctx, run)
	return &run, nil
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func (imp *Importer) walkProjectFiles(ctx context.Context, dir string, run *store.ImportRun, errorMsgs *[]string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
			return nil
		}

		projectName := determineProjectName(dir, path)
		if fileErr := imp.importFile(ctx, path, projectName, run, errorMsgs); fileErr != nil {
			imp.logger.Warn("error importing file", slog.String("file", path), slog.Any("error", fileErr))
		}
		return nil
	})
}

func determineProjectName(baseDir, filePath string) string {
	rel, err := filepath.Rel(baseDir, filePath)
	if err != nil {
		return filepath.Base(filepath.Dir(filePath))
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) > 1 && parts[0] != "" && parts[0] != "." {
		return parts[0]
	}
	return "default"
}

func openFileAtCursor(ctx context.Context, st *store.Store, filePath string) (*os.File, int64, int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to open %s: %w", filePath, err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, 0, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}

	var startOffset int64
	var lastLine int64

	cursor, err := st.GetCursor(ctx, "claude-code-local", filePath)
	if err == nil && cursor != nil {
		startOffset = cursor.ByteOffset
		lastLine = cursor.LastLine
	}

	if info.Size() < startOffset {
		startOffset = 0
		lastLine = 0
	}

	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, 0, 0, fmt.Errorf("failed to seek %s to %d: %w", filePath, startOffset, err)
		}
	}

	return f, startOffset, lastLine, nil
}

func processFileLine(lineBytes []byte, filePath, projectName string, currentLine int64, run *store.ImportRun, errorMsgs *[]string) (store.UsageRecord, bool) {
	run.LinesRead++
	rec, ok, parseErr := parseClaudeCodeLine(lineBytes, filePath, projectName, currentLine)
	if parseErr != nil {
		run.ErrorsCount++
		if len(*errorMsgs) < 10 {
			*errorMsgs = append(*errorMsgs, fmt.Sprintf("%s:%d: malformed json line", filepath.Base(filePath), currentLine))
		}
		return store.UsageRecord{}, false
	}
	return rec, ok
}

func parseLinesFromReader(r *bufio.Reader, filePath, projectName string, startOffset, lastLine int64, run *store.ImportRun, errorMsgs *[]string) ([]store.UsageRecord, int64, int64, error) {
	currentOffset := startOffset
	currentLine := lastLine
	var records []store.UsageRecord

	for {
		lineBytes, readErr := r.ReadBytes('\n')
		if len(lineBytes) > 0 {
			if errors.Is(readErr, io.EOF) && lineBytes[len(lineBytes)-1] != '\n' {
				break
			}
			currentOffset += int64(len(lineBytes))
			currentLine++
			if rec, ok := processFileLine(lineBytes, filePath, projectName, currentLine, run, errorMsgs); ok {
				records = append(records, rec)
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, currentOffset, currentLine, readErr
		}
	}

	return records, currentOffset, currentLine, nil
}

func (imp *Importer) importFile(ctx context.Context, filePath, projectName string, run *store.ImportRun, errorMsgs *[]string) error {
	f, startOffset, lastLine, err := openFileAtCursor(ctx, imp.store, filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	records, currentOffset, currentLine, err := parseLinesFromReader(reader, filePath, projectName, startOffset, lastLine, run, errorMsgs)
	if err != nil {
		return err
	}

	if len(records) > 0 {
		inserted, err := imp.store.RecordUsageBatch(ctx, records)
		if err != nil {
			return fmt.Errorf("failed to record batch for %s: %w", filePath, err)
		}
		run.RecordsImported += int64(inserted)
	}

	return imp.store.UpsertCursor(ctx, store.ImportCursor{
		Source:     "claude-code-local",
		FilePath:   filePath,
		ByteOffset: currentOffset,
		LastLine:   currentLine,
		UpdatedAt:  time.Now().UTC(),
	})
}

func extractUsageAndModel(entry rawClaudeCodeLine) (*rawClaudeCodeUsage, string, string) {
	var usage *rawClaudeCodeUsage
	var model string
	var msgID string

	if entry.Message != nil {
		usage = entry.Message.Usage
		model = entry.Message.Model
		msgID = entry.Message.ID
	}
	if usage == nil {
		usage = entry.Usage
	}
	if model == "" {
		model = entry.Model
	}
	if msgID == "" {
		msgID = entry.ID
	}
	if model == "" {
		model = DefaultClaudeCodeModel
	}
	return usage, model, msgID
}

func parseTimestampOrDefault(ts string) time.Time {
	if ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return t.UTC()
		}
		if t, err := time.Parse(time.RFC3339, ts); err == nil {
			return t.UTC()
		}
	}
	return time.Now().UTC()
}

func parseClaudeCodeLine(lineBytes []byte, filePath, projectName string, lineNum int64) (store.UsageRecord, bool, error) {
	trimmed := strings.TrimSpace(string(lineBytes))
	if trimmed == "" {
		return store.UsageRecord{}, false, nil
	}

	var entry rawClaudeCodeLine
	if err := json.Unmarshal([]byte(trimmed), &entry); err != nil {
		return store.UsageRecord{}, false, fmt.Errorf("malformed json: %w", err)
	}

	usage, model, msgID := extractUsageAndModel(entry)
	if usage == nil {
		return store.UsageRecord{}, false, nil
	}

	cacheRead := usage.CacheReadInputTokens
	if cacheRead == 0 {
		cacheRead = usage.CacheReadTokens
	}
	cacheWrite := usage.CacheCreationInputTokens
	if cacheWrite == 0 {
		cacheWrite = usage.CacheCreationTokens
	}

	if usage.InputTokens == 0 && usage.OutputTokens == 0 && cacheRead == 0 && cacheWrite == 0 {
		return store.UsageRecord{}, false, nil
	}

	var reqID string
	if msgID != "" {
		reqID = "ccl-" + msgID
	} else {
		hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", filePath, lineNum, entry.Timestamp)))
		reqID = fmt.Sprintf("ccl-%x", hash[:16])
	}

	occurredAt := parseTimestampOrDefault(entry.Timestamp)
	costMicros := core.CalculateTokenCostMicros(model, usage.InputTokens, usage.OutputTokens, cacheRead, cacheWrite)

	return store.UsageRecord{
		RequestID:        reqID,
		KeyID:            "",
		Project:          projectName,
		ModelRequested:   model,
		ModelUsed:        model,
		Provider:         "anthropic",
		Source:           string(core.SourceClaudeCodeLocal),
		CostKind:         string(core.CostKindEquivalente),
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		CacheReadTokens:  cacheRead,
		CacheWriteTokens: cacheWrite,
		CostMicros:       costMicros,
		LatencyMs:        0,
		Status:           "success",
		CacheHit:         cacheRead > 0,
		FallbackFrom:     "",
		CreatedAt:        occurredAt,
	}, true, nil
}
