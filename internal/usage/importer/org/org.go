package org

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
	"github.com/google/uuid"
)

var (
	ErrSourceNotEnabled = errors.New("organization importer not enabled for source")
	ErrUnsupportedOrg   = errors.New("unsupported organization source")
)

type Config struct {
	AnthropicAdminKey string
	AnthropicBaseURL  string
	OpenAIAdminKey    string
	OpenAIBaseURL     string
	HTTPClient        *http.Client
}

type Importer struct {
	store      *store.Store
	cfg        Config
	logger     *slog.Logger
	httpClient *http.Client
}

func New(st *store.Store, cfg Config, logger *slog.Logger) *Importer {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.AnthropicAdminKey == "" {
		cfg.AnthropicAdminKey = os.Getenv("LGW_ANTHROPIC_ADMIN_KEY")
	}
	if cfg.OpenAIAdminKey == "" {
		cfg.OpenAIAdminKey = os.Getenv("LGW_OPENAI_ADMIN_KEY")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &Importer{
		store:      st,
		cfg:        cfg,
		logger:     logger,
		httpClient: client,
	}
}

func (imp *Importer) IsEnabled(source string) bool {
	switch source {
	case "anthropic-org":
		return imp.cfg.AnthropicAdminKey != ""
	case "openai-org":
		return imp.cfg.OpenAIAdminKey != ""
	default:
		return false
	}
}

type UsageItem struct {
	Date             string `json:"date"`
	Model            string `json:"model"`
	InputTokens      int64  `json:"input_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	CacheReadTokens  int64  `json:"cache_read_tokens"`
	CacheWriteTokens int64  `json:"cache_write_tokens"`
	CostMicros       int64  `json:"cost_micros"`
}

type ReportResponse struct {
	Items []UsageItem `json:"items"`
}

func (imp *Importer) Import(ctx context.Context, source string) (*store.ImportRun, error) {
	if !imp.IsEnabled(source) {
		return nil, fmt.Errorf("%w: %s", ErrSourceNotEnabled, source)
	}

	runID := "run-" + uuid.NewString()
	startTime := time.Now().UTC()

	run := store.ImportRun{
		ID:        runID,
		Source:    source,
		Status:    "running",
		StartedAt: startTime,
	}

	if err := imp.store.CreateImportRun(ctx, run); err != nil {
		return nil, fmt.Errorf("failed to create import run: %w", err)
	}

	var (
		records  []store.UsageRecord
		fetchErr error
	)

	switch source {
	case "anthropic-org":
		records, fetchErr = imp.fetchAnthropicUsage(ctx)
	case "openai-org":
		records, fetchErr = imp.fetchOpenAIUsage(ctx)
	default:
		fetchErr = fmt.Errorf("%w: %s", ErrUnsupportedOrg, source)
	}

	finishTime := time.Now().UTC()
	run.FinishedAt = &finishTime

	if fetchErr != nil {
		run.Status = "failed"
		run.ErrorDetails = fetchErr.Error()
		_ = imp.store.UpdateImportRun(ctx, run)
		return &run, fetchErr
	}

	run.LinesRead = int64(len(records))
	if len(records) > 0 {
		inserted, err := imp.store.RecordUsageBatch(ctx, records)
		if err != nil {
			run.Status = "failed"
			run.ErrorDetails = err.Error()
			_ = imp.store.UpdateImportRun(ctx, run)
			return &run, err
		}
		run.RecordsImported = int64(inserted)
	}

	run.Status = "success"
	_ = imp.store.UpdateImportRun(ctx, run)
	return &run, nil
}

func (imp *Importer) fetchAnthropicUsage(ctx context.Context) ([]store.UsageRecord, error) {
	baseURL := imp.cfg.AnthropicBaseURL
	if baseURL == "" {
		baseURL = "https://api.anthropic.com"
	}
	url := baseURL + "/v1/organizations/usage"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", imp.cfg.AnthropicAdminKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	return imp.doFetchOrg(req, "anthropic", core.SourceAnthropicOrg)
}

func (imp *Importer) fetchOpenAIUsage(ctx context.Context) ([]store.UsageRecord, error) {
	baseURL := imp.cfg.OpenAIBaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com"
	}
	url := baseURL + "/v1/organization/usage"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+imp.cfg.OpenAIAdminKey)

	return imp.doFetchOrg(req, "openai", core.SourceOpenAIOrg)
}

func (imp *Importer) doFetchOrg(req *http.Request, provider string, source core.Source) ([]store.UsageRecord, error) {
	resp, err := imp.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status code %d from %s: %s", resp.StatusCode, req.URL.String(), string(body))
	}

	var payload ReportResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode response json: %w", err)
	}

	var records []store.UsageRecord
	for idx, item := range payload.Items {
		dateStr := item.Date
		if dateStr == "" {
			dateStr = time.Now().UTC().Format("2006-01-02")
		}
		t, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			t = time.Now().UTC()
		}

		cost := item.CostMicros
		if cost == 0 {
			cost = core.CalculateTokenCostMicros(item.Model, item.InputTokens, item.OutputTokens, item.CacheReadTokens, item.CacheWriteTokens)
		}

		reqID := fmt.Sprintf("org-%s-%s-%s-%d", source, dateStr, item.Model, idx)

		records = append(records, store.UsageRecord{
			RequestID:        reqID,
			ModelRequested:   item.Model,
			ModelUsed:        item.Model,
			Provider:         provider,
			Source:           string(source),
			CostKind:         string(core.CostKindCobrado),
			InputTokens:      item.InputTokens,
			OutputTokens:     item.OutputTokens,
			CacheReadTokens:  item.CacheReadTokens,
			CacheWriteTokens: item.CacheWriteTokens,
			CostMicros:       cost,
			Status:           "success",
			CreatedAt:        t,
		})
	}

	return records, nil
}
