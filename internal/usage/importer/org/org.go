package org

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
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

type AnthropicCostResult struct {
	Amount      any    `json:"amount"`
	Currency    string `json:"currency"`
	Model       string `json:"model"`
	WorkspaceID string `json:"workspace_id"`
}

type AnthropicCostBucket struct {
	StartingAt string                `json:"starting_at"`
	EndingAt   string                `json:"ending_at"`
	Results    []AnthropicCostResult `json:"results"`
}

type AnthropicCostReportResponse struct {
	Data     []AnthropicCostBucket `json:"data"`
	HasMore  bool                  `json:"has_more"`
	NextPage *string               `json:"next_page"`
}

type OpenAICostAmount struct {
	Value    float64 `json:"value"`
	Currency string  `json:"currency"`
}

type OpenAICostItem struct {
	Amount    OpenAICostAmount `json:"amount"`
	LineItem  string           `json:"line_item"`
	ProjectID string           `json:"project_id"`
	Timestamp int64            `json:"timestamp"`
}

type OpenAICostsResponse struct {
	Object   string           `json:"object"`
	Data     []OpenAICostItem `json:"data"`
	HasMore  bool             `json:"has_more"`
	NextPage *string          `json:"next_page"`
}

func parseAmountToMicros(amount any) int64 {
	switch v := amount.(type) {
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return int64(math.Round(f * 1_000_000))
	case float64:
		return int64(math.Round(v * 1_000_000))
	case int64:
		return v * 1_000_000
	case json.Number:
		f, _ := v.Float64()
		return int64(math.Round(f * 1_000_000))
	default:
		return 0
	}
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

func parseBucketDate(startingAt string) (time.Time, string) {
	t := time.Now().UTC()
	dateStr := t.Format("2006-01-02")
	if startingAt != "" {
		if parsedT, err := time.Parse(time.RFC3339, startingAt); err == nil {
			t = parsedT.UTC()
			dateStr = t.Format("2006-01-02")
		}
	}
	return t, dateStr
}

func convertAnthropicBucket(bucket AnthropicCostBucket) []store.UsageRecord {
	t, dateStr := parseBucketDate(bucket.StartingAt)
	var records []store.UsageRecord

	for _, res := range bucket.Results {
		cost := parseAmountToMicros(res.Amount)
		model := res.Model
		if model == "" {
			model = "claude-3-5-sonnet-20241022"
		}

		reqID := fmt.Sprintf("org-anthropic-%s-%s", dateStr, model)
		if res.WorkspaceID != "" {
			reqID = fmt.Sprintf("org-anthropic-%s-%s-%s", dateStr, model, res.WorkspaceID)
		}

		records = append(records, store.UsageRecord{
			RequestID:      reqID,
			ModelRequested: model,
			ModelUsed:      model,
			Provider:       "anthropic",
			Source:         string(core.SourceAnthropicOrg),
			CostKind:       string(core.CostKindCobrado),
			CostMicros:     cost,
			Status:         "success",
			CreatedAt:      t,
		})
	}
	return records
}

func (imp *Importer) fetchAnthropicPage(ctx context.Context, baseURL, start, end, page string) (*AnthropicCostReportResponse, error) {
	endpoint := fmt.Sprintf("%s/v1/organizations/cost_report?starting_at=%s&ending_at=%s&bucket_width=1d",
		baseURL, url.QueryEscape(start), url.QueryEscape(end))
	if page != "" {
		endpoint += "&page=" + url.QueryEscape(page)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", imp.cfg.AnthropicAdminKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := imp.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("anthropic cost_report request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status %d from anthropic: %s", resp.StatusCode, string(body))
	}

	var payload AnthropicCostReportResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode anthropic response: %w", err)
	}
	return &payload, nil
}

func (imp *Importer) fetchAnthropicUsage(ctx context.Context) ([]store.UsageRecord, error) {
	baseURL := imp.cfg.AnthropicBaseURL
	if baseURL == "" {
		baseURL = "https://api.anthropic.com"
	}

	now := time.Now().UTC()
	start := now.AddDate(0, 0, -30).Format("2006-01-02T15:04:05Z")
	end := now.Format("2006-01-02T15:04:05Z")

	var records []store.UsageRecord
	page := ""

	for pageCount := 0; pageCount < 10; pageCount++ {
		payload, err := imp.fetchAnthropicPage(ctx, baseURL, start, end, page)
		if err != nil {
			return nil, err
		}

		for _, bucket := range payload.Data {
			records = append(records, convertAnthropicBucket(bucket)...)
		}

		if !payload.HasMore || payload.NextPage == nil || *payload.NextPage == "" {
			break
		}
		page = *payload.NextPage
	}

	return records, nil
}

func convertOpenAIItem(item OpenAICostItem) store.UsageRecord {
	t := time.Now().UTC()
	if item.Timestamp > 0 {
		t = time.Unix(item.Timestamp, 0).UTC()
	}
	dateStr := t.Format("2006-01-02")
	cost := int64(math.Round(item.Amount.Value * 1_000_000))
	lineItem := item.LineItem
	if lineItem == "" {
		lineItem = "default"
	}

	reqID := fmt.Sprintf("org-openai-%s-%s", dateStr, lineItem)
	if item.ProjectID != "" {
		reqID = fmt.Sprintf("org-openai-%s-%s-%s", dateStr, lineItem, item.ProjectID)
	}

	return store.UsageRecord{
		RequestID:      reqID,
		ModelRequested: lineItem,
		ModelUsed:      lineItem,
		Provider:       "openai",
		Source:         string(core.SourceOpenAIOrg),
		CostKind:       string(core.CostKindCobrado),
		CostMicros:     cost,
		Status:         "success",
		CreatedAt:      t,
	}
}

func (imp *Importer) fetchOpenAIPage(ctx context.Context, baseURL string, startTime, endTime int64, page string) (*OpenAICostsResponse, error) {
	endpoint := fmt.Sprintf("%s/v1/organization/costs?start_time=%d&end_time=%d&limit=100",
		baseURL, startTime, endTime)
	if page != "" {
		endpoint += "&page=" + url.QueryEscape(page)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+imp.cfg.OpenAIAdminKey)

	resp, err := imp.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openai costs request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status %d from openai: %s", resp.StatusCode, string(body))
	}

	var payload OpenAICostsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode openai response: %w", err)
	}
	return &payload, nil
}

func (imp *Importer) fetchOpenAIUsage(ctx context.Context) ([]store.UsageRecord, error) {
	baseURL := imp.cfg.OpenAIBaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com"
	}

	now := time.Now().UTC()
	startTime := now.AddDate(0, 0, -30).Unix()
	endTime := now.Unix()

	var records []store.UsageRecord
	page := ""

	for pageCount := 0; pageCount < 10; pageCount++ {
		payload, err := imp.fetchOpenAIPage(ctx, baseURL, startTime, endTime, page)
		if err != nil {
			return nil, err
		}

		for _, item := range payload.Data {
			records = append(records, convertOpenAIItem(item))
		}

		if !payload.HasMore || payload.NextPage == nil || *payload.NextPage == "" {
			break
		}
		page = *payload.NextPage
	}

	return records, nil
}
