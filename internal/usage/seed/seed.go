package seed

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/report"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

func Demo(ctx context.Context, st *store.Store, outputPath string, asOf time.Time) (*report.SummaryReport, error) {
	if asOf.IsZero() {
		asOf = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	}

	claudeSub := store.Subscription{
		ID:                "sub-1",
		Name:              "Claude Code Pro",
		Provider:          "anthropic",
		MonthlyCostMicros: 20000000,
		RenewalDay:        15,
		CreatedAt:         asOf.AddDate(0, -3, 0),
		UpdatedAt:         asOf,
	}
	if err := st.CreateSubscription(ctx, claudeSub); err != nil {
		return nil, fmt.Errorf("failed to create subscription: %w", err)
	}

	records := generateDemoRecords(asOf)
	if _, err := st.RecordUsageBatch(ctx, records); err != nil {
		return nil, fmt.Errorf("failed to seed usage batch: %w", err)
	}

	repEngine := report.NewEngine(st)
	summary, err := repEngine.GenerateSummary(ctx, 30, asOf)
	if err != nil {
		return nil, fmt.Errorf("failed to generate demo summary: %w", err)
	}

	if outputPath != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create output dir: %w", err)
		}
		data, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to marshal summary: %w", err)
		}
		data = append(data, '\n')
		if err := os.WriteFile(outputPath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write %s: %w", outputPath, err)
		}
	}

	return summary, nil
}

func generateDemoRecords(asOf time.Time) []store.UsageRecord {
	var records []store.UsageRecord

	models := []string{
		"claude-3-5-sonnet-20241022",
		"gpt-4o",
		"claude-3-opus-20240229",
		"gpt-4o-mini",
	}

	keys := []string{
		"key-prod-backend",
		"key-analytics-worker",
		"key-mobile-api",
	}

	for d := 90; d >= 0; d-- {
		day := asOf.AddDate(0, 0, -d)
		dateStr := day.Format("2006-01-02")

		for mIdx, model := range models {
			provider := "anthropic"
			if mIdx%2 == 1 {
				provider = "openai"
			}
			keyID := keys[mIdx%len(keys)]

			inputTok := int64(100000 + (d*73+mIdx*1000)%50000)
			outputTok := int64(20000 + (d*31+mIdx*500)%15000)
			cacheRead := int64(10000 + (d*19)%5000)
			cacheWrite := int64(5000 + (d*11)%2000)

			cost := core.CalculateTokenCostMicros(model, inputTok, outputTok, cacheRead, cacheWrite)

			records = append(records, store.UsageRecord{
				RequestID:        fmt.Sprintf("demo-gw-%s-%d", dateStr, mIdx),
				KeyID:            keyID,
				Project:          "core-platform",
				ModelRequested:   model,
				ModelUsed:        model,
				Provider:         provider,
				Source:           string(core.SourceGateway),
				CostKind:         string(core.CostKindCobrado),
				InputTokens:      inputTok,
				OutputTokens:     outputTok,
				CacheReadTokens:  cacheRead,
				CacheWriteTokens: cacheWrite,
				CostMicros:       cost,
				LatencyMs:        int64(200 + (d+mIdx*50)%300),
				Status:           "success",
				CreatedAt:        day,
			})
		}
	}

	records = append(records, store.UsageRecord{
		RequestID:        "demo-ccl-target-month",
		KeyID:            "",
		Project:          "developer-tools",
		ModelRequested:   "claude-3-5-sonnet-20241022",
		ModelUsed:        "claude-3-5-sonnet-20241022",
		Provider:         "anthropic",
		Source:           string(core.SourceClaudeCodeLocal),
		CostKind:         string(core.CostKindEquivalente),
		InputTokens:      14000000,
		OutputTokens:     2800000,
		CacheReadTokens:  0,
		CacheWriteTokens: 0,
		CostMicros:       84000000,
		Status:           "success",
		CreatedAt:        asOf.AddDate(0, 0, -2),
	})

	return records
}
