package report

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/usage/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/usage/store"
)

type DailyItem struct {
	Date        string `json:"date"`
	SpendMicros int64  `json:"spend_micros"`
	Requests    int64  `json:"requests"`
	Tokens      int64  `json:"tokens"`
}

type ModelItem struct {
	Model       string `json:"model"`
	SpendMicros int64  `json:"spend_micros"`
	Requests    int64  `json:"requests"`
	Tokens      int64  `json:"tokens"`
}

type KeyItem struct {
	KeyID       string `json:"key_id"`
	SpendMicros int64  `json:"spend_micros"`
	Requests    int64  `json:"requests"`
	Tokens      int64  `json:"tokens"`
}

type ProviderItem struct {
	Provider    string `json:"provider"`
	SpendMicros int64  `json:"spend_micros"`
	Requests    int64  `json:"requests"`
	Tokens      int64  `json:"tokens"`
}

type SubscriptionItem struct {
	ID                    string  `json:"id"`
	Name                  string  `json:"name"`
	Provider              string  `json:"provider"`
	MonthlyCostMicros     int64   `json:"monthly_cost_micros"`
	EquivalentValueMicros int64   `json:"equivalent_value_micros"`
	Aproveitamento        float64 `json:"aproveitamento"`
}

type SummaryReport struct {
	PeriodDays           int                       `json:"period_days"`
	StartDate            string                    `json:"start_date"`
	EndDate              string                    `json:"end_date"`
	SpendMicros          int64                     `json:"spend_micros"`
	PrevSpendMicros      int64                     `json:"prev_spend_micros"`
	ProjectedMonthMicros int64                     `json:"projected_month_micros"`
	Daily                []DailyItem               `json:"daily"`
	ByModel              []ModelItem               `json:"by_model"`
	ByKey                []KeyItem                 `json:"by_key"`
	ByProvider           []ProviderItem            `json:"by_provider"`
	Subscriptions        []SubscriptionItem        `json:"subscriptions"`
	Reconciliation       core.ReconciliationReport `json:"reconciliation"`
}

type Engine struct {
	store *store.Store
}

func NewEngine(st *store.Store) *Engine {
	return &Engine{store: st}
}

func (e *Engine) GenerateSummary(ctx context.Context, days int, asOf time.Time) (*SummaryReport, error) {
	if days <= 0 {
		days = 30
	}
	asOf = asOf.UTC()

	endDate := asOf.Format("2006-01-02")
	startTime := asOf.AddDate(0, 0, -days)
	startDate := startTime.Format("2006-01-02")

	prevEndTime := startTime.AddDate(0, 0, -1)
	prevEndDate := prevEndTime.Format("2006-01-02")
	prevStartTime := prevEndTime.AddDate(0, 0, -days)
	prevStartDate := prevStartTime.Format("2006-01-02")

	currentAggs, err := e.store.QueryDailyAggregates(ctx, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query current aggregates: %w", err)
	}

	prevAggs, err := e.store.QueryDailyAggregates(ctx, prevStartDate, prevEndDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query previous aggregates: %w", err)
	}

	subs, err := e.store.ListSubscriptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %w", err)
	}

	rep := e.assembleReport(currentAggs, prevAggs, subs, days, startDate, endDate, asOf)
	return rep, nil
}

func (e *Engine) assembleReport(currentAggs, prevAggs []store.DailyAggregate, subs []store.Subscription, days int, startDate, endDate string, asOf time.Time) *SummaryReport {
	daily := buildDailyBreakdown(currentAggs)
	byModel := buildModelBreakdown(currentAggs)
	byKey := buildKeyBreakdown(currentAggs)
	byProvider := buildProviderBreakdown(currentAggs)
	subItems := buildSubscriptionSummary(subs, currentAggs)
	reconciliation := buildReconciliation(currentAggs)

	spendMicros := reconciliation.TotalReconciledMicros
	prevSpendMicros := calculateBilledSpend(prevAggs)

	firstOfMonth := time.Date(asOf.Year(), asOf.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	currentMonthSpend := calculateMonthToDateSpend(currentAggs, firstOfMonth)
	projectedMonthMicros := core.ProjectedMonthMicros(currentMonthSpend, asOf.Day(), core.DaysInMonth(asOf))

	return &SummaryReport{
		PeriodDays:           days,
		StartDate:            startDate,
		EndDate:              endDate,
		SpendMicros:          spendMicros,
		PrevSpendMicros:      prevSpendMicros,
		ProjectedMonthMicros: projectedMonthMicros,
		Daily:                daily,
		ByModel:              byModel,
		ByKey:                byKey,
		ByProvider:           byProvider,
		Subscriptions:        subItems,
		Reconciliation:       reconciliation,
	}
}

func calculateBilledSpend(aggs []store.DailyAggregate) int64 {
	gwByProvider := make(map[string]int64)
	orgByProvider := make(map[string]int64)

	for _, a := range aggs {
		if a.CostKind != string(core.CostKindCobrado) {
			continue
		}
		if a.Source == string(core.SourceGateway) {
			gwByProvider[a.Provider] += a.CostMicros
		} else if a.Source == string(core.SourceAnthropicOrg) || a.Source == string(core.SourceOpenAIOrg) {
			orgByProvider[a.Provider] += a.CostMicros
		}
	}

	rep := core.ReconcileAll(gwByProvider, orgByProvider)
	return rep.TotalReconciledMicros
}

func calculateMonthToDateSpend(aggs []store.DailyAggregate, firstOfMonth string) int64 {
	var mtdAggs []store.DailyAggregate
	for _, a := range aggs {
		if a.Date >= firstOfMonth {
			mtdAggs = append(mtdAggs, a)
		}
	}
	return calculateBilledSpend(mtdAggs)
}

func buildDailyBreakdown(aggs []store.DailyAggregate) []DailyItem {
	dateMap := make(map[string]*DailyItem)

	for _, a := range aggs {
		item, exists := dateMap[a.Date]
		if !exists {
			item = &DailyItem{Date: a.Date}
			dateMap[a.Date] = item
		}
		tokens := a.InputTokens + a.OutputTokens + a.CacheReadTokens + a.CacheWriteTokens
		item.Requests += a.Requests
		item.Tokens += tokens
		if a.CostKind == string(core.CostKindCobrado) {
			item.SpendMicros += a.CostMicros
		}
	}

	var results []DailyItem
	for _, item := range dateMap {
		results = append(results, *item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Date < results[j].Date
	})

	return results
}

func buildModelBreakdown(aggs []store.DailyAggregate) []ModelItem {
	modelMap := make(map[string]*ModelItem)

	for _, a := range aggs {
		if a.Model == "" {
			continue
		}
		item, exists := modelMap[a.Model]
		if !exists {
			item = &ModelItem{Model: a.Model}
			modelMap[a.Model] = item
		}
		tokens := a.InputTokens + a.OutputTokens + a.CacheReadTokens + a.CacheWriteTokens
		item.Requests += a.Requests
		item.Tokens += tokens
		if a.CostKind == string(core.CostKindCobrado) {
			item.SpendMicros += a.CostMicros
		}
	}

	var results []ModelItem
	for _, item := range modelMap {
		results = append(results, *item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].SpendMicros > results[j].SpendMicros
	})

	return results
}

func buildKeyBreakdown(aggs []store.DailyAggregate) []KeyItem {
	keyMap := make(map[string]*KeyItem)

	for _, a := range aggs {
		if a.KeyID == "" {
			continue
		}
		item, exists := keyMap[a.KeyID]
		if !exists {
			item = &KeyItem{KeyID: a.KeyID}
			keyMap[a.KeyID] = item
		}
		tokens := a.InputTokens + a.OutputTokens + a.CacheReadTokens + a.CacheWriteTokens
		item.Requests += a.Requests
		item.Tokens += tokens
		if a.CostKind == string(core.CostKindCobrado) {
			item.SpendMicros += a.CostMicros
		}
	}

	var results []KeyItem
	for _, item := range keyMap {
		results = append(results, *item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].SpendMicros > results[j].SpendMicros
	})

	return results
}

func buildProviderBreakdown(aggs []store.DailyAggregate) []ProviderItem {
	provMap := make(map[string]*ProviderItem)

	for _, a := range aggs {
		if a.Provider == "" {
			continue
		}
		item, exists := provMap[a.Provider]
		if !exists {
			item = &ProviderItem{Provider: a.Provider}
			provMap[a.Provider] = item
		}
		tokens := a.InputTokens + a.OutputTokens + a.CacheReadTokens + a.CacheWriteTokens
		item.Requests += a.Requests
		item.Tokens += tokens
		if a.CostKind == string(core.CostKindCobrado) {
			item.SpendMicros += a.CostMicros
		}
	}

	var results []ProviderItem
	for _, item := range provMap {
		results = append(results, *item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].SpendMicros > results[j].SpendMicros
	})

	return results
}

func buildSubscriptionSummary(subs []store.Subscription, aggs []store.DailyAggregate) []SubscriptionItem {
	equivByProvider := make(map[string]int64)

	for _, a := range aggs {
		if a.CostKind == string(core.CostKindEquivalente) {
			equivByProvider[a.Provider] += a.CostMicros
		}
	}

	var results []SubscriptionItem
	for _, s := range subs {
		equiv := equivByProvider[s.Provider]
		aproveitamento := core.CalculateAproveitamento(equiv, s.MonthlyCostMicros)
		results = append(results, SubscriptionItem{
			ID:                    s.ID,
			Name:                  s.Name,
			Provider:              s.Provider,
			MonthlyCostMicros:     s.MonthlyCostMicros,
			EquivalentValueMicros: equiv,
			Aproveitamento:        aproveitamento,
		})
	}

	return results
}

func buildReconciliation(aggs []store.DailyAggregate) core.ReconciliationReport {
	gwByProvider := make(map[string]int64)
	orgByProvider := make(map[string]int64)

	for _, a := range aggs {
		if a.CostKind != string(core.CostKindCobrado) {
			continue
		}
		if a.Source == string(core.SourceGateway) {
			gwByProvider[a.Provider] += a.CostMicros
		} else if a.Source == string(core.SourceAnthropicOrg) || a.Source == string(core.SourceOpenAIOrg) {
			orgByProvider[a.Provider] += a.CostMicros
		}
	}

	return core.ReconcileAll(gwByProvider, orgByProvider)
}
