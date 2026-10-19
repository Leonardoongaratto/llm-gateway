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

	firstOfMonth := time.Date(asOf.Year(), asOf.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	mtdAggs, err := e.store.QueryDailyAggregates(ctx, firstOfMonth, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query mtd aggregates: %w", err)
	}

	subs, err := e.store.ListSubscriptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %w", err)
	}

	var subAggs []store.DailyAggregate
	if len(subs) > 0 {
		minCycleStart := asOf
		for _, s := range subs {
			start := subscriptionCycleStart(asOf, s.RenewalDay)
			if start.Before(minCycleStart) {
				minCycleStart = start
			}
		}
		subAggs, err = e.store.QueryDailyAggregates(ctx, minCycleStart.Format("2006-01-02"), endDate)
		if err != nil {
			return nil, fmt.Errorf("failed to query subscription aggregates: %w", err)
		}
	}

	rep := e.assembleReport(currentAggs, prevAggs, mtdAggs, subAggs, subs, days, startDate, endDate, asOf)
	return rep, nil
}

func (e *Engine) assembleReport(currentAggs, prevAggs, mtdAggs, subAggs []store.DailyAggregate, subs []store.Subscription, days int, startDate, endDate string, asOf time.Time) *SummaryReport {
	reconciliation := buildReconciliation(currentAggs)
	spendMicros := reconciliation.TotalReconciledMicros
	prevSpendMicros := calculateBilledSpend(prevAggs)

	daily := buildDailyBreakdown(currentAggs)
	byModel := buildModelBreakdown(currentAggs)
	byKey := buildKeyBreakdown(currentAggs)
	byProvider := buildProviderBreakdown(currentAggs, reconciliation)
	subItems := buildSubscriptionSummary(subs, subAggs, asOf, endDate)

	currentMonthSpend := calculateBilledSpend(mtdAggs)
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
	daily := buildDailyBreakdown(aggs)
	var total int64
	for _, d := range daily {
		total += d.SpendMicros
	}
	return total
}

type provDaily struct {
	gw  int64
	org int64
}

func aggregateDailyProviderCosts(aggs []store.DailyAggregate) (map[string]map[string]*provDaily, map[string]bool) {
	dailyProv := make(map[string]map[string]*provDaily)
	allProviders := make(map[string]bool)

	for _, a := range aggs {
		if a.CostKind != string(core.CostKindCobrado) || a.Provider == "" {
			continue
		}
		allProviders[a.Provider] = true
		dp, exists := dailyProv[a.Date]
		if !exists {
			dp = make(map[string]*provDaily)
			dailyProv[a.Date] = dp
		}
		pData, exists := dp[a.Provider]
		if !exists {
			pData = &provDaily{}
			dp[a.Provider] = pData
		}
		if a.Source == string(core.SourceGateway) {
			pData.gw += a.CostMicros
		} else if a.Source == string(core.SourceAnthropicOrg) || a.Source == string(core.SourceOpenAIOrg) {
			pData.org += a.CostMicros
		}
	}
	return dailyProv, allProviders
}

func reconcileProviderAcrossDays(dailyProv map[string]map[string]*provDaily, p string) core.ProviderReconciliation {
	var totalGw, totalOrg, totalOverlap, totalDiscrepancy, totalReconciled int64
	for _, dp := range dailyProv {
		pData := dp[p]
		if pData == nil {
			continue
		}
		rec := core.ReconcileProvider(p, pData.gw, pData.org)
		totalGw += rec.GatewaySpendMicros
		totalOrg += rec.OrgSpendMicros
		totalOverlap += rec.OverlapMicros
		totalDiscrepancy += rec.DiscrepancyMicros
		totalReconciled += rec.ReconciledSpendMicros
	}
	return core.ProviderReconciliation{
		Provider:              p,
		GatewaySpendMicros:    totalGw,
		OrgSpendMicros:        totalOrg,
		OverlapMicros:         totalOverlap,
		DiscrepancyMicros:     totalDiscrepancy,
		ReconciledSpendMicros: totalReconciled,
	}
}

func buildReconciliation(aggs []store.DailyAggregate) core.ReconciliationReport {
	dailyProv, allProviders := aggregateDailyProviderCosts(aggs)

	report := core.ReconciliationReport{
		ByProvider: make(map[string]core.ProviderReconciliation),
	}

	for p := range allProviders {
		pRec := reconcileProviderAcrossDays(dailyProv, p)
		report.ByProvider[p] = pRec
		report.TotalGatewayMicros += pRec.GatewaySpendMicros
		report.TotalOrgMicros += pRec.OrgSpendMicros
		report.TotalOverlapMicros += pRec.OverlapMicros
		report.TotalDiscrepancyMicros += pRec.DiscrepancyMicros
		report.TotalReconciledMicros += pRec.ReconciledSpendMicros
	}

	return report
}

type dayData struct {
	requests int64
	tokens   int64
	gwCost   map[string]int64
	orgCost  map[string]int64
}

func (d *dayData) calculateSpend() int64 {
	allProviders := make(map[string]bool)
	for p := range d.gwCost {
		allProviders[p] = true
	}
	for p := range d.orgCost {
		allProviders[p] = true
	}
	var daySpend int64
	for p := range allProviders {
		rec := core.ReconcileProvider(p, d.gwCost[p], d.orgCost[p])
		daySpend += rec.ReconciledSpendMicros
	}
	return daySpend
}

func (d *dayData) addAggregate(a store.DailyAggregate) {
	tokens := a.InputTokens + a.OutputTokens + a.CacheReadTokens + a.CacheWriteTokens
	d.requests += a.Requests
	d.tokens += tokens

	if a.CostKind != string(core.CostKindCobrado) {
		return
	}
	if a.Source == string(core.SourceGateway) {
		d.gwCost[a.Provider] += a.CostMicros
	} else if a.Source == string(core.SourceAnthropicOrg) || a.Source == string(core.SourceOpenAIOrg) {
		d.orgCost[a.Provider] += a.CostMicros
	}
}

func buildDailyBreakdown(aggs []store.DailyAggregate) []DailyItem {
	daysMap := make(map[string]*dayData)

	for _, a := range aggs {
		d, exists := daysMap[a.Date]
		if !exists {
			d = &dayData{
				gwCost:  make(map[string]int64),
				orgCost: make(map[string]int64),
			}
			daysMap[a.Date] = d
		}
		d.addAggregate(a)
	}

	var results []DailyItem
	for date, d := range daysMap {
		results = append(results, DailyItem{
			Date:        date,
			SpendMicros: d.calculateSpend(),
			Requests:    d.requests,
			Tokens:      d.tokens,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Date < results[j].Date
	})

	return results
}

func buildModelBreakdown(aggs []store.DailyAggregate) []ModelItem {
	modelMap := make(map[string]*ModelItem)

	for _, a := range aggs {
		if a.Model == "" || a.Source != string(core.SourceGateway) {
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

func buildProviderBreakdown(aggs []store.DailyAggregate, rec core.ReconciliationReport) []ProviderItem {
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
	}

	var results []ProviderItem
	for prov, item := range provMap {
		if r, ok := rec.ByProvider[prov]; ok {
			item.SpendMicros = r.ReconciledSpendMicros
		}
		results = append(results, *item)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].SpendMicros > results[j].SpendMicros
	})

	return results
}

func subscriptionCycleStart(asOf time.Time, renewalDay int) time.Time {
	if renewalDay <= 0 {
		renewalDay = 1
	}
	daysInCurrentMonth := core.DaysInMonth(asOf)
	effectiveRenewalThisMonth := renewalDay
	if effectiveRenewalThisMonth > daysInCurrentMonth {
		effectiveRenewalThisMonth = daysInCurrentMonth
	}

	year := asOf.Year()
	month := asOf.Month()
	if effectiveRenewalThisMonth > asOf.Day() {
		month--
		if month < time.January {
			month = time.December
			year--
		}
	}
	daysInCycleMonth := core.DaysInMonth(time.Date(year, month, 1, 0, 0, 0, 0, time.UTC))
	day := renewalDay
	if day > daysInCycleMonth {
		day = daysInCycleMonth
	}
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func buildSubscriptionSummary(subs []store.Subscription, aggs []store.DailyAggregate, asOf time.Time, endDate string) []SubscriptionItem {
	var results []SubscriptionItem
	for _, s := range subs {
		cycleStartStr := subscriptionCycleStart(asOf, s.RenewalDay).Format("2006-01-02")
		var equiv int64
		for _, a := range aggs {
			if a.Date >= cycleStartStr && a.Date <= endDate && a.CostKind == string(core.CostKindEquivalente) && a.Provider == s.Provider {
				equiv += a.CostMicros
			}
		}
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
