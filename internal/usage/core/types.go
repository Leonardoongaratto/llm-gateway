package core

import (
	"errors"
	"math"
	"strings"
	"time"
)

var (
	ErrInvalidSource   = errors.New("invalid source")
	ErrInvalidCostKind = errors.New("invalid cost kind")
)

type Source string

const (
	SourceGateway         Source = "gateway"
	SourceClaudeCodeLocal Source = "claude-code-local"
	SourceAnthropicOrg    Source = "anthropic-org"
	SourceOpenAIOrg       Source = "openai-org"
)

func (s Source) String() string {
	return string(s)
}

func ValidSource(s string) bool {
	switch Source(s) {
	case SourceGateway, SourceClaudeCodeLocal, SourceAnthropicOrg, SourceOpenAIOrg:
		return true
	default:
		return false
	}
}

type CostKind string

const (
	CostKindCobrado     CostKind = "cobrado"
	CostKindEquivalente CostKind = "equivalente"
)

func (c CostKind) String() string {
	return string(c)
}

func NormalizeCostKind(s string) (CostKind, error) {
	lower := strings.ToLower(strings.TrimSpace(s))
	switch lower {
	case "cobrado", "billed":
		return CostKindCobrado, nil
	case "equivalente", "equivalent":
		return CostKindEquivalente, nil
	default:
		return "", ErrInvalidCostKind
	}
}

func ProjectedMonthMicros(spendMicros int64, currentDay int, totalDaysInMonth int) int64 {
	if currentDay <= 0 || totalDaysInMonth <= 0 {
		return spendMicros
	}
	if currentDay >= totalDaysInMonth {
		return spendMicros
	}
	return (spendMicros * int64(totalDaysInMonth)) / int64(currentDay)
}

func DaysInMonth(t time.Time) int {
	year, month, _ := t.Date()
	firstOfNextMonth := time.Date(year, month+1, 1, 0, 0, 0, 0, t.Location())
	lastOfThisMonth := firstOfNextMonth.Add(-24 * time.Hour)
	return lastOfThisMonth.Day()
}

func CalculateAproveitamento(equivalentMicros, monthlyCostMicros int64) float64 {
	if monthlyCostMicros <= 0 || equivalentMicros <= 0 {
		return 0.0
	}
	ratio := float64(equivalentMicros) / float64(monthlyCostMicros)
	return math.Round(ratio*10) / 10
}
