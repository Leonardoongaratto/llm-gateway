package core

import (
	"errors"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

// DefaultMaxTokens is used as conservative bound for cost estimation when max_tokens is not specified.
const DefaultMaxTokens int64 = 4096

// ErrPricingNotFound is returned when no pricing is configured for a model.
var ErrPricingNotFound = errors.New("pricing not found for model")

// EstimatePromptTokens estimates token count from messages based on character length heuristics.
func EstimatePromptTokens(messages []llm.Message) int64 {
	if len(messages) == 0 {
		return 1
	}

	var charCount int
	for _, m := range messages {
		charCount += len(m.Role) + len(m.Content) + 4
	}

	tokens := int64(charCount / 4)
	if tokens < 1 {
		return 1
	}
	return tokens
}

// EstimateMaxCost estimates the maximum possible cost in micros for a request before calling the provider.
func EstimateMaxCost(pricing *auth.PricingInfo, promptTokens int64, maxTokens int64) (money.Micros, error) {
	if pricing == nil {
		return 0, ErrPricingNotFound
	}

	if maxTokens <= 0 {
		maxTokens = DefaultMaxTokens
	}
	if promptTokens < 0 {
		promptTokens = 0
	}

	inputCost, err := money.CostFor(promptTokens, money.Micros(pricing.InputPerMTokMicros))
	if err != nil {
		return 0, err
	}

	outputCost, err := money.CostFor(maxTokens, money.Micros(pricing.OutputPerMTokMicros))
	if err != nil {
		return 0, err
	}

	return money.Add(inputCost, outputCost)
}

// CalculateCost computes the exact cost in micros for a completed request based on measured token usage.
func CalculateCost(pricing *auth.PricingInfo, usage llm.Usage) (money.Micros, error) {
	if pricing == nil {
		return 0, ErrPricingNotFound
	}

	inputCost, err := money.CostFor(usage.InputTokens, money.Micros(pricing.InputPerMTokMicros))
	if err != nil {
		return 0, err
	}

	outputCost, err := money.CostFor(usage.OutputTokens, money.Micros(pricing.OutputPerMTokMicros))
	if err != nil {
		return 0, err
	}

	cacheReadCost, err := money.CostFor(usage.CacheReadTokens, money.Micros(pricing.CacheReadPerMTokMicros))
	if err != nil {
		return 0, err
	}

	cacheWriteCost, err := money.CostFor(usage.CacheWriteTokens, money.Micros(pricing.CacheWritePerMTokMicros))
	if err != nil {
		return 0, err
	}

	total, err := money.Add(inputCost, outputCost)
	if err != nil {
		return 0, err
	}
	total, err = money.Add(total, cacheReadCost)
	if err != nil {
		return 0, err
	}
	return money.Add(total, cacheWriteCost)
}
