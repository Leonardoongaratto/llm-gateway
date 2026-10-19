package core

import "strings"

type ModelPricing struct {
	InputPerMTokMicros      int64
	OutputPerMTokMicros     int64
	CacheReadPerMTokMicros  int64
	CacheWritePerMTokMicros int64
}

func DefaultPricing(model string) ModelPricing {
	lower := strings.ToLower(model)
	switch {
	case strings.Contains(lower, "opus"):
		return ModelPricing{
			InputPerMTokMicros:      15000000,
			OutputPerMTokMicros:     75000000,
			CacheReadPerMTokMicros:  1500000,
			CacheWritePerMTokMicros: 18750000,
		}
	case strings.Contains(lower, "haiku"):
		return ModelPricing{
			InputPerMTokMicros:      800000,
			OutputPerMTokMicros:     4000000,
			CacheReadPerMTokMicros:  80000,
			CacheWritePerMTokMicros: 1000000,
		}
	case strings.Contains(lower, "gpt-4o-mini"):
		return ModelPricing{
			InputPerMTokMicros:      150000,
			OutputPerMTokMicros:     600000,
			CacheReadPerMTokMicros:  75000,
			CacheWritePerMTokMicros: 150000,
		}
	case strings.Contains(lower, "gpt-4o"):
		return ModelPricing{
			InputPerMTokMicros:      2500000,
			OutputPerMTokMicros:     10000000,
			CacheReadPerMTokMicros:  1250000,
			CacheWritePerMTokMicros: 2500000,
		}
	default:
		// Default to Sonnet rates
		return ModelPricing{
			InputPerMTokMicros:      3000000,
			OutputPerMTokMicros:     15000000,
			CacheReadPerMTokMicros:  300000,
			CacheWritePerMTokMicros: 3750000,
		}
	}
}

func CalculateTokenCostMicros(model string, input, output, cacheRead, cacheWrite int64) int64 {
	p := DefaultPricing(model)
	const mtok = 1000000

	cost := (input * p.InputPerMTokMicros) / mtok
	cost += (output * p.OutputPerMTokMicros) / mtok
	cost += (cacheRead * p.CacheReadPerMTokMicros) / mtok
	cost += (cacheWrite * p.CacheWritePerMTokMicros) / mtok
	return cost
}
