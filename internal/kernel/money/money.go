package money

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// Micros represents monetary amounts with 1 USD = 1,000,000 micros.
type Micros int64

var (
	ErrOverflow      = errors.New("integer overflow")
	ErrInvalidAmount = errors.New("invalid money amount")
)

const (
	MicrosPerUSD  Micros = 1_000_000
	TokensPerMTok int64  = 1_000_000
)

// Add sums two Micros values and returns ErrOverflow if the sum overflows int64.
func Add(a, b Micros) (Micros, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// Sub subtracts b from a and returns ErrOverflow if underflow/overflow occurs.
func Sub(a, b Micros) (Micros, error) {
	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return 0, ErrOverflow
	}
	return a - b, nil
}

// CostFor calculates the cost in micros for a number of tokens at the given pricePerMTok.
// Cost = tokens * pricePerMTok / 1,000,000 rounded down.
func CostFor(tokens int64, pricePerMTok Micros) (Micros, error) {
	if tokens < 0 || pricePerMTok < 0 {
		return 0, ErrInvalidAmount
	}
	if tokens == 0 || pricePerMTok == 0 {
		return 0, nil
	}

	hi, lo := bits.Mul64(uint64(tokens), uint64(pricePerMTok))
	quotient, _ := bits.Div64(hi, lo, uint64(TokensPerMTok))
	if quotient > math.MaxInt64 {
		return 0, ErrOverflow
	}

	return Micros(quotient), nil
}

// FormatUSD returns a human-friendly USD representation for logs and test diagnostics.
func FormatUSD(m Micros) string {
	dollars := float64(m) / float64(MicrosPerUSD)
	if m >= 1_000_000_000 {
		intPart := m / MicrosPerUSD
		decPart := (m % MicrosPerUSD) / 10000
		return fmt.Sprintf("$%d,%03d.%02d", intPart/1000, intPart%1000, decPart)
	}
	if m%10000 == 0 {
		return fmt.Sprintf("$%.2f", dollars)
	}
	return fmt.Sprintf("$%g", dollars)
}
