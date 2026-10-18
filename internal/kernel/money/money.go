package money

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"
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
	if hi >= uint64(TokensPerMTok) {
		return 0, ErrOverflow
	}
	quotient, _ := bits.Div64(hi, lo, uint64(TokensPerMTok))
	if quotient > math.MaxInt64 {
		return 0, ErrOverflow
	}

	return Micros(quotient), nil
}

func formatGrouped(val uint64, sep byte) string {
	s := fmt.Sprintf("%d", val)
	if len(s) <= 3 {
		return s
	}
	firstGroup := len(s) % 3
	if firstGroup == 0 {
		firstGroup = 3
	}
	var b strings.Builder
	b.WriteString(s[:firstGroup])
	for i := firstGroup; i < len(s); i += 3 {
		b.WriteByte(sep)
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func formatCurrency(m Micros, prefix string, thousandsSep, decimalSep byte) string {
	sign := ""
	var absM uint64
	if m < 0 {
		sign = "-"
		absM = uint64(-m)
	} else {
		absM = uint64(m)
	}

	whole := absM / uint64(MicrosPerUSD)
	rem := absM % uint64(MicrosPerUSD)
	wholeStr := formatGrouped(whole, thousandsSep)

	var decStr string
	switch {
	case rem == 0:
		decStr = "00"
	case rem%10000 == 0:
		decStr = fmt.Sprintf("%02d", rem/10000)
	default:
		decStr = strings.TrimRight(fmt.Sprintf("%06d", rem), "0")
		if len(decStr) < 2 {
			decStr = fmt.Sprintf("%02d", rem/10000)
		}
	}

	return fmt.Sprintf("%s%s%s%c%s", sign, prefix, wholeStr, decimalSep, decStr)
}

// FormatUSD returns a formatted USD string (e.g. "$1,234.56" or "-$1.00").
func FormatUSD(m Micros) string {
	return formatCurrency(m, "$", ',', '.')
}

// FormatPTBR returns a formatted representation in Brazilian Portuguese locale (e.g. "US$ 1.234,56").
func FormatPTBR(m Micros) string {
	return formatCurrency(m, "US$ ", '.', ',')
}
