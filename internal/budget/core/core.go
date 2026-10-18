package core

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrBudgetExceeded    = errors.New("budget exceeded")
	ErrRateLimited       = errors.New("rate limited")
	ErrNotFound          = errors.New("not found")
	ErrReservationClosed = errors.New("reservation already settled or released")
	ErrNegativeAmount    = errors.New("amount must not be negative")
	ErrOverflow          = errors.New("calculation overflow")
)

type KeyLimits struct {
	KeyID              string
	DailyLimitMicros   int64
	MonthlyLimitMicros int64
	RPM                int32
}

func DayPeriod(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

func MonthPeriod(t time.Time) string {
	return t.UTC().Format("2006-01")
}

func DaysInMonth(t time.Time) int {
	utc := t.UTC()
	return time.Date(utc.Year(), utc.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func ProjectedMonthlySpend(spentSoFar int64, now time.Time) (int64, error) {
	if spentSoFar < 0 {
		return 0, ErrNegativeAmount
	}
	if spentSoFar == 0 {
		return 0, nil
	}

	daysTotal := int64(DaysInMonth(now))
	daysElapsed := int64(now.UTC().Day())
	if daysElapsed < 1 {
		daysElapsed = 1
	}

	if spentSoFar > math.MaxInt64/daysTotal {
		return 0, ErrOverflow
	}

	projected := (spentSoFar * daysTotal) / daysElapsed
	return projected, nil
}

func CheckLimits(limits KeyLimits, currentDailySpend, currentDailyReserved, currentMonthlySpend, currentMonthlyReserved, requestedMicros int64) error {
	if requestedMicros < 0 {
		return fmt.Errorf("%w: requested micros %d", ErrNegativeAmount, requestedMicros)
	}

	if limits.DailyLimitMicros > 0 {
		if currentDailySpend+currentDailyReserved+requestedMicros > limits.DailyLimitMicros {
			return ErrBudgetExceeded
		}
	}

	if limits.MonthlyLimitMicros > 0 {
		if currentMonthlySpend+currentMonthlyReserved+requestedMicros > limits.MonthlyLimitMicros {
			return ErrBudgetExceeded
		}
	}

	return nil
}

type SlidingWindowRateLimiter struct {
	mu      sync.Mutex
	history map[string][]time.Time
}

func NewSlidingWindowRateLimiter() *SlidingWindowRateLimiter {
	return &SlidingWindowRateLimiter{
		history: make(map[string][]time.Time),
	}
}

func (rl *SlidingWindowRateLimiter) Allow(keyID string, rpm int32, now time.Time) (bool, time.Duration) {
	if rpm <= 0 {
		return true, 0
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	windowStart := now.Add(-1 * time.Minute)
	timestamps := rl.history[keyID]

	validIdx := 0
	for i, ts := range timestamps {
		if ts.After(windowStart) {
			validIdx = i
			break
		}
		if i == len(timestamps)-1 {
			validIdx = len(timestamps)
		}
	}
	timestamps = timestamps[validIdx:]

	if int32(len(timestamps)) >= rpm {
		oldestInWindow := timestamps[0]
		retryAfter := oldestInWindow.Add(time.Minute).Sub(now)
		if retryAfter <= 0 {
			retryAfter = time.Millisecond
		}
		rl.history[keyID] = timestamps
		return false, retryAfter
	}

	timestamps = append(timestamps, now)
	rl.history[keyID] = timestamps
	return true, 0
}
