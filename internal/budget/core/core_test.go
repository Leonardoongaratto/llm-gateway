package core_test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

func TestPeriods_Formatting(t *testing.T) {
	utc := time.FixedZone("UTC", 0)
	ts := time.Date(2026, time.October, 18, 14, 30, 0, 0, utc)

	if got := core.DayPeriod(ts); got != "2026-10-18" {
		t.Errorf("expected 2026-10-18, got %s", got)
	}

	if got := core.MonthPeriod(ts); got != "2026-10" {
		t.Errorf("expected 2026-10, got %s", got)
	}
}

func TestDaysInMonth(t *testing.T) {
	cases := []struct {
		year     int
		month    time.Month
		expected int
	}{
		{2024, time.February, 29},
		{2025, time.February, 28},
		{2026, time.January, 31},
		{2026, time.April, 30},
		{2026, time.October, 31},
	}

	for _, tc := range cases {
		ts := time.Date(tc.year, tc.month, 10, 0, 0, 0, 0, time.UTC)
		if got := core.DaysInMonth(ts); got != tc.expected {
			t.Errorf("year %d month %s: expected %d days, got %d", tc.year, tc.month, tc.expected, got)
		}
	}
}

func TestProjectedMonthlySpend_Normal(t *testing.T) {
	cases := []struct {
		name     string
		spent    money.Micros
		ts       time.Time
		expected money.Micros
	}{
		{
			name:     "mid month projection in 30-day month",
			spent:    1500,
			ts:       time.Date(2026, time.April, 15, 12, 0, 0, 0, time.UTC),
			expected: 3000,
		},
		{
			name:     "day 1 of 31-day month",
			spent:    100,
			ts:       time.Date(2026, time.January, 1, 10, 0, 0, 0, time.UTC),
			expected: 3100,
		},
		{
			name:     "zero spend",
			spent:    0,
			ts:       time.Date(2026, time.January, 10, 0, 0, 0, 0, time.UTC),
			expected: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proj, err := core.ProjectedMonthlySpend(tc.spent, tc.ts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if proj != tc.expected {
				t.Errorf("expected %d, got %d", tc.expected, proj)
			}
		})
	}
}

func TestProjectedMonthlySpend_Errors(t *testing.T) {
	ts := time.Date(2026, time.January, 10, 0, 0, 0, 0, time.UTC)

	if _, err := core.ProjectedMonthlySpend(money.Micros(-10), ts); err == nil {
		t.Fatal("expected error for negative spend, got nil")
	}

	if _, err := core.ProjectedMonthlySpend(money.Micros(math.MaxInt64/2), ts); err == nil {
		t.Fatal("expected overflow error, got nil")
	}
}

func TestCheckLimits_Allowed(t *testing.T) {
	limits := core.KeyLimits{
		KeyID:              "key-1",
		DailyLimitMicros:   1000,
		MonthlyLimitMicros: 10000,
		RPM:                60,
	}

	if err := core.CheckLimits(limits, 200, 100, 2000, 500, 300); err != nil {
		t.Fatalf("expected nil error within limits, got: %v", err)
	}

	unlimited := core.KeyLimits{KeyID: "key-2"}
	if err := core.CheckLimits(unlimited, 50000, 10000, 500000, 100000, 20000); err != nil {
		t.Fatalf("expected nil error for unlimited, got: %v", err)
	}
}

func TestCheckLimits_Errors(t *testing.T) {
	limits := core.KeyLimits{
		KeyID:              "key-1",
		DailyLimitMicros:   1000,
		MonthlyLimitMicros: 10000,
		RPM:                60,
	}

	err := core.CheckLimits(limits, 800, 100, 2000, 500, 200)
	if !errors.Is(err, core.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded for daily limit, got: %v", err)
	}

	err = core.CheckLimits(limits, 100, 50, 9000, 500, 600)
	if !errors.Is(err, core.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded for monthly limit, got: %v", err)
	}

	if err = core.CheckLimits(limits, 0, 0, 0, 0, -50); err == nil {
		t.Fatal("expected error for negative requested micros, got nil")
	}
}

func TestSlidingWindowRateLimiter(t *testing.T) {
	rl := core.NewSlidingWindowRateLimiter()
	now := time.Now()

	for i := 0; i < 3; i++ {
		allowed, retryAfter := rl.Allow("key-1", 3, now.Add(time.Duration(i)*time.Second))
		if !allowed {
			t.Fatalf("call %d should be allowed", i+1)
		}
		if retryAfter != 0 {
			t.Errorf("expected retryAfter 0, got %v", retryAfter)
		}
	}

	allowed, retryAfter := rl.Allow("key-1", 3, now.Add(10*time.Second))
	if allowed {
		t.Fatal("4th call should be rate limited")
	}
	if retryAfter <= 0 {
		t.Errorf("expected positive retryAfter, got %v", retryAfter)
	}

	allowedOther, _ := rl.Allow("key-2", 3, now.Add(10*time.Second))
	if !allowedOther {
		t.Fatal("different key should not be rate limited")
	}

	allowedAfter, _ := rl.Allow("key-1", 3, now.Add(62*time.Second))
	if !allowedAfter {
		t.Fatal("call after 62s should be allowed after old calls expired")
	}
}

func TestSlidingWindowRateLimiter_Concurrent(t *testing.T) {
	rl := core.NewSlidingWindowRateLimiter()
	now := time.Now()

	var wg sync.WaitGroup
	var allowedCount int64
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, _ := rl.Allow("concurrent-key", 20, now)
			if allowed {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowedCount != 20 {
		t.Errorf("expected exactly 20 allowed requests under concurrent load, got %d", allowedCount)
	}
}
