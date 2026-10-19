package core_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

func TestGenerateKey_Success(t *testing.T) {
	pepper := "secret-pepper-test"
	rawKey, hash, err := core.GenerateKey(pepper)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(rawKey, core.KeyPrefix) {
		t.Errorf("expected prefix %q, got %q", core.KeyPrefix, rawKey)
	}
	if len(rawKey) != core.KeyTotalLength {
		t.Errorf("expected length %d, got %d", core.KeyTotalLength, len(rawKey))
	}
	if len(hash) != 64 {
		t.Errorf("expected 64 hex chars hash, got %d", len(hash))
	}

	if err := core.ValidateKeyFormat(rawKey); err != nil {
		t.Errorf("expected valid key format, got %v", err)
	}

	expectedHash, err := core.HashKey(rawKey, pepper)
	if err != nil {
		t.Fatalf("unexpected hash error: %v", err)
	}
	if hash != expectedHash {
		t.Errorf("expected hash %q, got %q", expectedHash, hash)
	}
}

func TestGenerateKey_EmptyPepper(t *testing.T) {
	if _, _, err := core.GenerateKey(""); !errors.Is(err, core.ErrEmptyPepper) {
		t.Errorf("expected ErrEmptyPepper, got %v", err)
	}

	validKey := "lgw_0123456789abcdef0123456789abcdef"
	if _, err := core.HashKey(validKey, ""); !errors.Is(err, core.ErrEmptyPepper) {
		t.Errorf("expected ErrEmptyPepper, got %v", err)
	}
}

func TestValidateKeyFormat_InvalidCases(t *testing.T) {
	pepper := "secret-pepper-test"
	invalidKeys := []string{
		"",
		"foo",
		"lgw_short",
		"lgw_0123456789abcdef0123456789abcdeg", // 'g' is not hex
		"LGW_0123456789abcdef0123456789abcdef", // uppercase prefix
		"lgw_0123456789ABCDEF0123456789ABCDEF", // uppercase hex
		"lgw_0123456789abcdef0123456789abcdef0000",
	}

	for _, k := range invalidKeys {
		if err := core.ValidateKeyFormat(k); !errors.Is(err, core.ErrInvalidKeyFormat) {
			t.Errorf("key %q: expected ErrInvalidKeyFormat, got %v", k, err)
		}
		if _, err := core.HashKey(k, pepper); !errors.Is(err, core.ErrInvalidKeyFormat) {
			t.Errorf("hash key %q: expected ErrInvalidKeyFormat, got %v", k, err)
		}
	}
}

func TestVirtualKey_Active(t *testing.T) {
	now := time.Date(2026, 10, 18, 22, 0, 0, 0, time.UTC)
	past := now.Add(-1 * time.Hour)
	future := now.Add(1 * time.Hour)

	vk := core.VirtualKey{
		ID:                 "k-1",
		Name:               "Default Key",
		KeyHash:            "hash1",
		AllowedModels:      []string{"claude-3-5-sonnet", "gpt-4o"},
		DailyLimitMicros:   money.Micros(10000),
		MonthlyLimitMicros: money.Micros(50000),
		RPM:                60,
		CreatedAt:          past,
		ExpiresAt:          &future,
	}

	if err := vk.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if !vk.IsActive(now) {
		t.Error("expected key to be active")
	}
	if !vk.AllowsModel("claude-3-5-sonnet") {
		t.Error("expected claude-3-5-sonnet to be allowed")
	}
	if vk.AllowsModel("other-model") {
		t.Error("expected other-model to be denied")
	}
}

func TestVirtualKey_RevokedAndExpired(t *testing.T) {
	now := time.Date(2026, 10, 18, 22, 0, 0, 0, time.UTC)
	past := now.Add(-1 * time.Hour)

	revoked := core.VirtualKey{
		ID:        "k-2",
		Name:      "Revoked Key",
		KeyHash:   "hash2",
		CreatedAt: past,
		RevokedAt: &past,
	}
	if revoked.IsActive(now) {
		t.Error("expected revoked key to be inactive")
	}

	expired := core.VirtualKey{
		ID:        "k-3",
		Name:      "Expired Key",
		KeyHash:   "hash3",
		CreatedAt: past.Add(-2 * time.Hour),
		ExpiresAt: &past,
	}
	if expired.IsActive(now) {
		t.Error("expected expired key to be inactive")
	}
}

func TestVirtualKey_WildcardAndValidation(t *testing.T) {
	past := time.Now().Add(-1 * time.Hour)
	wildcard := core.VirtualKey{
		ID:            "k-4",
		Name:          "Wildcard Key",
		KeyHash:       "hash4",
		AllowedModels: nil,
		CreatedAt:     past,
	}
	if !wildcard.AllowsModel("any-model") {
		t.Error("expected any model to be allowed when AllowedModels is nil/empty")
	}

	invalidKeys := []core.VirtualKey{
		{Name: "", KeyHash: "h"},
		{Name: "valid", KeyHash: ""},
		{Name: "valid", KeyHash: "h", DailyLimitMicros: -1},
		{Name: "valid", KeyHash: "h", MonthlyLimitMicros: -1},
		{Name: "valid", KeyHash: "h", RPM: -1},
	}
	for i, ik := range invalidKeys {
		if err := ik.Validate(); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

func TestPricingValidation(t *testing.T) {
	p := core.Pricing{
		Model:             "claude-3-5-sonnet-20241022",
		Provider:          "anthropic",
		InputPerMTok:      money.Micros(3_000_000),
		OutputPerMTok:     money.Micros(15_000_000),
		CacheReadPerMTok:  money.Micros(300_000),
		CacheWritePerMTok: money.Micros(3_750_000),
		UpdatedAt:         time.Now(),
	}

	if err := p.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	invalidPricing := []core.Pricing{
		{Model: "", Provider: "anthropic"},
		{Model: "claude", Provider: ""},
		{Model: "claude", Provider: "anthropic", InputPerMTok: -1},
		{Model: "claude", Provider: "anthropic", OutputPerMTok: -1},
		{Model: "claude", Provider: "anthropic", CacheReadPerMTok: -1},
		{Model: "claude", Provider: "anthropic", CacheWritePerMTok: -1},
	}

	for i, ip := range invalidPricing {
		if err := ip.Validate(); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

func TestLogicalModelValidation(t *testing.T) {
	lm := core.LogicalModel{
		Name:      "claude-3-5-sonnet",
		Chain:     []string{"claude-3-5-sonnet-20241022", "anthropic/claude-3-5-sonnet"},
		UpdatedAt: time.Now(),
	}

	if err := lm.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	invalidModels := []core.LogicalModel{
		{Name: "", Chain: []string{"model-1"}},
		{Name: "claude", Chain: nil},
		{Name: "claude", Chain: []string{}},
		{Name: "claude", Chain: []string{"valid", ""}},
		{Name: "claude", Chain: []string{"   "}},
	}

	for i, im := range invalidModels {
		if err := im.Validate(); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}
