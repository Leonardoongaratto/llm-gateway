package core

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
)

const (
	KeyPrefix      = "lgw_"
	KeyRandomBytes = 16
	KeyHexLength   = KeyRandomBytes * 2
	KeyTotalLength = len(KeyPrefix) + KeyHexLength
)

var (
	ErrEmptyPepper         = errors.New("key pepper must not be empty")
	ErrInvalidKeyFormat    = errors.New("invalid virtual key format")
	ErrKeyNotFound         = errors.New("virtual key not found")
	ErrKeyRevoked          = errors.New("virtual key is revoked")
	ErrInvalidVirtualKey   = errors.New("invalid virtual key data")
	ErrInvalidPricing      = errors.New("invalid pricing data")
	ErrInvalidLogicalModel = errors.New("invalid logical model data")
)

// ValidateKeyFormat ensures the key conforms to lgw_<32 hex chars>.
func ValidateKeyFormat(rawKey string) error {
	if len(rawKey) != KeyTotalLength || !strings.HasPrefix(rawKey, KeyPrefix) {
		return ErrInvalidKeyFormat
	}
	hexPart := rawKey[len(KeyPrefix):]
	for i := 0; i < len(hexPart); i++ {
		c := hexPart[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ErrInvalidKeyFormat
		}
	}
	return nil
}

// HashKey computes SHA-256 of rawKey with pepper and returns a hex string.
func HashKey(rawKey, pepper string) (string, error) {
	if pepper == "" {
		return "", ErrEmptyPepper
	}
	if err := ValidateKeyFormat(rawKey); err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(rawKey + pepper))
	return hex.EncodeToString(h[:]), nil
}

// GenerateKey generates a new virtual key with lgw_ prefix and 32 random hex characters,
// and returns both the raw key and its peppered SHA-256 hash.
func GenerateKey(pepper string) (string, string, error) {
	if pepper == "" {
		return "", "", ErrEmptyPepper
	}
	var buf [KeyRandomBytes]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", "", err
	}
	rawKey := KeyPrefix + hex.EncodeToString(buf[:])
	hash, err := HashKey(rawKey, pepper)
	if err != nil {
		return "", "", err
	}
	return rawKey, hash, nil
}

// VirtualKey represents an authenticated virtual client key.
type VirtualKey struct {
	ID                 string
	Name               string
	KeyHash            string
	AllowedModels      []string
	DailyLimitMicros   money.Micros
	MonthlyLimitMicros money.Micros
	RPM                int32
	CreatedAt          time.Time
	ExpiresAt          *time.Time
	RevokedAt          *time.Time
}

// IsActive returns whether the key is currently usable at the given timestamp.
func (k *VirtualKey) IsActive(at time.Time) bool {
	if k.RevokedAt != nil {
		return false
	}
	if k.ExpiresAt != nil && !at.Before(*k.ExpiresAt) {
		return false
	}
	return true
}

// AllowsModel returns true if the key has access to the specified model.
func (k *VirtualKey) AllowsModel(model string) bool {
	if len(k.AllowedModels) == 0 {
		return true
	}
	for _, m := range k.AllowedModels {
		if m == model {
			return true
		}
	}
	return false
}

// Validate checks the structural constraints of the VirtualKey.
func (k *VirtualKey) Validate() error {
	if strings.TrimSpace(k.Name) == "" {
		return ErrInvalidVirtualKey
	}
	if strings.TrimSpace(k.KeyHash) == "" {
		return ErrInvalidVirtualKey
	}
	if k.DailyLimitMicros < 0 || k.MonthlyLimitMicros < 0 || k.RPM < 0 {
		return ErrInvalidVirtualKey
	}
	return nil
}

// Pricing defines token rates per million for a provider model.
type Pricing struct {
	Model             string
	Provider          string
	InputPerMTok      money.Micros
	OutputPerMTok     money.Micros
	CacheReadPerMTok  money.Micros
	CacheWritePerMTok money.Micros
	UpdatedAt         time.Time
}

// Validate checks the pricing rates and model names.
func (p *Pricing) Validate() error {
	if strings.TrimSpace(p.Model) == "" || strings.TrimSpace(p.Provider) == "" {
		return ErrInvalidPricing
	}
	if p.InputPerMTok < 0 || p.OutputPerMTok < 0 || p.CacheReadPerMTok < 0 || p.CacheWritePerMTok < 0 {
		return ErrInvalidPricing
	}
	return nil
}

// LogicalModel maps an alias name to a prioritized chain of physical models.
type LogicalModel struct {
	Name      string
	Chain     []string
	UpdatedAt time.Time
}

// Validate verifies logical model structure and non-empty fallback chains.
func (lm *LogicalModel) Validate() error {
	if strings.TrimSpace(lm.Name) == "" || len(lm.Chain) == 0 {
		return ErrInvalidLogicalModel
	}
	for _, m := range lm.Chain {
		if strings.TrimSpace(m) == "" {
			return ErrInvalidLogicalModel
		}
	}
	return nil
}

// Snapshot contains the entire active control configuration state.
type Snapshot struct {
	Version       int64
	Keys          []VirtualKey
	Pricing       []Pricing
	LogicalModels []LogicalModel
}
