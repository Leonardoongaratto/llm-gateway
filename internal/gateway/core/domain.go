package core

import (
	"context"
	"strings"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// KeyInfo contains authenticated metadata for a virtual key.
type KeyInfo struct {
	ID                 string
	Name               string
	KeyHash            string
	AllowedModels      []string
	Active             bool
	ExpiresAt          *time.Time
	DailyLimitMicros   int64
	MonthlyLimitMicros int64
	RPM                int32
}

// IsExpired checks whether the key is expired at the given time.
func (k *KeyInfo) IsExpired(now time.Time) bool {
	if k == nil {
		return true
	}
	if k.ExpiresAt == nil {
		return false
	}
	return k.ExpiresAt.Before(now)
}

// IsModelAllowed checks if the given model is permitted for this key.
func (k *KeyInfo) IsModelAllowed(model string) bool {
	if k == nil {
		return false
	}
	if len(k.AllowedModels) == 0 {
		return true
	}
	for _, m := range k.AllowedModels {
		if strings.EqualFold(m, model) {
			return true
		}
	}
	return false
}

// PricingInfo represents pricing for a model/provider combination.
type PricingInfo struct {
	Model                   string
	Provider                string
	InputPerMTokMicros      int64
	OutputPerMTokMicros     int64
	CacheReadPerMTokMicros  int64
	CacheWritePerMTokMicros int64
}

// Snapshot provides access to cached routing chains and model pricing.
type Snapshot interface {
	ResolveChain(model string) []string
	FindPricing(model, provider string) *PricingInfo
}

// AuthManager abstracts key authentication and authorization.
type AuthManager interface {
	Authenticate(rawKey string) (*KeyInfo, error)
	AuthorizeModel(keyInfo *KeyInfo, model string) error
	GetSnapshot() Snapshot
}

// ExecutionResult contains the response and routing metadata for a non-streaming call.
type ExecutionResult struct {
	Response     *llm.Response
	ProviderUsed string
	ModelUsed    string
	FallbackFrom string
}

// StreamExecutionResult contains the stream channel and routing metadata for a streaming call.
type StreamExecutionResult struct {
	Stream       <-chan llm.Chunk
	ProviderUsed string
	ModelUsed    string
	FallbackFrom string
}

// Router dispatches requests to providers resolving logical models and performing selective fallback.
type Router interface {
	Execute(ctx context.Context, snap Snapshot, req *llm.Request) (*ExecutionResult, error)
	ExecuteStream(ctx context.Context, snap Snapshot, req *llm.Request) (*StreamExecutionResult, error)
}

// BudgetClient abstracts the budget service client operations needed by the gateway.
type BudgetClient interface {
	Reserve(ctx context.Context, keyID string, maxMicros int64, requestID string) (reservationID string, remainingMicros int64, err error)
	SettleAsync(ctx context.Context, reservationID string, actualMicros int64)
	Release(ctx context.Context, reservationID string) error
}

// EventPublisher abstracts asynchronous event publishing for the gateway.
type EventPublisher interface {
	PublishUsage(ctx context.Context, evt *eventsv1.UsageRecorded)
	PublishFallback(ctx context.Context, evt *eventsv1.ProviderFallback)
}
