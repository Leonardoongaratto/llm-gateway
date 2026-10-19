package auth

import (
	"strings"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/gen/lgw/control/v1"
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
// An empty or nil AllowedModels list permits any model.
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

// Snapshot holds an immutable, in-memory view of the system configuration.
type Snapshot struct {
	Version       int64
	KeysByHash    map[string]*KeyInfo
	Pricing       map[string]*PricingInfo // indexed by "model:provider" and "model"
	LogicalModels map[string][]string     // logical model name -> chain of target models/providers
}

// FindKeyByHash returns the key matching the given SHA-256 peppered hash, or nil if not found.
func (s *Snapshot) FindKeyByHash(hash string) *KeyInfo {
	if s == nil || s.KeysByHash == nil {
		return nil
	}
	return s.KeysByHash[hash]
}

// FindPricing returns pricing for the model and provider.
func (s *Snapshot) FindPricing(model, provider string) *PricingInfo {
	if s == nil || s.Pricing == nil {
		return nil
	}
	if provider != "" {
		if p, ok := s.Pricing[strings.ToLower(model)+":"+strings.ToLower(provider)]; ok {
			return p
		}
	}
	if p, ok := s.Pricing[strings.ToLower(model)]; ok {
		return p
	}
	return nil
}

// ResolveChain returns the provider/model chain for a logical model, or [model] if direct.
func (s *Snapshot) ResolveChain(model string) []string {
	if s == nil || s.LogicalModels == nil {
		return []string{model}
	}
	if chain, ok := s.LogicalModels[strings.ToLower(model)]; ok && len(chain) > 0 {
		return chain
	}
	return []string{model}
}

func populateKeys(snap *Snapshot, keys []*controlv1.KeyInfo) {
	for _, k := range keys {
		if k == nil {
			continue
		}
		var exp *time.Time
		if k.ExpiresAt != nil {
			t := k.ExpiresAt.AsTime()
			exp = &t
		}
		snap.KeysByHash[k.KeyHash] = &KeyInfo{
			ID:                 k.Id,
			Name:               k.Name,
			KeyHash:            k.KeyHash,
			AllowedModels:      k.AllowedModels,
			Active:             k.Active,
			ExpiresAt:          exp,
			DailyLimitMicros:   k.DailyLimitMicros,
			MonthlyLimitMicros: k.MonthlyLimitMicros,
			RPM:                k.Rpm,
		}
	}
}

func populatePricing(snap *Snapshot, pricing []*controlv1.PricingInfo) {
	for _, pr := range pricing {
		if pr == nil {
			continue
		}
		info := &PricingInfo{
			Model:                   pr.Model,
			Provider:                pr.Provider,
			InputPerMTokMicros:      pr.InputPerMtokMicros,
			OutputPerMTokMicros:     pr.OutputPerMtokMicros,
			CacheReadPerMTokMicros:  pr.CacheReadPerMtokMicros,
			CacheWritePerMTokMicros: pr.CacheWritePerMtokMicros,
		}
		mKey := strings.ToLower(pr.Model)
		pKey := strings.ToLower(pr.Provider)
		if pKey != "" {
			snap.Pricing[mKey+":"+pKey] = info
		}
		if _, exists := snap.Pricing[mKey]; !exists {
			snap.Pricing[mKey] = info
		}
	}
}

func populateLogicalModels(snap *Snapshot, models []*controlv1.LogicalModelInfo) {
	for _, lm := range models {
		if lm == nil {
			continue
		}
		snap.LogicalModels[strings.ToLower(lm.Name)] = lm.Chain
	}
}

// FromProto converts a protobuf Snapshot to the internal Snapshot representation.
func FromProto(p *controlv1.Snapshot) *Snapshot {
	if p == nil {
		return &Snapshot{
			KeysByHash:    make(map[string]*KeyInfo),
			Pricing:       make(map[string]*PricingInfo),
			LogicalModels: make(map[string][]string),
		}
	}

	snap := &Snapshot{
		Version:       p.Version,
		KeysByHash:    make(map[string]*KeyInfo, len(p.Keys)),
		Pricing:       make(map[string]*PricingInfo, len(p.Pricing)*2),
		LogicalModels: make(map[string][]string, len(p.LogicalModels)),
	}

	populateKeys(snap, p.Keys)
	populatePricing(snap, p.Pricing)
	populateLogicalModels(snap, p.LogicalModels)

	return snap
}
