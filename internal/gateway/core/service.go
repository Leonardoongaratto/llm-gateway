package core

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/auth"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/cache"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/router"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BudgetClient abstracts the gRPC budget service client methods needed by the gateway.
type BudgetClient interface {
	Reserve(ctx context.Context, keyID string, maxMicros int64, requestID string) (reservationID string, remainingMicros int64, err error)
	SettleAsync(reservationID string, actualMicros int64)
	Release(ctx context.Context, reservationID string) error
}

// EventPublisher abstracts the asynchronous NATS JetStream event publisher for the gateway.
type EventPublisher interface {
	PublishUsage(ctx context.Context, evt *eventsv1.UsageRecorded)
	PublishFallback(ctx context.Context, evt *eventsv1.ProviderFallback)
}

// ServiceConfig holds the dependencies required to instantiate core.Service.
type ServiceConfig struct {
	AuthManager    *auth.Manager
	BudgetClient   BudgetClient
	EventPublisher EventPublisher
	LocalCache     *cache.Cache
	Router         *router.Router
	Logger         *slog.Logger
}

// CompletionResult contains the response, provider metadata, and cost metrics for a chat completion.
type CompletionResult struct {
	Response              *llm.Response
	Provider              string
	Model                 string
	CacheHit              bool
	CostMicros            money.Micros
	RemainingBudgetMicros int64
	ReservationID         string
}

// StreamResult contains the stream channel and metadata for a streaming chat completion.
type StreamResult struct {
	Stream                <-chan llm.Chunk
	Provider              string
	Model                 string
	ReservationID         string
	RemainingBudgetMicros int64
}

// Service is the central orchestrator for authentication, budget reservations, caching, routing, and event recording.
type Service struct {
	authMgr        *auth.Manager
	budgetClient   BudgetClient
	eventPublisher EventPublisher
	localCache     *cache.Cache
	router         *router.Router
	logger         *slog.Logger
}

// NewService creates a new gateway core.Service orchestrator.
func NewService(cfg ServiceConfig) *Service {
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
	}

	return &Service{
		authMgr:        cfg.AuthManager,
		budgetClient:   cfg.BudgetClient,
		eventPublisher: cfg.EventPublisher,
		localCache:     cfg.LocalCache,
		router:         cfg.Router,
		logger:         l,
	}
}

func (s *Service) prepareRequest(rawKey string, req *llm.Request) (*auth.KeyInfo, error) {
	keyInfo, err := s.authMgr.Authenticate(rawKey)
	if err != nil {
		return nil, err
	}
	if err := s.authMgr.AuthorizeModel(keyInfo, req.Model); err != nil {
		return nil, err
	}
	req.KeyID = keyInfo.ID
	if req.ID == "" {
		req.ID = "req-" + uuid.New().String()
	}
	return keyInfo, nil
}

func (s *Service) checkCache(ctx context.Context, keyID string, req *llm.Request, temperature float64) (*CompletionResult, string, bool) {
	if s.localCache == nil || !cache.IsEligible(temperature, req.Stream) {
		return nil, "", false
	}
	cacheKey := cache.ComputeKey(keyID, req)
	cachedResp, hit := s.localCache.Get(cacheKey)
	if !hit {
		return nil, cacheKey, false
	}
	s.publishUsage(ctx, req.ID, keyID, req.Model, cachedResp.Model, cachedResp.Provider, "", cachedResp.Usage, 0, 0, true)
	return &CompletionResult{
		Response:   cachedResp,
		Provider:   cachedResp.Provider,
		Model:      cachedResp.Model,
		CacheHit:   true,
		CostMicros: 0,
	}, cacheKey, true
}

func (s *Service) estimateAndReserve(ctx context.Context, snap *auth.Snapshot, keyID string, req *llm.Request) (*auth.PricingInfo, money.Micros, string, int64, error) {
	chain := snap.ResolveChain(req.Model)
	targetModel := req.Model
	if len(chain) > 0 {
		targetModel = chain[0]
	}

	pricing := snap.FindPricing(targetModel, "")
	if pricing == nil {
		pricing = snap.FindPricing(req.Model, "")
	}

	promptTokens := req.PromptTokens
	if promptTokens <= 0 {
		promptTokens = EstimatePromptTokens(req.Messages)
	}

	maxCost, err := EstimateMaxCost(pricing, promptTokens, req.MaxTokens)
	if err != nil {
		return nil, 0, "", 0, fmt.Errorf("failed to estimate max cost: %w", err)
	}

	if s.budgetClient == nil {
		return pricing, maxCost, "", 0, nil
	}

	resID, rem, err := s.budgetClient.Reserve(ctx, keyID, int64(maxCost), req.ID)
	if err != nil {
		return nil, 0, "", 0, err
	}
	return pricing, maxCost, resID, rem, nil
}

func (s *Service) publishUsage(ctx context.Context, reqID, keyID, modelReq, modelUsed, providerUsed, fallbackFrom string, usage llm.Usage, cost int64, latencyMs int64, cacheHit bool) {
	if s.eventPublisher == nil {
		return
	}
	s.eventPublisher.PublishUsage(ctx, &eventsv1.UsageRecorded{
		RequestId:        reqID,
		OccurredAt:       timestamppb.Now(),
		Source:           string(llm.SourceGateway),
		CostKind:         string(llm.CostKindBilled),
		KeyId:            keyID,
		ModelRequested:   modelReq,
		ModelUsed:        modelUsed,
		Provider:         providerUsed,
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		CacheReadTokens:  usage.CacheReadTokens,
		CacheWriteTokens: usage.CacheWriteTokens,
		CostMicros:       cost,
		LatencyMs:        latencyMs,
		Status:           "success",
		CacheHit:         cacheHit,
		FallbackFrom:     fallbackFrom,
	})
}

// ChatCompletion handles a non-streaming chat completion request through the full gateway pipeline.
func (s *Service) ChatCompletion(ctx context.Context, rawKey string, req *llm.Request, temperature float64) (*CompletionResult, error) {
	keyInfo, err := s.prepareRequest(rawKey, req)
	if err != nil {
		return nil, err
	}

	if cachedRes, _, hit := s.checkCache(ctx, keyInfo.ID, req, temperature); hit {
		return cachedRes, nil
	}

	snap := s.authMgr.GetSnapshot()
	pricing, maxCost, reservationID, remainingMicros, err := s.estimateAndReserve(ctx, snap, keyInfo.ID, req)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	execResult, execErr := s.router.Execute(ctx, snap, req)
	if execErr != nil {
		if reservationID != "" && s.budgetClient != nil {
			_ = s.budgetClient.Release(ctx, reservationID)
		}
		return nil, execErr
	}
	latencyMs := time.Since(startTime).Milliseconds()

	actualPricing := snap.FindPricing(execResult.ModelUsed, execResult.ProviderUsed)
	if actualPricing == nil {
		actualPricing = pricing
	}
	actualCost, err := CalculateCost(actualPricing, execResult.Response.Usage)
	if err != nil {
		actualCost = maxCost
	}

	if reservationID != "" && s.budgetClient != nil {
		s.budgetClient.SettleAsync(reservationID, int64(actualCost))
	}

	if s.localCache != nil && cache.IsEligible(temperature, req.Stream) {
		cacheKey := cache.ComputeKey(keyInfo.ID, req)
		s.localCache.Set(cacheKey, execResult.Response)
	}

	s.publishUsage(ctx, req.ID, keyInfo.ID, req.Model, execResult.ModelUsed, execResult.ProviderUsed, execResult.FallbackFrom, execResult.Response.Usage, int64(actualCost), latencyMs, false)

	return &CompletionResult{
		Response:              execResult.Response,
		Provider:              execResult.ProviderUsed,
		Model:                 execResult.ModelUsed,
		CacheHit:              false,
		CostMicros:            actualCost,
		RemainingBudgetMicros: remainingMicros,
		ReservationID:         reservationID,
	}, nil
}

func (s *Service) finalizeStream(reqID, keyID, modelReq, reservationID string, initialPricing *auth.PricingInfo, maxCost money.Micros, startTime time.Time, snap *auth.Snapshot, streamExec *router.StreamExecutionResult, finalUsage *llm.Usage) {
	latencyMs := time.Since(startTime).Milliseconds()
	if finalUsage != nil {
		actualPricing := snap.FindPricing(streamExec.ModelUsed, streamExec.ProviderUsed)
		if actualPricing == nil {
			actualPricing = initialPricing
		}
		actualCost, costErr := CalculateCost(actualPricing, *finalUsage)
		if costErr != nil {
			actualCost = maxCost
		}
		if reservationID != "" && s.budgetClient != nil {
			s.budgetClient.SettleAsync(reservationID, int64(actualCost))
		}
		s.publishUsage(context.Background(), reqID, keyID, modelReq, streamExec.ModelUsed, streamExec.ProviderUsed, streamExec.FallbackFrom, *finalUsage, int64(actualCost), latencyMs, false)
		return
	}

	if reservationID != "" && s.budgetClient != nil {
		_ = s.budgetClient.Release(context.Background(), reservationID)
	}
}

func forwardStream(ctx context.Context, in <-chan llm.Chunk, out chan<- llm.Chunk) *llm.Usage {
	var finalUsage *llm.Usage
	for {
		select {
		case <-ctx.Done():
			return finalUsage
		case chunk, ok := <-in:
			if !ok {
				return finalUsage
			}
			if chunk.Usage != nil {
				finalUsage = chunk.Usage
			}
			select {
			case <-ctx.Done():
				return finalUsage
			case out <- chunk:
			}
		}
	}
}

func (s *Service) pipeStream(ctx context.Context, reqID, keyID, modelReq string, reservationID string, initialPricing *auth.PricingInfo, maxCost money.Micros, startTime time.Time, snap *auth.Snapshot, streamExec *router.StreamExecutionResult, outChan chan<- llm.Chunk) {
	defer close(outChan)
	finalUsage := forwardStream(ctx, streamExec.Stream, outChan)
	s.finalizeStream(reqID, keyID, modelReq, reservationID, initialPricing, maxCost, startTime, snap, streamExec, finalUsage)
}

// ChatCompletionStream handles a streaming chat completion request through the gateway pipeline.
func (s *Service) ChatCompletionStream(ctx context.Context, rawKey string, req *llm.Request) (*StreamResult, error) {
	keyInfo, err := s.prepareRequest(rawKey, req)
	if err != nil {
		return nil, err
	}
	req.Stream = true

	snap := s.authMgr.GetSnapshot()
	pricing, maxCost, reservationID, remainingMicros, err := s.estimateAndReserve(ctx, snap, keyInfo.ID, req)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	streamExec, err := s.router.ExecuteStream(ctx, snap, req)
	if err != nil {
		if reservationID != "" && s.budgetClient != nil {
			_ = s.budgetClient.Release(ctx, reservationID)
		}
		return nil, err
	}

	outChan := make(chan llm.Chunk, 64)
	go s.pipeStream(ctx, req.ID, keyInfo.ID, req.Model, reservationID, pricing, maxCost, startTime, snap, streamExec, outChan)

	return &StreamResult{
		Stream:                outChan,
		Provider:              streamExec.ProviderUsed,
		Model:                 streamExec.ModelUsed,
		ReservationID:         reservationID,
		RemainingBudgetMicros: remainingMicros,
	}, nil
}
