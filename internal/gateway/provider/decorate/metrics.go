package decorate

import (
	"context"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metricsDecorator struct {
	inner            provider.Provider
	requestsCounter  metric.Int64Counter
	errorsCounter    metric.Int64Counter
	latencyHistogram metric.Float64Histogram
}

// WithMetrics wraps a provider with OpenTelemetry telemetry metrics.
func WithMetrics(p provider.Provider, m metric.Meter) provider.Provider {
	if m == nil {
		m = otel.Meter("github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider")
	}

	reqCounter, _ := m.Int64Counter(
		"llm_gateway_provider_requests_total",
		metric.WithDescription("Total number of requests dispatched to upstream LLM providers"),
	)
	errCounter, _ := m.Int64Counter(
		"llm_gateway_provider_errors_total",
		metric.WithDescription("Total number of errors returned by upstream LLM providers"),
	)
	latencyHist, _ := m.Float64Histogram(
		"llm_gateway_provider_latency_seconds",
		metric.WithDescription("Upstream LLM provider roundtrip latency in seconds"),
		metric.WithUnit("s"),
	)

	return &metricsDecorator{
		inner:            p,
		requestsCounter:  reqCounter,
		errorsCounter:    errCounter,
		latencyHistogram: latencyHist,
	}
}

func (m *metricsDecorator) Name() string {
	return m.inner.Name()
}

func (m *metricsDecorator) record(ctx context.Context, model string, start time.Time, err error) {
	attrs := []attribute.KeyValue{
		attribute.String("provider", m.inner.Name()),
		attribute.String("model", model),
	}

	if m.requestsCounter != nil {
		m.requestsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.latencyHistogram != nil {
		m.latencyHistogram.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
	}
	if err != nil && m.errorsCounter != nil {
		m.errorsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

func (m *metricsDecorator) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	start := time.Now()
	resp, err := m.inner.Do(ctx, req)
	model := ""
	if req != nil {
		model = req.Model
	}
	m.record(ctx, model, start, err)
	return resp, err
}

func (m *metricsDecorator) DoStream(ctx context.Context, req *llm.Request) (<-chan llm.Chunk, error) {
	start := time.Now()
	ch, err := m.inner.DoStream(ctx, req)
	model := ""
	if req != nil {
		model = req.Model
	}
	m.record(ctx, model, start, err)
	return ch, err
}
