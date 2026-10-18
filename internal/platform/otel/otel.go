package otel

import (
	"context"
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Config struct {
	ServiceName  string
	OTelEndpoint string
}

type Provider struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
}

// Init sets up tracing, W3C propagation, and Prometheus metrics.
func Init(ctx context.Context, cfg Config) (*Provider, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	var tracerProvider *sdktrace.TracerProvider
	if cfg.OTelEndpoint != "" {
		exp, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(cfg.OTelEndpoint),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			return nil, err
		}
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
	} else {
		tracerProvider = sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	}
	otel.SetTracerProvider(tracerProvider)

	promExporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	return &Provider{
		tracerProvider: tracerProvider,
		meterProvider:  meterProvider,
	}, nil
}

// MetricsHandler returns an http.Handler that serves Prometheus metrics.
func (p *Provider) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// Shutdown flushes and closes tracing and metrics providers.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if p.meterProvider != nil {
		if err := p.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// HTTPHandler wraps an http.Handler with OpenTelemetry tracing and propagation.
func HTTPHandler(handler http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(handler, operation)
}

// HTTPTransport returns an http.RoundTripper with OpenTelemetry tracing and propagation.
func HTTPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}

// HTTPClient returns an http.Client equipped with OpenTelemetry tracing and propagation.
func HTTPClient(base *http.Client) *http.Client {
	if base == nil {
		base = &http.Client{}
	}
	c := *base
	c.Transport = HTTPTransport(c.Transport)
	return &c
}
