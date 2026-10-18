package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/logging"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// AC-1A-06: Given um log com atributos authorization, api_key e prompt (inclusive aninhados em grupos),
// when emitido, then os valores aparecem como [REDACTED], e um span ativo no contexto inclui trace_id.
func TestLogging_RedactionAndTraceInjection_AC1A06(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, "info")

	// Set up an in-memory OTel tracer to produce valid spans
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)
	tracer := tp.Tracer("test-logger")

	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	expectedTraceID := span.SpanContext().TraceID().String()
	expectedSpanID := span.SpanContext().SpanID().String()

	logger.InfoContext(ctx, "inbound request processed",
		slog.String("authorization", "Bearer super-secret-jwt"),
		slog.String("api_key", "sk-ant-api03-12345"),
		slog.String("prompt", "What is the secret recipe?"),
		slog.Group("headers",
			slog.String("x-api-key", "nested-key-val"),
			slog.String("password", "p@ssword123"),
			slog.String("content", "sensitive message content"),
			slog.String("public_header", "safe-value"),
		),
	)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to unmarshal log JSON: %v\nOutput: %s", err, buf.String())
	}

	// Verify trace context injection
	if entry["trace_id"] != expectedTraceID {
		t.Errorf("expected trace_id=%s, got %v", expectedTraceID, entry["trace_id"])
	}
	if entry["span_id"] != expectedSpanID {
		t.Errorf("expected span_id=%s, got %v", expectedSpanID, entry["span_id"])
	}

	// Verify top-level redaction
	if entry["authorization"] != "[REDACTED]" {
		t.Errorf("expected authorization to be redacted, got %v", entry["authorization"])
	}
	if entry["api_key"] != "[REDACTED]" {
		t.Errorf("expected api_key to be redacted, got %v", entry["api_key"])
	}
	if entry["prompt"] != "[REDACTED]" {
		t.Errorf("expected prompt to be redacted, got %v", entry["prompt"])
	}

	// Verify nested group redaction
	headers, ok := entry["headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected headers group in log, got %v", entry["headers"])
	}
	if headers["x-api-key"] != "[REDACTED]" {
		t.Errorf("expected headers.x-api-key to be redacted, got %v", headers["x-api-key"])
	}
	if headers["password"] != "[REDACTED]" {
		t.Errorf("expected headers.password to be redacted, got %v", headers["password"])
	}
	if headers["content"] != "[REDACTED]" {
		t.Errorf("expected headers.content to be redacted, got %v", headers["content"])
	}
	if headers["public_header"] != "safe-value" {
		t.Errorf("expected headers.public_header to remain safe-value, got %v", headers["public_header"])
	}
}

func TestLogging_HandlerMethodsAndLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, "debug")

	if !logger.Handler().Enabled(context.Background(), slog.LevelDebug) {
		t.Error("expected debug level to be enabled")
	}

	subLogger := logger.WithGroup("mygroup").With("extra", "value")
	subLogger.DebugContext(context.Background(), "debug msg", slog.String("password", "secret"))

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log json: %v", err)
	}

	levels := []string{"warn", "warning", "error", "unknown"}
	for _, lvl := range levels {
		var b bytes.Buffer
		l := logging.New(&b, lvl)
		l.Info("test")
	}
}
