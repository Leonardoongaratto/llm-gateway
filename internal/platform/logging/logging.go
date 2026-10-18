package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

var sensitiveKeys = map[string]struct{}{
	"authorization": {},
	"api_key":       {},
	"x-api-key":     {},
	"secret":        {},
	"token":         {},
	"password":      {},
	"content":       {},
	"prompt":        {},
}

func isSensitive(key string) bool {
	_, found := sensitiveKeys[strings.ToLower(key)]
	return found
}

type traceHandler struct {
	inner slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		r.AddAttrs(
			slog.String("trace_id", span.SpanContext().TraceID().String()),
			slog.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{inner: h.inner.WithGroup(name)}
}

// New creates a new slog.Logger with JSON formatting, case-insensitive sensitive key redaction,
// and automatic trace_id / span_id context injection.
func New(w io.Writer, levelStr string) *slog.Logger {
	level := parseLevel(levelStr)

	jsonHandler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if isSensitive(a.Key) {
				return slog.String(a.Key, "[REDACTED]")
			}
			return a
		},
	})

	return slog.New(&traceHandler{inner: jsonHandler})
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
