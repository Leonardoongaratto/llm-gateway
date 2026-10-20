package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/devserver"
	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type traceCapturingProvider struct {
	mu              sync.Mutex
	capturedTraceID string
}

func (p *traceCapturingProvider) Name() string { return "openai" }

func (p *traceCapturingProvider) Do(ctx context.Context, req *llm.Request) (*llm.Response, error) {
	spanCtx := trace.SpanContextFromContext(ctx)
	p.mu.Lock()
	p.capturedTraceID = spanCtx.TraceID().String()
	p.mu.Unlock()

	return &llm.Response{
		ID:      "resp-trace",
		Model:   req.Model,
		Content: "Trace test response",
		Usage:   llm.Usage{InputTokens: 10, OutputTokens: 10},
	}, nil
}

func (p *traceCapturingProvider) DoStream(_ context.Context, _ *llm.Request) (<-chan llm.Chunk, error) {
	ch := make(chan llm.Chunk, 1)
	ch <- llm.Chunk{Content: "Stream trace response"}
	close(ch)
	return ch, nil
}

func (p *traceCapturingProvider) TraceID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.capturedTraceID
}

func subscribeNATSUsageTrace(t *testing.T, js jetstream.JetStream) (<-chan string, func()) {
	traceChan := make(chan string, 1)
	consumer, err := js.CreateOrUpdateConsumer(context.Background(), "LGW_USAGE", jetstream.ConsumerConfig{
		FilterSubject: "lgw.usage.recorded.v1",
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckNonePolicy,
	})
	if err != nil {
		t.Fatalf("failed to create test watcher consumer: %v", err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if headers := msg.Headers(); headers != nil {
			traceChan <- headers.Get("Traceparent")
		}
	})
	if err != nil {
		t.Fatalf("failed to start consuming for trace test: %v", err)
	}
	return traceChan, consumeCtx.Stop
}

func collectTraceSpans(recorder *tracetest.SpanRecorder, traceID string) map[string]bool {
	spanNames := make(map[string]bool)
	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID().String() == traceID {
			spanNames[s.Name()] = true
		}
	}
	return spanNames
}

func hasAllSpans(spanNames map[string]bool, required []string) bool {
	for _, reqSpan := range required {
		if !spanNames[reqSpan] {
			return false
		}
	}
	return true
}

func verifySpans(t *testing.T, recorder *tracetest.SpanRecorder, knownTraceID string, requiredSpans []string) {
	deadline := time.Now().Add(3 * time.Second)
	var spanNames map[string]bool
	for time.Now().Before(deadline) {
		spanNames = collectTraceSpans(recorder, knownTraceID)
		if hasAllSpans(spanNames, requiredSpans) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	for _, reqSpan := range requiredSpans {
		if !spanNames[reqSpan] {
			t.Errorf("expected span %q in trace %s, but got spans: %v", reqSpan, knownTraceID, spanNames)
		}
	}
}

func verifyMetricsEndpoints(t *testing.T, client *http.Client, cluster *devserver.Cluster) {
	endpoints := []struct {
		name string
		url  string
	}{
		{"Edge", cluster.EdgeURL() + "/metrics"},
		{"Gateway 1", cluster.Gateway1URL() + "/metrics"},
		{"Gateway 2", cluster.Gateway2URL() + "/metrics"},
		{"Control", cluster.ControlURL() + "/metrics"},
		{"Usage", cluster.UsageURL() + "/metrics"},
		{"Budget", cluster.BudgetURL() + "/metrics"},
	}

	for _, ep := range endpoints {
		checkMetricsEndpoint(t, client, ep.name, ep.url)
	}
}

func TestE2E_AC1F10_AC37_Observability_TraceContextAndMetrics(t *testing.T) {
	traceProv := &traceCapturingProvider{}
	spanRecorder := tracetest.NewSpanRecorder()

	cluster, client := startTestCluster(t, func(c *devserver.Config) {
		c.Providers = map[string]provider.Provider{
			"openai": traceProv,
		}
		c.SpanProcessor = spanRecorder
	})

	traceChan, stopConsumer := subscribeNATSUsageTrace(t, cluster.JS())
	defer stopConsumer()

	knownTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	knownSpanID := "00f067aa0ba902b7"
	testTraceparent := fmt.Sprintf("00-%s-%s-01", knownTraceID, knownSpanID)

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Trace and metrics test"},
		},
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, cluster.EdgeURL()+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer lgw_0123456789abcdef0123456789abcdef")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", testTraceparent)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	if traceProv.TraceID() != knownTraceID {
		t.Errorf("provider did not receive propagated trace ID: got %s, want %s", traceProv.TraceID(), knownTraceID)
	}

	select {
	case natsTraceparent := <-traceChan:
		if !strings.Contains(natsTraceparent, knownTraceID) {
			t.Errorf("NATS message header traceparent missing trace ID %s, got: %s", knownTraceID, natsTraceparent)
		}
	case <-time.After(3 * time.Second):
		t.Error("timed out waiting for NATS usage event with propagated traceparent")
	}

	requiredSpans := []string{
		"POST /",
		"POST /v1/chat/completions",
		"lgw.budget.v1.BudgetService/Reserve",
		"lgw.budget.v1.BudgetService/Settle",
		"usage.consume_event",
	}
	verifySpans(t, spanRecorder, knownTraceID, requiredSpans)

	verifyMetricsEndpoints(t, client, cluster)
}

func checkMetricsEndpoint(t *testing.T, client *http.Client, name, url string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("%s metrics request failed: %v", name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s metrics expected 200 OK, got %d", name, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read %s metrics body: %v", name, err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "# HELP") && !strings.Contains(bodyStr, "go_") {
		t.Errorf("%s metrics missing expected Prometheus format: %s", name, bodyStr[:min(len(bodyStr), 200)])
	}
}
