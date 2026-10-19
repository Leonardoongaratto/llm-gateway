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

func TestE2E_AC1F10_AC37_Observability_TraceContextAndMetrics(t *testing.T) {
	traceProv := &traceCapturingProvider{}

	cluster, client := startTestCluster(t, func(c *devserver.Config) {
		c.Providers = map[string]provider.Provider{
			"openai": traceProv,
		}
	})

	ctx := context.Background()

	// 1. Subscribe to NATS JetStream events to verify trace propagation in asynchronous messages
	js := cluster.JS()
	var natsTraceparent string
	var natsReceived sync.WaitGroup
	natsReceived.Add(1)

	consumer, err := js.CreateOrUpdateConsumer(ctx, "LGW_USAGE", jetstream.ConsumerConfig{
		FilterSubject: "lgw.usage.recorded.v1",
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckNonePolicy,
	})
	if err != nil {
		t.Fatalf("failed to create test watcher consumer: %v", err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		headers := msg.Headers()
		if headers != nil {
			natsTraceparent = headers.Get("Traceparent")
		}
		natsReceived.Done()
	})
	if err != nil {
		t.Fatalf("failed to start consuming for trace test: %v", err)
	}
	defer consumeCtx.Stop()

	// 2. Send request with known W3C traceparent
	knownTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	knownSpanID := "00f067aa0ba902b7"
	testTraceparent := fmt.Sprintf("00-%s-%s-01", knownTraceID, knownSpanID)

	payload := map[string]any{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "user", "content": "Trace and metrics test"},
		},
	}
	bodyBytes, _ := json.Marshal(payload)

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

	// 3. Verify provider received traceparent context with same trace ID
	if traceProv.TraceID() != knownTraceID {
		t.Errorf("provider did not receive propagated trace ID: got %s, want %s", traceProv.TraceID(), knownTraceID)
	}

	// 4. Verify NATS received traceparent context with same trace ID
	natsChan := make(chan struct{})
	go func() {
		natsReceived.Wait()
		close(natsChan)
	}()

	select {
	case <-natsChan:
		if !strings.Contains(natsTraceparent, knownTraceID) {
			t.Errorf("NATS message header traceparent missing trace ID %s, got: %s", knownTraceID, natsTraceparent)
		}
	case <-time.After(3 * time.Second):
		t.Error("timed out waiting for NATS usage event with propagated traceparent")
	}

	// 5. Verify Prometheus metrics endpoints on all services
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
