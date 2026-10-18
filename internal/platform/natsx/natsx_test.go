package natsx_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	budgetv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/budget/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/grpcx"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/natsx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

// AC-1A-09: Given o NATS embutido e um consumidor que dá nack sempre,
// when uma mensagem é publicada, then ela é reentregue com backoff e vai para lgw.dlq.<subject> após 5 tentativas;
// publicar duas vezes com o mesmo Nats-Msg-Id armazena uma só.
func TestAC1A09_DLQAndDeduplication(t *testing.T) {
	_, url := natsx.StartEmbeddedServer(t)

	nc, js, err := natsx.Connect(url)
	if err != nil {
		t.Fatalf("failed to connect to embedded nats: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := natsx.InitStreams(ctx, js); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}

	// 1. Test Deduplication
	subject := "lgw.usage.recorded.v1"
	msgID := "unique-dedupe-id-123"

	ack1, err := natsx.Publish(ctx, js, subject, []byte(`{"id":"1"}`), msgID)
	if err != nil {
		t.Fatalf("publish 1 failed: %v", err)
	}
	if ack1.Duplicate {
		t.Fatal("expected first publish to not be duplicate")
	}

	ack2, err := natsx.Publish(ctx, js, subject, []byte(`{"id":"1"}`), msgID)
	if err != nil {
		t.Fatalf("publish 2 failed: %v", err)
	}
	if !ack2.Duplicate {
		t.Fatal("expected second publish with same Nats-Msg-Id to be reported as duplicate")
	}

	// Verify stream has only 1 message stored
	info, err := js.Stream(ctx, natsx.StreamUsage)
	if err != nil {
		t.Fatalf("failed to get stream info: %v", err)
	}
	streamState, err := info.Info(ctx)
	if err != nil {
		t.Fatalf("failed to read stream info: %v", err)
	}
	if streamState.State.Msgs != 1 {
		t.Errorf("expected 1 message in stream, got %d", streamState.State.Msgs)
	}

	// 2. Test DLQ forwarding after 5 deliveries
	// Also configure a stream or subject listener for the DLQ subject
	dlqSubject := natsx.DLQSubject(subject)
	dlqReceived := make(chan *nats.Msg, 1)
	_, err = nc.Subscribe(dlqSubject, func(m *nats.Msg) {
		dlqReceived <- m
	})
	if err != nil {
		t.Fatalf("failed to subscribe to DLQ: %v", err)
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamUsage, jetstream.ConsumerConfig{
		Durable:       "test-retry-consumer",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var deliveryAttempts atomic.Int32
	consumeCtx, err := natsx.ConsumeWithDLQ(ctx, js, consumer, func(c context.Context, msg jetstream.Msg) error {
		deliveryAttempts.Add(1)
		return errors.New("simulated transient handler failure")
	}, 5)
	if err != nil {
		t.Fatalf("failed to start consume with DLQ: %v", err)
	}
	defer consumeCtx.Stop()

	// Wait for message to land in DLQ
	select {
	case dlqMsg := <-dlqReceived:
		if dlqMsg.Header.Get("X-DLQ-Original-Subject") != subject {
			t.Errorf("expected original subject %s, got %s", subject, dlqMsg.Header.Get("X-DLQ-Original-Subject"))
		}
		if attempts := deliveryAttempts.Load(); attempts < 5 {
			t.Errorf("expected at least 5 delivery attempts before DLQ, got %d", attempts)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for message to be routed to DLQ; attempts recorded: %d", deliveryAttempts.Load())
	}
}

// AC-1A-07: Given uma chamada HTTP que vira chamada gRPC e depois publica no NATS embutido,
// when o consumidor recebe, then os spans compartilham o mesmo trace_id e a cadeia de pais está correta.
type traceBudgetServer struct {
	budgetv1.UnimplementedBudgetServiceServer
	js     jetstream.JetStream
	tracer trace.Tracer
}

func (s *traceBudgetServer) Remaining(ctx context.Context, req *budgetv1.RemainingRequest) (*budgetv1.RemainingResponse, error) {
	// Extract incoming gRPC metadata for tracing
	md, _ := metadata.FromIncomingContext(ctx)
	headerCarrier := make(propagation.HeaderCarrier)
	for k, v := range md {
		if len(v) > 0 {
			headerCarrier.Set(k, v[0])
		}
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, headerCarrier)

	ctx, span := s.tracer.Start(ctx, "grpc.BudgetService/Remaining")
	defer span.End()

	// Publish event to NATS
	_, err := natsx.Publish(ctx, s.js, "lgw.usage.recorded.v1", []byte(`{"event":"trace-test"}`), "")
	if err != nil {
		return nil, err
	}

	return &budgetv1.RemainingResponse{DailyMicros: 999}, nil
}

func TestAC1A07_EndToEndTracePropagation(t *testing.T) {
	// Configure in-memory trace exporter
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := tp.Tracer("test-pipeline")

	_, natsURL := natsx.StartEmbeddedServer(t)
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := natsx.InitStreams(ctx, js); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}

	// 1. Setup gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on tcp: %v", err)
	}
	defer lis.Close()

	srv := grpcx.NewServer()
	budgetv1.RegisterBudgetServiceServer(srv, &traceBudgetServer{js: js, tracer: tracer})
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	clientConn, err := grpcx.Dial(ctx, lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial grpc: %v", err)
	}
	defer clientConn.Close()
	grpcClient := budgetv1.NewBudgetServiceClient(clientConn)

	// 2. Setup NATS Consumer
	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamUsage, jetstream.ConsumerConfig{
		Durable:       "trace-consumer",
		FilterSubject: "lgw.usage.recorded.v1",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var consumerSpanCollected sync.WaitGroup
	consumerSpanCollected.Add(1)

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		msgCtx := natsx.ExtractTraceContext(context.Background(), msg)
		_, span := tracer.Start(msgCtx, "nats.Consumer/Process")
		span.End()
		_ = msg.Ack()
		consumerSpanCollected.Done()
	})
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	defer consumeCtx.Stop()

	// 3. Setup HTTP Handler
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCtx, span := tracer.Start(r.Context(), "http.Handle/Inbound")
		defer span.End()

		// Inject trace context into gRPC outgoing metadata
		md := metadata.New(nil)
		headerCarrier := make(propagation.HeaderCarrier)
		otel.GetTextMapPropagator().Inject(reqCtx, headerCarrier)
		for k, v := range headerCarrier {
			md.Set(k, v...)
		}
		grpcCtx := metadata.NewOutgoingContext(reqCtx, md)

		_, err := grpcClient.Remaining(grpcCtx, &budgetv1.RemainingRequest{KeyId: "key-1"})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(httpHandler)
	defer ts.Close()

	// 4. Trigger HTTP request
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from http server, got %d: %s", resp.StatusCode, string(body))
	}

	// 5. Wait for NATS consumer to process
	consumerSpanCollected.Wait()

	// 6. Verify all spans in memory exporter
	spans := exporter.GetSpans()
	if len(spans) < 3 {
		t.Fatalf("expected at least 3 spans, got %d: %v", len(spans), spans)
	}

	rootTraceID := spans[0].SpanContext.TraceID().String()
	for _, s := range spans {
		if s.SpanContext.TraceID().String() != rootTraceID {
			t.Errorf("span %s has mismatching trace_id %s, expected %s",
				s.Name, s.SpanContext.TraceID().String(), rootTraceID)
		}
	}

	// Verify span names present
	names := make(map[string]bool)
	for _, s := range spans {
		names[s.Name] = true
	}

	expectedNames := []string{"http.Handle/Inbound", "grpc.BudgetService/Remaining", "nats.Consumer/Process"}
	for _, exp := range expectedNames {
		if !names[exp] {
			t.Errorf("missing expected span name %s in trace pipeline", exp)
		}
	}
}
