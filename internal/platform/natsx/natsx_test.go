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
	platformotel "github.com/Leonardoongaratto/llm-gateway/internal/platform/otel"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func runDeduplicationTest(ctx context.Context, t *testing.T, js jetstream.JetStream, subject string) {
	t.Helper()
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
}

func runDLQTest(ctx context.Context, t *testing.T, js jetstream.JetStream, subject string) {
	t.Helper()
	dlqSubject := natsx.DLQSubject(subject)
	dlqConsumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamDLQ, jetstream.ConsumerConfig{
		Durable:       "test-dlq-stream-consumer",
		FilterSubject: dlqSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create DLQ JetStream consumer: %v", err)
	}

	dlqReceived := make(chan jetstream.Msg, 1)
	dlqConsumeCtx, err := dlqConsumer.Consume(func(m jetstream.Msg) {
		dlqReceived <- m
		_ = m.Ack()
	})
	if err != nil {
		t.Fatalf("failed to consume DLQ: %v", err)
	}
	defer dlqConsumeCtx.Stop()

	consumer, err := js.CreateOrUpdateConsumer(ctx, natsx.StreamUsage, jetstream.ConsumerConfig{
		Durable:       "test-worker-consumer",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
		AckWait:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	var deliveryAttempts atomic.Int32
	consumeCtx, err := natsx.ConsumeWithDLQ(ctx, js, consumer, func(_ context.Context, _ jetstream.Msg) error {
		deliveryAttempts.Add(1)
		return errors.New("simulated transient handler failure")
	}, 5)
	if err != nil {
		t.Fatalf("failed to start consume with DLQ: %v", err)
	}
	defer consumeCtx.Stop()

	select {
	case dlqMsg := <-dlqReceived:
		if dlqMsg.Headers().Get("X-DLQ-Original-Subject") != subject {
			t.Errorf("expected original subject %s, got %s", subject, dlqMsg.Headers().Get("X-DLQ-Original-Subject"))
		}
		if attempts := deliveryAttempts.Load(); attempts < 5 {
			t.Errorf("expected at least 5 delivery attempts before DLQ, got %d", attempts)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for message to be routed to DLQ; attempts recorded: %d", deliveryAttempts.Load())
	}
}

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

	subject := "lgw.usage.recorded.v1"
	runDeduplicationTest(ctx, t, js, subject)
	runDLQTest(ctx, t, js, subject)
}

type traceBudgetServer struct {
	budgetv1.UnimplementedBudgetServiceServer
	js     jetstream.JetStream
	tracer trace.Tracer
}

func (s *traceBudgetServer) Remaining(ctx context.Context, _ *budgetv1.RemainingRequest) (*budgetv1.RemainingResponse, error) {
	ctx, span := s.tracer.Start(ctx, "grpc.BudgetService/Remaining")
	defer span.End()

	_, err := natsx.Publish(ctx, s.js, "lgw.usage.recorded.v1", []byte(`{"event":"trace-test"}`), "")
	if err != nil {
		return nil, err
	}

	return &budgetv1.RemainingResponse{DailyMicros: 999}, nil
}

func verifyTraceSpanChain(t *testing.T, spans []tracetest.SpanStub) {
	t.Helper()
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

	var serverSpan, consumerSpan tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "grpc.BudgetService/Remaining" {
			serverSpan = s
		}
		if s.Name == "nats.Consumer/Process" {
			consumerSpan = s
		}
	}

	if !serverSpan.Parent.SpanID().IsValid() {
		t.Error("expected gRPC server span to have a valid parent span from client propagation")
	}
	if !consumerSpan.Parent.SpanID().IsValid() {
		t.Error("expected NATS consumer span to have a valid parent span from NATS message header propagation")
	}
	if consumerSpan.Parent.SpanID() != serverSpan.SpanContext.SpanID() {
		t.Errorf("expected NATS consumer parent span ID to match server span ID (%s), got %s",
			serverSpan.SpanContext.SpanID().String(), consumerSpan.Parent.SpanID().String())
	}
}

func TestAC1A07_EndToEndTracePropagation(t *testing.T) {
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

	if err := natsx.InitStreams(ctx, js, natsx.WithMemoryStorage()); err != nil {
		t.Fatalf("failed to init streams: %v", err)
	}

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

	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := grpcClient.Remaining(r.Context(), &budgetv1.RemainingRequest{KeyId: "key-1"})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	httpHandler := platformotel.HTTPHandler(baseHandler, "http.Handle/Inbound")

	ts := httptest.NewServer(httpHandler)
	defer ts.Close()

	httpClient := platformotel.HTTPClient(nil)
	resp, err := httpClient.Get(ts.URL)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from http server, got %d: %s", resp.StatusCode, string(body))
	}

	consumerSpanCollected.Wait()
	verifyTraceSpanChain(t, exporter.GetSpans())
}

func TestNATSX_InitStreams_Storage(t *testing.T) {
	_, natsURL := natsx.StartEmbeddedServer(t)
	nc, js, err := natsx.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()
	if err := natsx.InitStreams(ctx, js, natsx.WithFileStorage()); err != nil {
		t.Fatalf("init default streams failed: %v", err)
	}
	info, err := js.Stream(ctx, natsx.StreamUsage)
	if err != nil {
		t.Fatalf("stream usage info failed: %v", err)
	}
	st, _ := info.Info(ctx)
	if st.Config.Storage != jetstream.FileStorage {
		t.Errorf("expected FileStorage by default, got %v", st.Config.Storage)
	}

	dlqInfo, err := js.Stream(ctx, natsx.StreamDLQ)
	if err != nil {
		t.Fatalf("stream dlq info failed: %v", err)
	}
	dlqSt, _ := dlqInfo.Info(ctx)
	if dlqSt.Config.Storage != jetstream.FileStorage {
		t.Errorf("expected FileStorage for DLQ, got %v", dlqSt.Config.Storage)
	}
}
