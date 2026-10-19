package fake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider/fake"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestFakeProvider_Do_Success(t *testing.T) {
	fp := fake.New(
		fake.WithName("test-fake"),
		fake.WithContent("Generated fake content"),
		fake.WithUsage(llm.Usage{InputTokens: 50, OutputTokens: 25}),
	)

	req := &llm.Request{
		ID:    "req-1",
		Model: "fake-model",
	}

	resp, err := fp.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Provider != "test-fake" {
		t.Errorf("expected provider test-fake, got %s", resp.Provider)
	}
	if resp.Content != "Generated fake content" {
		t.Errorf("expected content 'Generated fake content', got %s", resp.Content)
	}
	if resp.Usage.InputTokens != 50 || resp.Usage.OutputTokens != 25 {
		t.Errorf("unexpected usage: %+v", resp.Usage)
	}
	if fp.CallCount() != 1 {
		t.Errorf("expected call count 1, got %d", fp.CallCount())
	}
}

func TestFakeProvider_Do_ErrorsByAttempt(t *testing.T) {
	errTemp := errors.New("temporary error")
	fp := fake.New(
		fake.WithErrorSequence(errTemp, nil), // fail attempt 1, succeed attempt 2
	)

	req := &llm.Request{ID: "req-seq"}

	// Attempt 1 fails
	_, err := fp.Do(context.Background(), req)
	if !errors.Is(err, errTemp) {
		t.Fatalf("expected errTemp on attempt 1, got %v", err)
	}

	// Attempt 2 succeeds
	resp, err := fp.Do(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error on attempt 2: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response on attempt 2")
	}
}

func TestFakeProvider_Do_DelayAndContextCancel(t *testing.T) {
	fp := fake.New(
		fake.WithDelay(200 * time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := fp.Do(ctx, &llm.Request{ID: "req-delay"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestFakeProvider_DoStream_Success(t *testing.T) {
	fp := fake.New(
		fake.WithStreamChunks(
			llm.Chunk{ID: "c1", Content: "Hello "},
			llm.Chunk{ID: "c2", Content: "world!"},
			llm.Chunk{
				ID:           "c3",
				FinishReason: "stop",
				Usage:        &llm.Usage{InputTokens: 10, OutputTokens: 5},
			},
		),
	)

	ch, err := fp.DoStream(context.Background(), &llm.Request{ID: "req-stream"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var chunks []llm.Chunk
	for chunk := range ch {
		chunks = append(chunks, chunk)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0].Content != "Hello " || chunks[1].Content != "world!" {
		t.Errorf("unexpected chunk content: %+v", chunks)
	}
	if chunks[2].Usage == nil || chunks[2].Usage.Total() != 15 {
		t.Errorf("unexpected final usage chunk: %+v", chunks[2])
	}
}

func TestFakeProvider_DoStream_Error(t *testing.T) {
	testErr := errors.New("stream start failure")
	fp := fake.New(
		fake.WithError(testErr),
	)

	_, err := fp.DoStream(context.Background(), &llm.Request{ID: "req-err"})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected testErr, got %v", err)
	}
}

func TestFakeProvider_Setters(t *testing.T) {
	fp := fake.New()
	fp.SetDelay(10 * time.Millisecond)
	fp.SetError(errors.New("dynamic error"))
	fp.SetStreamChunks([]llm.Chunk{{ID: "dyn", Content: "Dynamic chunk"}})

	_, err := fp.Do(context.Background(), &llm.Request{ID: "r1"})
	if err == nil || err.Error() != "dynamic error" {
		t.Errorf("expected dynamic error, got %v", err)
	}

	reqs := fp.Requests()
	if len(reqs) != 1 || reqs[0].ID != "r1" {
		t.Errorf("unexpected recorded requests: %+v", reqs)
	}
}
