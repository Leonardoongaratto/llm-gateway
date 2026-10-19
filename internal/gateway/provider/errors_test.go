package provider_test

import (
	"errors"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/gateway/provider"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

func TestUpstreamError(t *testing.T) {
	errNonRetry := &provider.UpstreamError{
		StatusCode: 400,
		Message:    "bad prompt",
		Retryable:  false,
	}
	if errNonRetry.Error() != "upstream 400: bad prompt" {
		t.Errorf("unexpected error string: %s", errNonRetry.Error())
	}
	if !errors.Is(errNonRetry, llm.ErrInvalidRequest) {
		t.Errorf("expected ErrInvalidRequest, got %v", errNonRetry)
	}
	if errors.Is(errNonRetry, llm.ErrRetryable) {
		t.Errorf("did not expect ErrRetryable")
	}
	if provider.IsRetryable(errNonRetry) {
		t.Errorf("expected IsRetryable=false")
	}

	errRetry := &provider.UpstreamError{
		StatusCode: 503,
		Message:    "overloaded",
		Retryable:  true,
	}
	if !errors.Is(errRetry, llm.ErrRetryable) {
		t.Errorf("expected ErrRetryable")
	}
	if !provider.IsRetryable(errRetry) {
		t.Errorf("expected IsRetryable=true")
	}

	// nil error
	if provider.IsRetryable(nil) {
		t.Errorf("nil error should not be retryable")
	}

	// standard llm.ErrRetryable
	if !provider.IsRetryable(llm.ErrRetryable) {
		t.Errorf("llm.ErrRetryable should be retryable")
	}
}
