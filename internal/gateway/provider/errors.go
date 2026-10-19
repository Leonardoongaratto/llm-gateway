package provider

import (
	"errors"
	"fmt"

	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/llm"
)

// UpstreamError represents an HTTP error returned by an external LLM provider.
type UpstreamError struct {
	StatusCode int
	Message    string
	Retryable  bool
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream %d: %s", e.StatusCode, e.Message)
}

func (e *UpstreamError) Is(target error) bool {
	if e.Retryable && errors.Is(target, llm.ErrRetryable) {
		return true
	}
	if !e.Retryable && errors.Is(target, llm.ErrInvalidRequest) {
		return true
	}
	return false
}

// IsRetryable determines whether a provider error should trigger fallback.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var uErr *UpstreamError
	if errors.As(err, &uErr) {
		return uErr.Retryable
	}
	return errors.Is(err, llm.ErrRetryable)
}
