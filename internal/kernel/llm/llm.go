package llm

import "errors"

var (
	ErrInvalidKey         = errors.New("invalid api key")
	ErrModelNotAllowed    = errors.New("model not allowed for key")
	ErrBudgetExceeded     = errors.New("budget exceeded")
	ErrRateLimited        = errors.New("rate limited")
	ErrBudgetUnavailable  = errors.New("budget service unavailable")
	ErrRetryable          = errors.New("temporary upstream failure")
	ErrAllProvidersFailed = errors.New("all providers in fallback chain failed")
	ErrInvalidRequest     = errors.New("invalid request")
)

type CostKind string

const (
	CostKindBilled     CostKind = "billed"
	CostKindEquivalent CostKind = "equivalent"
)

type Source string

const (
	SourceGateway         Source = "gateway"
	SourceClaudeCodeLocal Source = "claude-code-local"
	SourceAnthropicOrg    Source = "anthropic-org"
	SourceOpenAIOrg       Source = "openai-org"
)

type Usage struct {
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
	CacheReadTokens  int64 `json:"cache_read_tokens"`
	CacheWriteTokens int64 `json:"cache_write_tokens"`
}

func (u Usage) Total() int64 {
	return u.InputTokens + u.OutputTokens + u.CacheReadTokens + u.CacheWriteTokens
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content,omitempty"`
}

type Request struct {
	ID           string    `json:"id"`
	KeyID        string    `json:"key_id"`
	Model        string    `json:"model"`
	Provider     string    `json:"provider,omitempty"`
	Stream       bool      `json:"stream"`
	Messages     []Message `json:"messages,omitempty"`
	PromptTokens int64     `json:"prompt_tokens,omitempty"`
	MaxTokens    int64     `json:"max_tokens,omitempty"`
	Temperature  *float64  `json:"temperature,omitempty"`
}

type Response struct {
	ID        string `json:"id"`
	Model     string `json:"model"`
	Provider  string `json:"provider"`
	Content   string `json:"content,omitempty"`
	Usage     Usage  `json:"usage"`
	LatencyMs int64  `json:"latency_ms"`
	Cached    bool   `json:"cached"`
}

type Chunk struct {
	ID           string `json:"id"`
	Content      string `json:"content"`
	FinishReason string `json:"finish_reason,omitempty"`
	Usage        *Usage `json:"usage,omitempty"`
	Err          error  `json:"error,omitempty"`
}
