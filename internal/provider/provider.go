// Package provider defines the interface every upstream LLM API implements,
// plus shared error classification used by the router to decide on retries and fallback.
package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
)

// Provider is an upstream LLM API. Requests arrive with Model already set to the upstream model name.
type Provider interface {
	Name() string
	Complete(ctx context.Context, req *api.ChatRequest) (*api.ChatResponse, error)
	// Stream opens a streaming completion. It returns only after the upstream has accepted
	// the request, so the router can still fall back if opening fails.
	Stream(ctx context.Context, req *api.ChatRequest) (Stream, error)
}

// Stream yields OpenAI-format chunks. Recv returns io.EOF when the stream ends normally.
type Stream interface {
	Recv() (*api.ChatChunk, error)
	Close() error
}

// Error is an upstream failure annotated with whether retrying (or falling back) can help.
type Error struct {
	Provider  string
	Status    int // HTTP status from upstream, 0 for network errors
	Message   string
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s: upstream returned %d: %s", e.Provider, e.Status, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Provider, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

// IsRetryable reports whether err is worth retrying or falling back on.
func IsRetryable(err error) bool {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Retryable
	}
	return false
}

// FromStatus classifies a non-2xx upstream response. Rate limits and server errors are
// retryable; other client errors (bad request, auth, not found) are not.
func FromStatus(name string, status int, body io.Reader) *Error {
	msg := extractMessage(body)
	if msg == "" {
		msg = http.StatusText(status)
	}
	return &Error{
		Provider:  name,
		Status:    status,
		Message:   msg,
		Retryable: status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500,
	}
}

// FromTransport classifies network-level failures. A cancelled client request is final;
// everything else (timeouts, refused connections, DNS, TLS, truncated bodies) is usually
// transient, so it is retryable.
func FromTransport(name string, err error) *Error {
	if errors.Is(err, context.Canceled) {
		return &Error{Provider: name, Message: "request cancelled", Err: err}
	}
	return &Error{Provider: name, Message: err.Error(), Retryable: true, Err: err}
}

// extractMessage pulls a human-readable message out of common error JSON shapes.
func extractMessage(r io.Reader) string {
	if r == nil {
		return ""
	}
	raw, _ := io.ReadAll(io.LimitReader(r, 64<<10))
	var body struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(raw, &body) == nil {
		var obj struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body.Error, &obj) == nil && obj.Message != "" {
			return obj.Message
		}
		var s string
		if json.Unmarshal(body.Error, &s) == nil && s != "" {
			return s
		}
		if body.Message != "" {
			return body.Message
		}
	}
	if len(raw) > 200 {
		raw = raw[:200]
	}
	return string(raw)
}

// HTTPClient builds a client whose timeout applies to the wait for response headers, so long
// streams are not cut off while a stalled upstream still fails fast.
func HTTPClient(headerTimeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = headerTimeout
	t.MaxIdleConnsPerHost = 32
	return &http.Client{Transport: t}
}

// EstimateTokens approximates a token count (~4 characters per token for English text).
// Used only when an upstream does not report usage.
func EstimateTokens(s string) int {
	if s == "" {
		return 0
	}
	return (len(s) + 3) / 4
}

// PromptTokens estimates the prompt size of a request.
func PromptTokens(req *api.ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += EstimateTokens(m.Content) + 4 // per-message overhead
	}
	return n
}
