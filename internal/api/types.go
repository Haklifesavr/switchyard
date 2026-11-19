// Package api defines the OpenAI-compatible wire types that Switchyard speaks to clients.
// Providers translate to and from these types.
package api

import "encoding/json"

// Message is a single chat message. Content is plain text; multimodal content is out of scope.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest mirrors the OpenAI /v1/chat/completions request body (the commonly used subset).
type ChatRequest struct {
	Model         string         `json:"model"`
	Messages      []Message      `json:"messages"`
	Temperature   *float64       `json:"temperature,omitempty"`
	TopP          *float64       `json:"top_p,omitempty"`
	MaxTokens     *int           `json:"max_tokens,omitempty"`
	Stop          []string       `json:"stop,omitempty"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
	User          string         `json:"user,omitempty"`
}

// StreamOptions asks OpenAI-compatible upstreams to report usage in the final stream chunk.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// Clone returns a deep copy so routing can rewrite the model per target safely.
func (r *ChatRequest) Clone() *ChatRequest {
	c := *r
	c.Messages = append([]Message(nil), r.Messages...)
	c.Stop = append([]string(nil), r.Stop...)
	if r.StreamOptions != nil {
		so := *r.StreamOptions
		c.StreamOptions = &so
	}
	return &c
}

// Usage reports token counts.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Choice is one completion alternative.
type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

// ChatResponse mirrors the OpenAI chat completion response.
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// Delta is the incremental message content in a streaming chunk.
type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// ChunkChoice is one choice inside a streaming chunk.
type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// ChatChunk mirrors an OpenAI "chat.completion.chunk" streaming event.
type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// Model is an entry in GET /v1/models.
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// ModelList is the GET /v1/models response.
type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

// ErrorBody is the OpenAI-style error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes an error to the client.
type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

// StrPtr is a small helper for optional string fields such as FinishReason.
func StrPtr(s string) *string { return &s }

// MustJSON marshals v and panics on failure; only used for types that always marshal.
func MustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
