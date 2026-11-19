// Package anthropic adapts the Anthropic Messages API to Switchyard's OpenAI-compatible types,
// in both directions and for streaming.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
	"github.com/Haklifesavr/switchyard/internal/sse"
)

const (
	// DefaultBaseURL is the public Anthropic API.
	DefaultBaseURL = "https://api.anthropic.com/v1"
	apiVersion     = "2023-06-01"
	// defaultMaxTokens is used when the client omits max_tokens, which Anthropic requires.
	defaultMaxTokens = 1024
)

// Provider is an Anthropic upstream.
type Provider struct {
	name    string
	baseURL string
	apiKey  string
	timeout time.Duration
	client  *http.Client
}

// New creates a provider. An empty baseURL means the public Anthropic API.
func New(name, baseURL, apiKey string, timeout time.Duration) *Provider {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Provider{
		name: name, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey,
		timeout: timeout, client: provider.HTTPClient(timeout),
	}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

// --- wire types ---

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type request struct {
	Model         string    `json:"model"`
	System        string    `json:"system,omitempty"`
	Messages      []message `json:"messages"`
	MaxTokens     int       `json:"max_tokens"`
	Temperature   *float64  `json:"temperature,omitempty"`
	TopP          *float64  `json:"top_p,omitempty"`
	StopSequences []string  `json:"stop_sequences,omitempty"`
	Stream        bool      `json:"stream,omitempty"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type response struct {
	ID         string         `json:"id"`
	Model      string         `json:"model"`
	Content    []contentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
	Usage      usage          `json:"usage"`
}

// toAnthropic converts an OpenAI-style request. System messages are lifted into the
// top-level system field, and consecutive same-role turns are merged because Anthropic
// requires user/assistant turns to alternate.
func toAnthropic(r *api.ChatRequest) request {
	out := request{
		Model:         r.Model,
		Temperature:   r.Temperature,
		TopP:          r.TopP,
		StopSequences: r.Stop,
		Stream:        r.Stream,
		MaxTokens:     defaultMaxTokens,
	}
	if r.MaxTokens != nil && *r.MaxTokens > 0 {
		out.MaxTokens = *r.MaxTokens
	}
	var system []string
	for _, m := range r.Messages {
		switch m.Role {
		case "system", "developer":
			system = append(system, m.Content)
			continue
		case "assistant":
		default:
			m.Role = "user" // tool/function roles are folded into user turns
		}
		if n := len(out.Messages); n > 0 && out.Messages[n-1].Role == m.Role {
			out.Messages[n-1].Content += "\n\n" + m.Content
			continue
		}
		out.Messages = append(out.Messages, message{Role: m.Role, Content: m.Content})
	}
	out.System = strings.Join(system, "\n\n")
	return out
}

// finishReason maps Anthropic stop reasons onto OpenAI finish reasons.
func finishReason(stop string) string {
	switch stop {
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	default: // end_turn, stop_sequence
		return "stop"
	}
}

func fromAnthropic(r *response) *api.ChatResponse {
	var text strings.Builder
	for _, b := range r.Content {
		if b.Type == "text" {
			text.WriteString(b.Text)
		}
	}
	return &api.ChatResponse{
		ID:      r.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   r.Model,
		Choices: []api.Choice{{
			Message:      api.Message{Role: "assistant", Content: text.String()},
			FinishReason: finishReason(r.StopReason),
		}},
		Usage: api.Usage{
			PromptTokens:     r.Usage.InputTokens,
			CompletionTokens: r.Usage.OutputTokens,
			TotalTokens:      r.Usage.InputTokens + r.Usage.OutputTokens,
		},
	}
}

func (p *Provider) do(ctx context.Context, body request) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/messages", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("anthropic-version", apiVersion)
	req.Header.Set("content-type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, provider.FromTransport(p.name, err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		e := provider.FromStatus(p.name, resp.StatusCode, resp.Body)
		if resp.StatusCode == 529 { // Anthropic "overloaded"
			e.Retryable = true
		}
		return nil, e
	}
	return resp, nil
}

// Complete implements provider.Provider.
func (p *Provider) Complete(ctx context.Context, req *api.ChatRequest) (*api.ChatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	body := toAnthropic(req)
	body.Stream = false
	resp, err := p.do(ctx, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, provider.FromTransport(p.name, fmt.Errorf("decode response: %w", err))
	}
	return fromAnthropic(&out), nil
}

// Stream implements provider.Provider.
func (p *Provider) Stream(ctx context.Context, req *api.ChatRequest) (provider.Stream, error) {
	body := toAnthropic(req)
	body.Stream = true
	resp, err := p.do(ctx, body) //nolint:bodyclose // the stream owns the body and closes it in Close
	if err != nil {
		return nil, err
	}
	return &stream{name: p.name, body: resp.Body, r: sse.NewReader(resp.Body), model: req.Model, created: time.Now().Unix()}, nil
}

// stream converts Anthropic stream events into OpenAI chunks:
//
//	message_start        -> first chunk with role "assistant" (records input tokens)
//	content_block_delta  -> chunk with the text delta
//	message_delta        -> final chunk with finish_reason and usage
//	message_stop         -> io.EOF
//	error                -> retryable provider error
type stream struct {
	name    string
	body    io.ReadCloser
	r       *sse.Reader
	id      string
	model   string
	created int64
	input   int
	done    bool
}

type streamEvent struct {
	Type    string `json:"type"`
	Message struct {
		ID    string `json:"id"`
		Model string `json:"model"`
		Usage usage  `json:"usage"`
	} `json:"message"`
	Delta struct {
		Type       string `json:"type"`
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage usage `json:"usage"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *stream) chunk(delta api.Delta, finish *string) *api.ChatChunk {
	return &api.ChatChunk{
		ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model,
		Choices: []api.ChunkChoice{{Delta: delta, FinishReason: finish}},
	}
}

func (s *stream) Recv() (*api.ChatChunk, error) {
	for !s.done {
		ev, err := s.r.Next()
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, provider.FromTransport(s.name, err)
		}
		if ev.Data == "" {
			continue
		}
		var e streamEvent
		if err := json.Unmarshal([]byte(ev.Data), &e); err != nil {
			return nil, provider.FromTransport(s.name, fmt.Errorf("decode event: %w", err))
		}
		switch e.Type {
		case "message_start":
			s.id = e.Message.ID
			if e.Message.Model != "" {
				s.model = e.Message.Model
			}
			s.input = e.Message.Usage.InputTokens
			return s.chunk(api.Delta{Role: "assistant"}, nil), nil
		case "content_block_delta":
			if e.Delta.Type == "text_delta" && e.Delta.Text != "" {
				return s.chunk(api.Delta{Content: e.Delta.Text}, nil), nil
			}
		case "message_delta":
			c := s.chunk(api.Delta{}, api.StrPtr(finishReason(e.Delta.StopReason)))
			c.Usage = &api.Usage{
				PromptTokens:     s.input,
				CompletionTokens: e.Usage.OutputTokens,
				TotalTokens:      s.input + e.Usage.OutputTokens,
			}
			return c, nil
		case "message_stop":
			s.done = true
		case "error":
			return nil, &provider.Error{Provider: s.name, Message: e.Error.Message, Retryable: true}
		}
		// ping, content_block_start, content_block_stop: nothing to forward
	}
	return nil, io.EOF
}

func (s *stream) Close() error { return s.body.Close() }
