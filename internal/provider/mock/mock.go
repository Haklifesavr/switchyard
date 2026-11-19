// Package mock is a deterministic in-process provider for demos, tests and load testing.
// It echoes the last user message, and can simulate latency and failures so fallback,
// retries and circuit breaking can be seen working without any API keys.
package mock

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
)

// Provider is the mock upstream.
type Provider struct {
	name        string
	latency     time.Duration
	failureRate float64

	mu  sync.Mutex
	rng *rand.Rand
}

// New creates a mock provider. failureRate is the probability (0..1) that a request fails with 503.
func New(name string, latency time.Duration, failureRate float64) *Provider {
	return &Provider{name: name, latency: latency, failureRate: failureRate, rng: rand.New(rand.NewPCG(1, 2))}
}

// Name implements provider.Provider.
func (p *Provider) Name() string { return p.name }

func (p *Provider) fail() bool {
	if p.failureRate <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Float64() < p.failureRate
}

func (p *Provider) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return provider.FromTransport(p.name, ctx.Err())
	case <-t.C:
		return nil
	}
}

func reply(req *api.ChatRequest) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			return fmt.Sprintf("Echo from %s: %s", req.Model, req.Messages[i].Content)
		}
	}
	return fmt.Sprintf("Hello from %s.", req.Model)
}

func (p *Provider) unavailable() error {
	return &provider.Error{Provider: p.name, Status: http.StatusServiceUnavailable, Message: "simulated outage", Retryable: true}
}

// Complete implements provider.Provider.
func (p *Provider) Complete(ctx context.Context, req *api.ChatRequest) (*api.ChatResponse, error) {
	if err := p.wait(ctx, p.latency); err != nil {
		return nil, err
	}
	if p.fail() {
		return nil, p.unavailable()
	}
	text := reply(req)
	prompt, completion := provider.PromptTokens(req), provider.EstimateTokens(text)
	return &api.ChatResponse{
		ID:      fmt.Sprintf("mock-%d", time.Now().UnixNano()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []api.Choice{{Message: api.Message{Role: "assistant", Content: text}, FinishReason: "stop"}},
		Usage:   api.Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion},
	}, nil
}

// Stream implements provider.Provider, emitting one word per chunk.
func (p *Provider) Stream(ctx context.Context, req *api.ChatRequest) (provider.Stream, error) {
	if err := p.wait(ctx, p.latency); err != nil {
		return nil, err
	}
	if p.fail() {
		return nil, p.unavailable()
	}
	text := reply(req)
	words := strings.SplitAfter(text, " ")
	return &stream{
		ctx: ctx, p: p, model: req.Model, words: words,
		id: fmt.Sprintf("mock-%d", time.Now().UnixNano()), created: time.Now().Unix(),
		usage: api.Usage{PromptTokens: provider.PromptTokens(req), CompletionTokens: provider.EstimateTokens(text)},
	}, nil
}

type stream struct {
	ctx     context.Context
	p       *Provider
	model   string
	id      string
	created int64
	words   []string
	i       int
	done    bool
	usage   api.Usage
}

func (s *stream) chunk(delta api.Delta, finish *string) *api.ChatChunk {
	return &api.ChatChunk{
		ID: s.id, Object: "chat.completion.chunk", Created: s.created, Model: s.model,
		Choices: []api.ChunkChoice{{Delta: delta, FinishReason: finish}},
	}
}

func (s *stream) Recv() (*api.ChatChunk, error) {
	if s.done {
		return nil, io.EOF
	}
	if s.i < len(s.words) {
		if err := s.p.wait(s.ctx, s.p.latency/10); err != nil {
			return nil, err
		}
		d := api.Delta{Content: s.words[s.i]}
		if s.i == 0 {
			d.Role = "assistant"
		}
		s.i++
		return s.chunk(d, nil), nil
	}
	s.done = true
	c := s.chunk(api.Delta{}, api.StrPtr("stop"))
	u := s.usage
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	c.Usage = &u
	return c, nil
}

func (s *stream) Close() error { return nil }
