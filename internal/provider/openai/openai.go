// Package openai talks to the OpenAI Chat Completions API, or any OpenAI-compatible server
// (Azure OpenAI, vLLM, Ollama, LM Studio, ...) via a custom base URL.
package openai

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

// DefaultBaseURL is the public OpenAI API.
const DefaultBaseURL = "https://api.openai.com/v1"

// Provider is an OpenAI-compatible upstream.
type Provider struct {
	name    string
	baseURL string
	apiKey  string
	timeout time.Duration
	client  *http.Client
}

// New creates a provider. An empty baseURL means the public OpenAI API.
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

func (p *Provider) do(ctx context.Context, req *api.ChatRequest) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Stream {
		httpReq.Header.Set("Accept", "text/event-stream")
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, provider.FromTransport(p.name, err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		return nil, provider.FromStatus(p.name, resp.StatusCode, resp.Body)
	}
	return resp, nil
}

// Complete implements provider.Provider.
func (p *Provider) Complete(ctx context.Context, req *api.ChatRequest) (*api.ChatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	r := req.Clone()
	r.Stream, r.StreamOptions = false, nil
	resp, err := p.do(ctx, r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out api.ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, provider.FromTransport(p.name, fmt.Errorf("decode response: %w", err))
	}
	return &out, nil
}

// Stream implements provider.Provider. It asks for usage in the final chunk so token
// budgets stay accurate for streamed responses too.
func (p *Provider) Stream(ctx context.Context, req *api.ChatRequest) (provider.Stream, error) {
	r := req.Clone()
	r.Stream = true
	r.StreamOptions = &api.StreamOptions{IncludeUsage: true}
	resp, err := p.do(ctx, r) //nolint:bodyclose // the stream owns the body and closes it in Close
	if err != nil {
		return nil, err
	}
	return &stream{name: p.name, body: resp.Body, r: sse.NewReader(resp.Body)}, nil
}

type stream struct {
	name string
	body io.ReadCloser
	r    *sse.Reader
}

func (s *stream) Recv() (*api.ChatChunk, error) {
	for {
		ev, err := s.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, provider.FromTransport(s.name, err)
		}
		if ev.Data == "" {
			continue
		}
		if ev.Data == "[DONE]" {
			return nil, io.EOF
		}
		var c api.ChatChunk
		if err := json.Unmarshal([]byte(ev.Data), &c); err != nil {
			return nil, provider.FromTransport(s.name, fmt.Errorf("decode chunk: %w", err))
		}
		return &c, nil
	}
}

func (s *stream) Close() error { return s.body.Close() }
