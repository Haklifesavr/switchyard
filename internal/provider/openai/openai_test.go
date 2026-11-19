package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
)

func req() *api.ChatRequest {
	return &api.ChatRequest{Model: "gpt-test", Messages: []api.Message{{Role: "user", Content: "hi"}}}
}

func TestCompleteSendsAuthAndDecodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
			t.Errorf("Authorization = %q", got)
		}
		var in api.ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Model != "gpt-test" || in.Stream {
			t.Errorf("unexpected request %+v", in)
		}
		_, _ = io.WriteString(w, `{"id":"c1","object":"chat.completion","model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`)
	}))
	defer srv.Close()

	p := New("oa", srv.URL+"/v1", "sk-test", 5*time.Second)
	out, err := p.Complete(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	if out.Choices[0].Message.Content != "hello" || out.Usage.TotalTokens != 4 {
		t.Errorf("unexpected response %+v", out)
	}
}

func TestErrorClassification(t *testing.T) {
	cases := []struct {
		status    int
		body      string
		retryable bool
		msg       string
	}{
		{429, `{"error":{"message":"slow down"}}`, true, "slow down"},
		{503, `oops`, true, "oops"},
		{400, `{"error":{"message":"bad model"}}`, false, "bad model"},
		{401, `{"error":{"message":"invalid key"}}`, false, "invalid key"},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
			_, _ = io.WriteString(w, c.body)
		}))
		_, err := New("oa", srv.URL, "k", time.Second).Complete(context.Background(), req())
		srv.Close()
		var pe *provider.Error
		if !errors.As(err, &pe) {
			t.Fatalf("status %d: want *provider.Error, got %v", c.status, err)
		}
		if pe.Retryable != c.retryable || pe.Status != c.status || pe.Message != c.msg {
			t.Errorf("status %d: got %+v", c.status, pe)
		}
	}
}

func TestStreamParsesChunksAndUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in api.ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&in)
		if !in.Stream || in.StreamOptions == nil || !in.StreamOptions.IncludeUsage {
			t.Errorf("stream options not set: %+v", in)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"s\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"s\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"s\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":2,\"total_tokens\":4}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	s, err := New("oa", srv.URL, "k", time.Second).Stream(context.Background(), req())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var text strings.Builder
	var usage *api.Usage
	for {
		c, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, ch := range c.Choices {
			text.WriteString(ch.Delta.Content)
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}
	if text.String() != "Hello" || usage == nil || usage.TotalTokens != 4 {
		t.Errorf("text=%q usage=%+v", text.String(), usage)
	}
}

func TestCancelledRequestIsNotRetryable(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select { // hang until the test is done, like a stalled upstream
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release) // runs first, so Close doesn't wait on the handler
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	_, err := New("oa", srv.URL, "k", 5*time.Second).Complete(ctx, req())
	if err == nil || provider.IsRetryable(err) {
		t.Fatalf("want non-retryable cancellation error, got %v", err)
	}
}

func TestUnreachableUpstreamIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listening any more
	_, err := New("oa", url, "k", time.Second).Complete(context.Background(), req())
	if !provider.IsRetryable(err) {
		t.Fatalf("connection failure should be retryable, got %v", err)
	}
}
