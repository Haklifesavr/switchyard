package anthropic

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

func TestToAnthropicTranslatesRequest(t *testing.T) {
	max := 300
	got := toAnthropic(&api.ChatRequest{
		Model: "claude-x", MaxTokens: &max, Stop: []string{"END"},
		Messages: []api.Message{
			{Role: "system", Content: "You are terse."},
			{Role: "user", Content: "a"},
			{Role: "user", Content: "b"}, // merged with previous user turn
			{Role: "assistant", Content: "c"},
			{Role: "developer", Content: "Use British spelling."},
			{Role: "user", Content: "d"},
		},
	})
	if got.System != "You are terse.\n\nUse British spelling." {
		t.Errorf("system = %q", got.System)
	}
	want := []message{{"user", "a\n\nb"}, {"assistant", "c"}, {"user", "d"}}
	if len(got.Messages) != len(want) {
		t.Fatalf("messages = %+v", got.Messages)
	}
	for i := range want {
		if got.Messages[i] != want[i] {
			t.Errorf("message %d = %+v, want %+v", i, got.Messages[i], want[i])
		}
	}
	if got.MaxTokens != 300 || got.StopSequences[0] != "END" {
		t.Errorf("max_tokens/stop not carried over: %+v", got)
	}
	if d := toAnthropic(&api.ChatRequest{Model: "m"}); d.MaxTokens != defaultMaxTokens {
		t.Errorf("default max_tokens = %d", d.MaxTokens)
	}
}

func TestCompleteMapsResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "ak" || r.Header.Get("anthropic-version") == "" {
			t.Errorf("bad request: %s %v", r.URL.Path, r.Header)
		}
		var in request
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Stream || in.System != "sys" {
			t.Errorf("unexpected body %+v", in)
		}
		_, _ = io.WriteString(w, `{"id":"msg_1","model":"claude-x","content":[{"type":"text","text":"Hi "},{"type":"text","text":"there"}],"stop_reason":"max_tokens","usage":{"input_tokens":7,"output_tokens":2}}`)
	}))
	defer srv.Close()
	out, err := New("an", srv.URL+"/v1", "ak", time.Second).Complete(context.Background(), &api.ChatRequest{
		Model: "claude-x", Messages: []api.Message{{Role: "system", Content: "sys"}, {Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	c := out.Choices[0]
	if c.Message.Content != "Hi there" || c.FinishReason != "length" || out.Usage.TotalTokens != 9 || out.Object != "chat.completion" {
		t.Errorf("unexpected response %+v", out)
	}
}

func TestOverloadedIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(529)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
	}))
	defer srv.Close()
	_, err := New("an", srv.URL, "ak", time.Second).Complete(context.Background(), &api.ChatRequest{Model: "m"})
	if !provider.IsRetryable(err) || !strings.Contains(err.Error(), "Overloaded") {
		t.Fatalf("want retryable overloaded error, got %v", err)
	}
}

const streamBody = `event: message_start
data: {"type":"message_start","message":{"id":"msg_s","model":"claude-x","usage":{"input_tokens":11,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: ping
data: {"type":"ping"}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":", world"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}

event: message_stop
data: {"type":"message_stop"}

`

func TestStreamTranslatesEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in request
		_ = json.NewDecoder(r.Body).Decode(&in)
		if !in.Stream {
			t.Error("stream flag not set upstream")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, streamBody)
	}))
	defer srv.Close()

	s, err := New("an", srv.URL, "ak", time.Second).Stream(context.Background(), &api.ChatRequest{Model: "claude-x", Messages: []api.Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var chunks []*api.ChatChunk
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
	}
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4 (role, 2 deltas, final): %+v", len(chunks), chunks)
	}
	if chunks[0].Choices[0].Delta.Role != "assistant" || chunks[0].ID != "msg_s" {
		t.Errorf("first chunk = %+v", chunks[0])
	}
	if chunks[1].Choices[0].Delta.Content+chunks[2].Choices[0].Delta.Content != "Hello, world" {
		t.Error("text deltas not forwarded")
	}
	last := chunks[3]
	if *last.Choices[0].FinishReason != "stop" || last.Usage == nil || last.Usage.PromptTokens != 11 || last.Usage.CompletionTokens != 4 || last.Usage.TotalTokens != 15 {
		t.Errorf("final chunk = %+v usage=%+v", last, last.Usage)
	}
}

func TestStreamErrorEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n")
	}))
	defer srv.Close()
	s, err := New("an", srv.URL, "ak", time.Second).Stream(context.Background(), &api.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recv(); !provider.IsRetryable(err) {
		t.Fatalf("want retryable stream error, got %v", err)
	}
}
