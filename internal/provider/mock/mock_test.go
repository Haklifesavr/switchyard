package mock

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
)

func TestCompleteEchoesLastUserMessage(t *testing.T) {
	p := New("mock", 0, 0)
	out, err := p.Complete(context.Background(), &api.ChatRequest{Model: "m", Messages: []api.Message{
		{Role: "system", Content: "be nice"}, {Role: "user", Content: "first"}, {Role: "user", Content: "second"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Choices[0].Message.Content; got != "Echo from m: second" {
		t.Errorf("reply = %q", got)
	}
	if out.Usage.TotalTokens != out.Usage.PromptTokens+out.Usage.CompletionTokens || out.Usage.TotalTokens == 0 {
		t.Errorf("bad usage %+v", out.Usage)
	}
}

func TestStreamReassemblesToSameText(t *testing.T) {
	p := New("mock", 0, 0)
	r := &api.ChatRequest{Model: "m", Messages: []api.Message{{Role: "user", Content: "stream me please"}}}
	s, err := p.Stream(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	var last *api.ChatChunk
	for {
		c, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString(c.Choices[0].Delta.Content)
		last = c
	}
	if b.String() != "Echo from m: stream me please" {
		t.Errorf("text = %q", b.String())
	}
	if last.Usage == nil || *last.Choices[0].FinishReason != "stop" {
		t.Errorf("final chunk missing usage or finish reason: %+v", last)
	}
}

func TestFailureRateOneAlwaysFailsRetryably(t *testing.T) {
	p := New("mock", 0, 1)
	_, err := p.Complete(context.Background(), &api.ChatRequest{Model: "m"})
	if !provider.IsRetryable(err) {
		t.Fatalf("want retryable simulated outage, got %v", err)
	}
}
