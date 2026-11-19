package sse

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReaderParsesEvents(t *testing.T) {
	in := ": keep-alive\n\nevent: message_start\ndata: {\"a\":1}\n\ndata: line1\ndata: line2\n\ndata: [DONE]"
	r := NewReader(strings.NewReader(in))
	want := []Event{{"message_start", `{"a":1}`}, {"", "line1\nline2"}, {"", "[DONE]"}}
	for i, w := range want {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if got != w {
			t.Errorf("event %d = %+v, want %+v", i, got, w)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
}

func TestWriterFramesAndSetsHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	w, err := NewWriter(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Data([]byte("a\nb")); err != nil {
		t.Fatal(err)
	}
	if got := rec.Body.String(); got != "data: a\ndata: b\n\n" {
		t.Errorf("body = %q", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	if !rec.Flushed {
		t.Error("expected flush")
	}
}
