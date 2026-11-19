// Package sse reads and writes Server-Sent Events, the framing used by streaming LLM APIs.
package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Event is one SSE event. Name is empty when the server sends no "event:" field.
type Event struct {
	Name string
	Data string
}

// Reader parses an SSE stream.
type Reader struct {
	s *bufio.Scanner
}

// NewReader wraps r. Lines up to 1 MiB are supported, which comfortably fits LLM chunks.
func NewReader(r io.Reader) *Reader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64<<10), 1<<20)
	return &Reader{s: s}
}

// Next returns the next event, or io.EOF when the stream ends.
func (r *Reader) Next() (Event, error) {
	var ev Event
	var data []string
	hasField := false
	for r.s.Scan() {
		line := r.s.Text()
		if line == "" {
			if hasField {
				ev.Data = strings.Join(data, "\n")
				return ev, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") { // comment / keep-alive
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			ev.Name = value
			hasField = true
		case "data":
			data = append(data, value)
			hasField = true
		}
	}
	if err := r.s.Err(); err != nil {
		return Event{}, err
	}
	if hasField { // stream ended without a trailing blank line
		ev.Data = strings.Join(data, "\n")
		return ev, nil
	}
	return Event{}, io.EOF
}

// Writer writes SSE events to an HTTP response and flushes after each one.
type Writer struct {
	w io.Writer
	f http.Flusher
}

// NewWriter sets streaming headers on w. It fails if w cannot flush.
func NewWriter(w http.ResponseWriter) (*Writer, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("response writer does not support flushing")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	return &Writer{w: w, f: f}, nil
}

// Data writes a "data:" event. Multi-line payloads are split per the SSE spec.
func (w *Writer) Data(payload []byte) error {
	var b bytes.Buffer
	for _, line := range bytes.Split(payload, []byte("\n")) {
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	if _, err := w.w.Write(b.Bytes()); err != nil {
		return err
	}
	w.f.Flush()
	return nil
}
