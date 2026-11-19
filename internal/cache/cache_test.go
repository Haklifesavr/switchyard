package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
)

func resp(id string) *api.ChatResponse { return &api.ChatResponse{ID: id} }

func TestLRUEviction(t *testing.T) {
	c := New(2, time.Hour, nil)
	c.Set("a", resp("a"))
	c.Set("b", resp("b"))
	c.Get("a") // a is now most recently used
	c.Set("c", resp("c"))
	if _, ok := c.Get("b"); ok {
		t.Error("b should have been evicted")
	}
	for _, k := range []string{"a", "c"} {
		if v, ok := c.Get(k); !ok || v.ID != k {
			t.Errorf("%s missing", k)
		}
	}
}

func TestExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	c := New(10, time.Minute, func() time.Time { return now })
	c.Set("k", resp("k"))
	now = now.Add(59 * time.Second)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("entry expired early")
	}
	now = now.Add(time.Second)
	if _, ok := c.Get("k"); ok || c.Len() != 0 {
		t.Fatal("expired entry should be gone")
	}
}

func TestKeyIgnoresIrrelevantFields(t *testing.T) {
	temp := 0.0
	base := &api.ChatRequest{Model: "m", Messages: []api.Message{{Role: "user", Content: "hi"}}, Temperature: &temp}
	same := base.Clone()
	same.User, same.Stream = "alice", true
	if Key(base) != Key(same) {
		t.Error("user/stream should not affect the key")
	}
	diff := base.Clone()
	diff.Messages[0].Content = "hello"
	if Key(base) == Key(diff) {
		t.Error("different messages must produce different keys")
	}
}

func BenchmarkGetHit(b *testing.B) {
	c := New(10_000, time.Hour, nil)
	for i := 0; i < 10_000; i++ {
		c.Set(fmt.Sprint(i), resp("x"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprint(i % 10_000))
	}
}
