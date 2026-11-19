// Package cache is a size-bounded LRU cache with per-entry expiry for completed responses.
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
)

// Cache stores responses keyed by a hash of the request.
type Cache struct {
	mu    sync.Mutex
	ll    *list.List
	items map[string]*list.Element
	max   int
	ttl   time.Duration
	now   func() time.Time
}

type entry struct {
	key     string
	value   *api.ChatResponse
	expires time.Time
}

// New creates a cache holding up to max entries for ttl each; now may be nil.
func New(max int, ttl time.Duration, now func() time.Time) *Cache {
	if now == nil {
		now = time.Now
	}
	return &Cache{ll: list.New(), items: map[string]*list.Element{}, max: max, ttl: ttl, now: now}
}

// Get returns a fresh entry and marks it recently used.
func (c *Cache) Get(key string) (*api.ChatResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if !c.now().Before(e.expires) {
		c.ll.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e.value, true
}

// Set stores a value, evicting the least recently used entry when full.
func (c *Cache) Set(key string, v *api.ChatResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	exp := c.now().Add(c.ttl)
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		e.value, e.expires = v, exp
		c.ll.MoveToFront(el)
		return
	}
	c.items[key] = c.ll.PushFront(&entry{key: key, value: v, expires: exp})
	for c.ll.Len() > c.max {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*entry).key)
	}
}

// Len returns the number of entries (including expired ones not yet evicted).
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Key derives a cache key from everything that affects the answer. Fields like user and
// stream don't change the completion, so they are left out.
func Key(r *api.ChatRequest) string {
	b, _ := json.Marshal(struct {
		Model       string        `json:"m"`
		Messages    []api.Message `json:"msgs"`
		Temperature *float64      `json:"t,omitempty"`
		TopP        *float64      `json:"p,omitempty"`
		MaxTokens   *int          `json:"mt,omitempty"`
		Stop        []string      `json:"s,omitempty"`
	}{r.Model, r.Messages, r.Temperature, r.TopP, r.MaxTokens, r.Stop})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
