// Package limit implements per-key request rate limits (token bucket) and daily token budgets.
// State is in memory, which suits a single gateway instance; a shared store (e.g. Redis) would
// slot in behind the same methods for multi-instance deployments.
package limit

import (
	"math"
	"sync"
	"time"
)

// RateLimiter is a keyed token bucket: each key holds up to rpm requests, refilled continuously.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter creates a limiter; now may be nil.
func NewRateLimiter(now func() time.Time) *RateLimiter {
	if now == nil {
		now = time.Now
	}
	return &RateLimiter{buckets: map[string]*bucket{}, now: now}
}

// Allow takes one request from key's bucket. rpm <= 0 means unlimited. When denied, it
// returns how long until a request would be allowed.
func (l *RateLimiter) Allow(key string, rpm int) (bool, time.Duration) {
	if rpm <= 0 {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	capacity := float64(rpm)
	perSecond := capacity / 60
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: capacity, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(capacity, b.tokens+now.Sub(b.last).Seconds()*perSecond)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / perSecond * float64(time.Second))
	return false, wait
}

// Budget tracks tokens used per key per UTC day.
type Budget struct {
	mu   sync.Mutex
	day  string
	used map[string]int
	now  func() time.Time
}

// NewBudget creates a budget tracker; now may be nil.
func NewBudget(now func() time.Time) *Budget {
	if now == nil {
		now = time.Now
	}
	return &Budget{used: map[string]int{}, now: now}
}

func (b *Budget) roll() {
	if d := b.now().UTC().Format("2006-01-02"); d != b.day {
		b.day, b.used = d, map[string]int{}
	}
}

// Remaining returns tokens left today for key. limit <= 0 means unlimited (returns -1).
func (b *Budget) Remaining(key string, limit int) int {
	if limit <= 0 {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	if r := limit - b.used[key]; r > 0 {
		return r
	}
	return 0
}

// Add records tokens used by key.
func (b *Budget) Add(key string, tokens int) {
	if tokens <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	b.used[key] += tokens
}

// ResetIn returns the time until budgets reset (next UTC midnight).
func (b *Budget) ResetIn() time.Duration {
	now := b.now().UTC()
	next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	return next.Sub(now)
}
