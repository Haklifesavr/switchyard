package router

import (
	"sync"
	"time"
)

// State is a circuit breaker state.
type State int

// Breaker states.
const (
	Closed   State = iota // healthy: requests flow
	Open                  // failing: requests are skipped until the cooldown passes
	HalfOpen              // probing: one request is let through to test recovery
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Breaker is a per-provider circuit breaker. After `threshold` consecutive failures it opens;
// after `cooldown` it lets a single probe through, closing on success and reopening on failure.
type Breaker struct {
	mu        sync.Mutex
	state     State
	failures  int
	openedAt  time.Time
	probing   bool
	threshold int
	cooldown  time.Duration
	now       func() time.Time
}

// NewBreaker creates a breaker. now may be nil (defaults to time.Now); tests inject a fake clock.
func NewBreaker(threshold int, cooldown time.Duration, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: now}
}

// Allow reports whether a request may be sent now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state, b.probing = HalfOpen, true
		return true
	case HalfOpen:
		if b.probing {
			return false // one probe at a time
		}
		b.probing = true
		return true
	default:
		return true
	}
}

// Success records a healthy response and closes the breaker.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.probing = Closed, 0, false
}

// Failure records an upstream failure.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if b.state == HalfOpen {
		b.state, b.openedAt = Open, b.now()
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state, b.openedAt = Open, b.now()
	}
}

// Release frees a half-open probe slot without judging health (e.g. the client cancelled).
func (b *Breaker) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
}

// State returns the current state, accounting for an elapsed cooldown.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cooldown {
		return HalfOpen
	}
	return b.state
}
