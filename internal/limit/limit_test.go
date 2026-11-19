package limit

import (
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestRateLimiterBurstThenRefill(t *testing.T) {
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	l := NewRateLimiter(clk.Now)
	for i := 0; i < 60; i++ {
		if ok, _ := l.Allow("k", 60); !ok {
			t.Fatalf("request %d of burst denied", i)
		}
	}
	ok, wait := l.Allow("k", 60)
	if ok || wait <= 0 || wait > time.Second {
		t.Fatalf("61st request: ok=%v wait=%v, want denied with wait ~1s", ok, wait)
	}
	if ok, _ := l.Allow("other", 60); !ok {
		t.Fatal("keys must not share buckets")
	}
	clk.Advance(time.Second) // 60 rpm refills one request per second
	if ok, _ := l.Allow("k", 60); !ok {
		t.Fatal("expected refill after 1s")
	}
	if ok, _ := l.Allow("k", 0); !ok {
		t.Fatal("rpm 0 means unlimited")
	}
}

func TestBudgetResetsAtUTCMidnight(t *testing.T) {
	clk := &clock{t: time.Date(2026, 1, 1, 23, 59, 0, 0, time.UTC)}
	b := NewBudget(clk.Now)
	b.Add("k", 900)
	if r := b.Remaining("k", 1000); r != 100 {
		t.Fatalf("remaining = %d", r)
	}
	b.Add("k", 500)
	if r := b.Remaining("k", 1000); r != 0 {
		t.Fatalf("overspent budget should report 0, got %d", r)
	}
	if d := b.ResetIn(); d != time.Minute {
		t.Fatalf("ResetIn = %v", d)
	}
	clk.Advance(2 * time.Minute)
	if r := b.Remaining("k", 1000); r != 1000 {
		t.Fatalf("budget should reset on a new UTC day, got %d", r)
	}
	if r := b.Remaining("k", 0); r != -1 {
		t.Fatalf("limit 0 means unlimited, got %d", r)
	}
}

func TestRateLimiterConcurrent(t *testing.T) {
	l := NewRateLimiter(nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.Allow("k", 100); ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed < 100 || allowed > 101 { // burst of 100, plus at most a sliver of refill
		t.Fatalf("allowed %d of 200 with rpm=100", allowed)
	}
}
