package router

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
)

// fake replays a script of errors (nil = success) and records calls.
type fake struct {
	name   string
	mu     sync.Mutex
	script []error
	calls  []string // upstream model per call
}

func (f *fake) Name() string { return f.name }

func (f *fake) next(model string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, model)
	if len(f.script) == 0 {
		return nil
	}
	err := f.script[0]
	f.script = f.script[1:]
	return err
}

func (f *fake) Complete(_ context.Context, r *api.ChatRequest) (*api.ChatResponse, error) {
	if err := f.next(r.Model); err != nil {
		return nil, err
	}
	return &api.ChatResponse{Model: r.Model, Choices: []api.Choice{{Message: api.Message{Role: "assistant", Content: f.name}}}}, nil
}

func (f *fake) Stream(_ context.Context, r *api.ChatRequest) (provider.Stream, error) {
	if err := f.next(r.Model); err != nil {
		return nil, err
	}
	return nopStream{}, nil
}

func (f *fake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type nopStream struct{}

func (nopStream) Recv() (*api.ChatChunk, error) { return nil, io.EOF }
func (nopStream) Close() error                  { return nil }

var (
	outage   = &provider.Error{Provider: "x", Status: http.StatusServiceUnavailable, Message: "down", Retryable: true}
	badInput = &provider.Error{Provider: "x", Status: http.StatusBadRequest, Message: "bad", Retryable: false}
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type setup struct {
	r      *Router
	a, b   *fake
	sleeps []time.Duration
	clk    *clock
}

func newSetup(t *testing.T, attempts, threshold int) *setup {
	t.Helper()
	s := &setup{a: &fake{name: "a"}, b: &fake{name: "b"}, clk: &clock{t: time.Unix(0, 0)}}
	var mu sync.Mutex
	r, err := New(
		map[string]provider.Provider{"a": s.a, "b": s.b},
		map[string][]Target{"smart": {{"a", "a-large"}, {"b", "b-large"}}},
		Options{
			Attempts: attempts, Backoff: 100 * time.Millisecond, Threshold: threshold, Cooldown: 30 * time.Second,
			Now: s.clk.Now,
			Sleep: func(ctx context.Context, d time.Duration) error {
				mu.Lock()
				s.sleeps = append(s.sleeps, d)
				mu.Unlock()
				return ctx.Err()
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	s.r = r
	return s
}

func chat(model string) *api.ChatRequest {
	return &api.ChatRequest{Model: model, Messages: []api.Message{{Role: "user", Content: "hi"}}}
}

func TestFallsBackAfterRetriesAreExhausted(t *testing.T) {
	s := newSetup(t, 2, 10)
	s.a.script = []error{outage, outage}
	out, res, err := s.r.Complete(context.Background(), chat("smart"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Provider != "b" || res.Model != "b-large" || res.Attempts != 3 || out.Model != "b-large" {
		t.Errorf("result = %+v, model %q", res, out.Model)
	}
	if len(s.sleeps) != 1 {
		t.Errorf("want 1 backoff between the two attempts on a, got %v", s.sleeps)
	}
	if d := s.sleeps[0]; d < 50*time.Millisecond || d >= 150*time.Millisecond {
		t.Errorf("backoff %v outside jitter range [50ms,150ms)", d)
	}
}

func TestClientErrorsAreNotRetriedOrFallenBack(t *testing.T) {
	s := newSetup(t, 3, 10)
	s.a.script = []error{badInput}
	_, res, err := s.r.Complete(context.Background(), chat("smart"))
	if !errors.Is(err, badInput) {
		t.Fatalf("want the 400 passed through, got %v", err)
	}
	if res.Attempts != 1 || s.b.callCount() != 0 {
		t.Errorf("attempts=%d, b calls=%d", res.Attempts, s.b.callCount())
	}
	if st := s.r.BreakerStates()["a"]; st != Closed {
		t.Errorf("a 400 must not trip the breaker, state=%v", st)
	}
}

func TestResolve(t *testing.T) {
	s := newSetup(t, 1, 10)
	if ts, err := s.r.Resolve("b/b-mini"); err != nil || ts[0] != (Target{"b", "b-mini"}) {
		t.Errorf("direct provider/model: %v %v", ts, err)
	}
	for _, m := range []string{"nope", "zzz/model", "a/"} {
		if _, err := s.r.Resolve(m); !errors.Is(err, ErrUnknownModel) {
			t.Errorf("Resolve(%q) = %v, want ErrUnknownModel", m, err)
		}
	}
	if got := s.r.Models(); strings.Join(got, ",") != "smart" {
		t.Errorf("Models() = %v", got)
	}
}

func TestBreakerOpensSkipsAndRecovers(t *testing.T) {
	s := newSetup(t, 1, 2)
	s.a.script = []error{outage, outage}
	for i := 0; i < 2; i++ { // two failures trip a's breaker; each request falls back to b
		if _, res, err := s.r.Complete(context.Background(), chat("smart")); err != nil || res.Provider != "b" {
			t.Fatalf("request %d: %+v %v", i, res, err)
		}
	}
	if st := s.r.BreakerStates()["a"]; st != Open {
		t.Fatalf("a should be open, got %v", st)
	}
	before := s.a.callCount()
	if _, res, _ := s.r.Complete(context.Background(), chat("smart")); res.Provider != "b" || s.a.callCount() != before {
		t.Fatalf("open breaker should skip a entirely: %+v, a calls %d->%d", res, before, s.a.callCount())
	}
	s.clk.Advance(31 * time.Second) // cooldown over: next request probes a, which now succeeds
	if _, res, err := s.r.Complete(context.Background(), chat("smart")); err != nil || res.Provider != "a" {
		t.Fatalf("probe should go to a: %+v %v", res, err)
	}
	if st := s.r.BreakerStates()["a"]; st != Closed {
		t.Errorf("successful probe should close the breaker, got %v", st)
	}
}

func TestHalfOpenAllowsOneProbeAtATime(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	b := NewBreaker(1, time.Second, clk.Now)
	b.Failure()
	if b.Allow() {
		t.Fatal("open breaker allowed a request")
	}
	clk.Advance(time.Second)
	if !b.Allow() || b.Allow() {
		t.Fatal("half-open should allow exactly one probe")
	}
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe should reopen the breaker")
	}
}

func TestAllTargetsFail(t *testing.T) {
	s := newSetup(t, 1, 10)
	s.a.script = []error{outage}
	s.b.script = []error{outage}
	_, res, err := s.r.Complete(context.Background(), chat("smart"))
	if err == nil || !strings.Contains(err.Error(), `all upstreams failed for "smart"`) || !errors.Is(err, outage) {
		t.Fatalf("got %v", err)
	}
	if res.Attempts != 2 {
		t.Errorf("attempts = %d", res.Attempts)
	}
}

func TestCancelledDuringBackoffStops(t *testing.T) {
	s := newSetup(t, 3, 10)
	s.a.script = []error{outage, outage, outage}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := s.r.Complete(ctx, chat("smart"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancellation, got %v", err)
	}
	if s.b.callCount() != 0 {
		t.Error("should not fall back after the client cancelled")
	}
}

func TestStreamFallsBackOnOpenFailure(t *testing.T) {
	s := newSetup(t, 1, 10)
	s.a.script = []error{outage}
	st, res, err := s.r.Stream(context.Background(), chat("smart"))
	if err != nil || res.Provider != "b" {
		t.Fatalf("%+v %v", res, err)
	}
	_ = st.Close()
}

func TestConcurrentRequestsAreSafe(t *testing.T) {
	s := newSetup(t, 2, 3)
	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := s.r.Complete(context.Background(), chat("smart")); err == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 100 {
		t.Errorf("%d/100 succeeded", ok.Load())
	}
}
