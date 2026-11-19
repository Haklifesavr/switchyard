// Package router resolves client model names to upstream targets and sends requests with
// retries, fallback across providers and per-provider circuit breaking.
package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/provider"
)

// ErrUnknownModel means the requested model is neither a configured alias nor provider/model.
var ErrUnknownModel = errors.New("unknown model")

// ErrCircuitOpen means a target was skipped because its provider's breaker is open.
var ErrCircuitOpen = errors.New("circuit open")

// Target is a concrete upstream for a model alias.
type Target struct {
	Provider string
	Model    string
}

// Result describes how a request was served.
type Result struct {
	Provider string
	Model    string
	Attempts int
}

// AttemptFunc observes every upstream attempt (for metrics and logs).
type AttemptFunc func(provider, model string, err error, took time.Duration)

// Options configure a Router.
type Options struct {
	Attempts  int           // attempts per target before falling back
	Backoff   time.Duration // base backoff between attempts (exponential, with jitter)
	Threshold int           // breaker failure threshold
	Cooldown  time.Duration // breaker cooldown
	OnAttempt AttemptFunc
	// Sleep and Now are injectable for tests.
	Sleep func(context.Context, time.Duration) error
	Now   func() time.Time
}

// Router routes chat requests.
type Router struct {
	routes    map[string][]Target
	providers map[string]provider.Provider
	breakers  map[string]*Breaker
	opts      Options
}

// New builds a router. Every target must reference a provider in providers.
func New(providers map[string]provider.Provider, routes map[string][]Target, opts Options) (*Router, error) {
	if opts.Attempts < 1 {
		opts.Attempts = 1
	}
	if opts.Threshold < 1 {
		opts.Threshold = 5
	}
	if opts.Sleep == nil {
		opts.Sleep = sleep
	}
	r := &Router{routes: routes, providers: providers, breakers: map[string]*Breaker{}, opts: opts}
	for name := range providers {
		r.breakers[name] = NewBreaker(opts.Threshold, opts.Cooldown, opts.Now)
	}
	for alias, ts := range routes {
		for _, t := range ts {
			if _, ok := providers[t.Provider]; !ok {
				return nil, fmt.Errorf("model %q: unknown provider %q", alias, t.Provider)
			}
		}
	}
	return r, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Models lists client-facing model names (aliases), sorted.
func (r *Router) Models() []string {
	out := make([]string, 0, len(r.routes))
	for name := range r.routes {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// BreakerStates reports each provider's breaker state, for metrics and health checks.
func (r *Router) BreakerStates() map[string]State {
	out := make(map[string]State, len(r.breakers))
	for name, b := range r.breakers {
		out[name] = b.State()
	}
	return out
}

// Resolve returns the targets for a model: a configured alias, or "provider/model" to address
// a provider's model directly.
func (r *Router) Resolve(model string) ([]Target, error) {
	if ts, ok := r.routes[model]; ok {
		return ts, nil
	}
	if p, m, ok := strings.Cut(model, "/"); ok && m != "" {
		if _, exists := r.providers[p]; exists {
			return []Target{{Provider: p, Model: m}}, nil
		}
	}
	return nil, fmt.Errorf("%w %q", ErrUnknownModel, model)
}

// backoff returns base * 2^(attempt-1), scaled by a random factor in [0.5, 1.5).
func (r *Router) backoff(attempt int) time.Duration {
	d := r.opts.Backoff << (attempt - 1)
	return time.Duration(float64(d) * (0.5 + rand.Float64()))
}

// try runs call against each target in order with retries, breakers and fallback.
func try[T any](ctx context.Context, r *Router, model string, call func(provider.Provider, *api.ChatRequest) (T, error), req *api.ChatRequest) (T, Result, error) {
	var zero T
	targets, err := r.Resolve(model)
	if err != nil {
		return zero, Result{}, err
	}
	var res Result
	var lastErr error
	for _, t := range targets {
		b := r.breakers[t.Provider]
		p := r.providers[t.Provider]
		for attempt := 1; attempt <= r.opts.Attempts; attempt++ {
			if err := ctx.Err(); err != nil {
				return zero, res, err
			}
			if !b.Allow() {
				lastErr = fmt.Errorf("%s: %w", t.Provider, ErrCircuitOpen)
				break
			}
			res.Attempts++
			out := req.Clone()
			out.Model = t.Model
			start := time.Now()
			v, err := call(p, out)
			if r.opts.OnAttempt != nil {
				r.opts.OnAttempt(t.Provider, t.Model, err, time.Since(start))
			}
			if err == nil {
				b.Success()
				res.Provider, res.Model = t.Provider, t.Model
				return v, res, nil
			}
			if cerr := ctx.Err(); cerr != nil { // client went away: don't blame the provider
				b.Release()
				return zero, res, cerr
			}
			if !provider.IsRetryable(err) {
				b.Release()
				return zero, res, err // bad request, auth, etc.: falling back won't help
			}
			b.Failure()
			lastErr = err
			if attempt < r.opts.Attempts {
				if serr := r.opts.Sleep(ctx, r.backoff(attempt)); serr != nil {
					return zero, res, serr
				}
			}
		}
	}
	return zero, res, fmt.Errorf("all upstreams failed for %q: %w", model, lastErr)
}

// Complete serves a non-streaming request.
func (r *Router) Complete(ctx context.Context, req *api.ChatRequest) (*api.ChatResponse, Result, error) {
	return try(ctx, r, req.Model, func(p provider.Provider, rq *api.ChatRequest) (*api.ChatResponse, error) {
		return p.Complete(ctx, rq)
	}, req)
}

// Stream opens a streaming request. Fallback is possible until the upstream accepts the
// request; once chunks flow, a mid-stream failure ends the response.
func (r *Router) Stream(ctx context.Context, req *api.ChatRequest) (provider.Stream, Result, error) {
	return try(ctx, r, req.Model, func(p provider.Provider, rq *api.ChatRequest) (provider.Stream, error) {
		return p.Stream(ctx, rq)
	}, req)
}
