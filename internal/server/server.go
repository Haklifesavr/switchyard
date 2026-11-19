// Package server is Switchyard's HTTP layer: an OpenAI-compatible API in front of the router,
// with authentication, limits, caching, streaming relay, metrics and request logging.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/auth"
	"github.com/Haklifesavr/switchyard/internal/cache"
	"github.com/Haklifesavr/switchyard/internal/limit"
	"github.com/Haklifesavr/switchyard/internal/metrics"
	"github.com/Haklifesavr/switchyard/internal/provider"
	"github.com/Haklifesavr/switchyard/internal/router"
	"github.com/Haklifesavr/switchyard/internal/sse"
)

// maxBodyBytes caps request bodies; chat requests are small, and this blocks memory abuse.
const maxBodyBytes = 1 << 20

// Server wires the gateway together.
type Server struct {
	router  *router.Router
	auth    *auth.Authenticator
	rate    *limit.RateLimiter
	budget  *limit.Budget
	cache   *cache.Cache // nil when disabled
	metrics *metrics.Metrics
	log     *slog.Logger
}

// Options configure a Server.
type Options struct {
	Router  *router.Router
	Auth    *auth.Authenticator
	Rate    *limit.RateLimiter
	Budget  *limit.Budget
	Cache   *cache.Cache
	Metrics *metrics.Metrics
	Logger  *slog.Logger
}

// New creates a server.
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.Rate == nil {
		o.Rate = limit.NewRateLimiter(nil)
	}
	if o.Budget == nil {
		o.Budget = limit.NewBudget(nil)
	}
	if o.Metrics == nil {
		o.Metrics = metrics.New()
	}
	return &Server{router: o.Router, auth: o.Auth, rate: o.Rate, budget: o.Budget, cache: o.Cache, metrics: o.Metrics, log: o.Logger}
}

// Handler returns the HTTP handler with middleware applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("GET /v1/models", s.models)
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	return s.withRequestID(s.withRecover(mux))
}

// ready fails only when every provider's breaker is open, so orchestrators can stop sending
// traffic to an instance that can't serve anything.
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	for _, st := range s.router.BreakerStates() {
		if st != router.Open {
			_, _ = io.WriteString(w, "ready\n")
			return
		}
	}
	http.Error(w, "all upstream providers are unavailable", http.StatusServiceUnavailable)
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	if _, err := s.auth.Authenticate(r); err != nil {
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", err.Error())
		return
	}
	list := api.ModelList{Object: "list"}
	for _, m := range s.router.Models() {
		list.Data = append(list.Data, api.Model{ID: m, Object: "model", OwnedBy: "switchyard"})
	}
	writeJSON(w, http.StatusOK, list)
}

// reqLog accumulates per-request fields for one structured log line.
type reqLog struct {
	key, model, provider, cache string
	attempts, tokens            int
	stream                      bool
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	w = rec
	var rl reqLog
	defer func() {
		model := rl.model
		if model == "" {
			model = "unknown"
		}
		s.metrics.Requests.WithLabelValues(model, strconv.Itoa(rec.status)).Inc()
		s.metrics.RequestDuration.WithLabelValues(model, strconv.FormatBool(rl.stream)).Observe(time.Since(start).Seconds())
		s.log.LogAttrs(r.Context(), slog.LevelInfo, "chat completion",
			slog.String("request_id", requestID(r.Context())), slog.String("key", rl.key),
			slog.String("model", rl.model), slog.String("provider", rl.provider), slog.Int("status", rec.status),
			slog.Bool("stream", rl.stream), slog.String("cache", rl.cache), slog.Int("attempts", rl.attempts),
			slog.Int("tokens", rl.tokens), slog.Duration("duration", time.Since(start)))
	}()

	key, err := s.auth.Authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", err.Error())
		return
	}
	rl.key = key.Name

	if ok, wait := s.rate.Allow(key.Name, key.RequestsPerMinute); !ok {
		s.metrics.RateLimited.WithLabelValues(key.Name, "rpm").Inc()
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded",
			fmt.Sprintf("rate limit of %d requests per minute exceeded", key.RequestsPerMinute))
		return
	}
	if s.budget.Remaining(key.Name, key.TokensPerDay) == 0 {
		s.metrics.RateLimited.WithLabelValues(key.Name, "budget").Inc()
		w.Header().Set("Retry-After", strconv.Itoa(int(s.budget.ResetIn().Seconds())))
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "token_budget_exceeded",
			fmt.Sprintf("daily token budget of %d is used up; it resets at 00:00 UTC", key.TokensPerDay))
		return
	}

	req, err := decodeRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	rl.model, rl.stream = req.Model, req.Stream
	if _, err := s.router.Resolve(req.Model); err != nil {
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", err.Error())
		return
	}

	if req.Stream {
		s.stream(w, r, req, key, &rl)
		return
	}

	useCache := s.cache != nil && !strings.Contains(r.Header.Get("Cache-Control"), "no-cache")
	var cacheKey string
	if useCache {
		cacheKey = cache.Key(req)
		if hit, ok := s.cache.Get(cacheKey); ok {
			s.metrics.Cache.WithLabelValues("hit").Inc()
			rl.cache = "hit"
			w.Header().Set("X-Switchyard-Cache", "hit")
			writeJSON(w, http.StatusOK, hit)
			return
		}
		s.metrics.Cache.WithLabelValues("miss").Inc()
		rl.cache = "miss"
		w.Header().Set("X-Switchyard-Cache", "miss")
	}

	resp, res, err := s.router.Complete(r.Context(), req)
	rl.provider, rl.attempts = res.Provider, res.Attempts
	if err != nil {
		s.writeUpstreamError(w, r, err)
		return
	}
	rl.tokens = resp.Usage.TotalTokens
	s.account(key, res.Provider, resp.Usage)
	if useCache {
		s.cache.Set(cacheKey, resp)
	}
	setRouteHeaders(w, res)
	writeJSON(w, http.StatusOK, resp)
}

// stream relays an upstream stream to the client as OpenAI-style SSE.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, req *api.ChatRequest, key *auth.Key, rl *reqLog) {
	st, res, err := s.router.Stream(r.Context(), req)
	rl.provider, rl.attempts = res.Provider, res.Attempts
	if err != nil {
		s.writeUpstreamError(w, r, err)
		return
	}
	defer st.Close()

	setRouteHeaders(w, res)
	out, err := sse.NewWriter(w)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "server_error", "streaming_unsupported", err.Error())
		return
	}
	wantUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage
	var usage *api.Usage
	var text strings.Builder
	for {
		c, err := st.Recv()
		if errors.Is(err, io.EOF) {
			_ = out.Data([]byte("[DONE]"))
			break
		}
		if err != nil {
			// Headers are already sent, so report the failure in-band and end the stream.
			if r.Context().Err() == nil {
				s.log.Warn("stream interrupted", "request_id", requestID(r.Context()), "provider", res.Provider, "error", err)
				_ = out.Data(api.MustJSON(api.ErrorBody{Error: api.ErrorDetail{Message: "upstream stream interrupted", Type: "server_error", Code: "stream_interrupted"}}))
			}
			break
		}
		if c.Usage != nil {
			u := *c.Usage
			usage = &u
		}
		for _, ch := range c.Choices {
			text.WriteString(ch.Delta.Content)
		}
		// Only forward usage if the client asked for it, matching OpenAI's behaviour.
		if !wantUsage {
			if len(c.Choices) == 0 {
				continue
			}
			c.Usage = nil
		}
		if err := out.Data(api.MustJSON(c)); err != nil {
			break // client disconnected
		}
	}
	if usage == nil { // upstream didn't report usage: estimate so budgets still apply
		p := provider.PromptTokens(req)
		c := provider.EstimateTokens(text.String())
		usage = &api.Usage{PromptTokens: p, CompletionTokens: c, TotalTokens: p + c}
	}
	rl.tokens = usage.TotalTokens
	s.account(key, res.Provider, *usage)
}

func (s *Server) account(key *auth.Key, prov string, u api.Usage) {
	s.budget.Add(key.Name, u.TotalTokens)
	s.metrics.Tokens.WithLabelValues(prov, "prompt").Add(float64(u.PromptTokens))
	s.metrics.Tokens.WithLabelValues(prov, "completion").Add(float64(u.CompletionTokens))
}

// RecordAttempt is the router's AttemptFunc, feeding upstream metrics.
func (s *Server) RecordAttempt(prov, _ string, err error, took time.Duration) {
	outcome := "ok"
	switch {
	case err == nil:
	case provider.IsRetryable(err):
		outcome = "retryable_error"
	default:
		outcome = "client_error"
	}
	s.metrics.Attempts.WithLabelValues(prov, outcome).Inc()
	s.metrics.UpstreamLatency.WithLabelValues(prov).Observe(took.Seconds())
}

// writeUpstreamError maps routing failures to client-facing HTTP errors.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		w.WriteHeader(499) // client closed the request (nginx convention); nobody is listening
		return
	}
	var pe *provider.Error
	switch {
	case errors.Is(err, router.ErrUnknownModel):
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", err.Error())
	case errors.As(err, &pe) && !pe.Retryable && pe.Status == http.StatusBadRequest:
		writeError(w, http.StatusBadRequest, "invalid_request_error", "upstream_rejected_request", pe.Message)
	case errors.As(err, &pe) && !pe.Retryable && (pe.Status == http.StatusUnauthorized || pe.Status == http.StatusForbidden):
		// The upstream rejected *our* credentials: a gateway misconfiguration, not the client's fault.
		s.log.Error("upstream rejected gateway credentials", "provider", pe.Provider, "status", pe.Status)
		writeError(w, http.StatusBadGateway, "server_error", "upstream_auth_failed", "the gateway is misconfigured for this model; contact the operator")
	case errors.As(err, &pe) && !pe.Retryable && pe.Status != 0:
		writeError(w, pe.Status, "invalid_request_error", "upstream_error", pe.Message)
	default:
		s.log.Warn("all upstreams failed", "request_id", requestID(r.Context()), "error", err)
		writeError(w, http.StatusBadGateway, "server_error", "upstream_unavailable", "all upstream providers failed; try again shortly")
	}
}

var validRoles = map[string]bool{"system": true, "developer": true, "user": true, "assistant": true, "tool": true}

func decodeRequest(w http.ResponseWriter, r *http.Request) (*api.ChatRequest, error) {
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req api.ChatRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, fmt.Errorf("request body exceeds %d bytes", maxBodyBytes)
		}
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if req.Model == "" {
		return nil, errors.New("model is required")
	}
	if len(req.Messages) == 0 {
		return nil, errors.New("messages must contain at least one message")
	}
	for i, m := range req.Messages {
		if !validRoles[m.Role] {
			return nil, fmt.Errorf("messages[%d]: unsupported role %q", i, m.Role)
		}
	}
	if req.MaxTokens != nil && *req.MaxTokens < 1 {
		return nil, errors.New("max_tokens must be at least 1")
	}
	return &req, nil
}

func setRouteHeaders(w http.ResponseWriter, res router.Result) {
	w.Header().Set("X-Switchyard-Provider", res.Provider)
	w.Header().Set("X-Switchyard-Upstream-Model", res.Model)
	w.Header().Set("X-Switchyard-Attempts", strconv.Itoa(res.Attempts))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, typ, code, msg string) {
	writeJSON(w, status, api.ErrorBody{Error: api.ErrorDetail{Message: msg, Type: typ, Code: code}})
}

// statusRecorder captures the response status for logs and metrics while still supporting
// streaming (Flush) through the wrapped writer.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// --- middleware ---

type ctxKey struct{}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// withRequestID propagates a caller's X-Request-Id (if sane) or generates one.
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if len(id) == 0 || len(id) > 128 || strings.ContainsAny(id, " \r\n\t") {
			id = newID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

func (s *Server) withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v) // let net/http abort the connection as intended
				}
				s.log.Error("panic serving request", "request_id", requestID(r.Context()), "panic", v)
				writeError(w, http.StatusInternalServerError, "server_error", "internal_error", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
