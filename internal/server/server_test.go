package server

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Haklifesavr/switchyard/internal/api"
	"github.com/Haklifesavr/switchyard/internal/auth"
	"github.com/Haklifesavr/switchyard/internal/cache"
	"github.com/Haklifesavr/switchyard/internal/config"
	"github.com/Haklifesavr/switchyard/internal/metrics"
	"github.com/Haklifesavr/switchyard/internal/provider"
	"github.com/Haklifesavr/switchyard/internal/provider/mock"
	"github.com/Haklifesavr/switchyard/internal/provider/openai"
	"github.com/Haklifesavr/switchyard/internal/router"
)

const testKey = "sy_test_key"

type env struct {
	srv *httptest.Server
	m   *metrics.Metrics
}

func newEnv(t *testing.T, keys []config.Key, extra map[string]provider.Provider, routes map[string][]router.Target) *env {
	t.Helper()
	providers := map[string]provider.Provider{
		"good": mock.New("good", 0, 0),
		"down": mock.New("down", 0, 1), // always fails with a retryable 503
	}
	for k, v := range extra {
		providers[k] = v
	}
	all := map[string][]router.Target{
		"demo":      {{Provider: "good", Model: "mock-1"}},
		"resilient": {{Provider: "down", Model: "mock-down"}, {Provider: "good", Model: "mock-1"}},
	}
	for k, v := range routes {
		all[k] = v
	}
	if keys == nil {
		keys = []config.Key{{Name: "test", SHA256: auth.Hash(testKey)}}
	}
	m := metrics.New()
	var s *Server
	rt, err := router.New(providers, all, router.Options{
		Attempts: 1, Threshold: 100, Cooldown: time.Minute,
		OnAttempt: func(p, model string, err error, d time.Duration) { s.RecordAttempt(p, model, err, d) },
	})
	if err != nil {
		t.Fatal(err)
	}
	m.RegisterBreakerGauge(func() map[string]int {
		out := map[string]int{}
		for k, st := range rt.BreakerStates() {
			out[k] = int(st)
		}
		return out
	})
	s = New(Options{Router: rt, Auth: auth.New(config.Auth{Keys: keys}), Cache: cache.New(100, time.Minute, nil), Metrics: m})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &env{srv: srv, m: m}
}

func (e *env) post(t *testing.T, body string, headers ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", e.srv.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testKey)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func decode[T any](t *testing.T, r io.Reader) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

const hello = `{"model":"demo","messages":[{"role":"user","content":"hello"}]}`

func TestRequiresAPIKey(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	resp, err := http.Post(e.srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(hello))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := decode[api.ErrorBody](t, resp.Body)
	if resp.StatusCode != 401 || body.Error.Type != "authentication_error" {
		t.Fatalf("status %d, body %+v", resp.StatusCode, body)
	}
}

func TestModelsList(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	req, _ := http.NewRequest("GET", e.srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	list := decode[api.ModelList](t, resp.Body)
	if len(list.Data) != 2 || list.Data[0].ID != "demo" || list.Data[1].ID != "resilient" {
		t.Fatalf("models = %+v", list)
	}
}

func TestCompletionHeadersAndCache(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	first := e.post(t, hello, "X-Request-Id", "req-123")
	out := decode[api.ChatResponse](t, first.Body)
	if first.StatusCode != 200 || out.Choices[0].Message.Content != "Echo from mock-1: hello" {
		t.Fatalf("status %d, out %+v", first.StatusCode, out)
	}
	h := first.Header
	if h.Get("X-Switchyard-Provider") != "good" || h.Get("X-Switchyard-Upstream-Model") != "mock-1" ||
		h.Get("X-Switchyard-Cache") != "miss" || h.Get("X-Request-Id") != "req-123" {
		t.Errorf("headers = %v", h)
	}
	if again := e.post(t, hello); again.Header.Get("X-Switchyard-Cache") != "hit" {
		t.Error("identical request should be a cache hit")
	}
	if bypass := e.post(t, hello, "Cache-Control", "no-cache"); bypass.Header.Get("X-Switchyard-Cache") != "" {
		t.Error("Cache-Control: no-cache should bypass the cache")
	}
}

func readSSE(t *testing.T, r io.Reader) []string {
	t.Helper()
	var data []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			data = append(data, d)
		}
	}
	return data
}

func TestStreaming(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	resp := e.post(t, `{"model":"demo","stream":true,"messages":[{"role":"user","content":"stream this"}]}`)
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	events := readSSE(t, resp.Body)
	if len(events) < 3 || events[len(events)-1] != "[DONE]" {
		t.Fatalf("events = %v", events)
	}
	var text strings.Builder
	for _, ev := range events[:len(events)-1] {
		var c api.ChatChunk
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", ev, err)
		}
		if c.Usage != nil {
			t.Error("usage must not be sent unless stream_options.include_usage is set")
		}
		text.WriteString(c.Choices[0].Delta.Content)
	}
	if text.String() != "Echo from mock-1: stream this" {
		t.Errorf("text = %q", text.String())
	}

	withUsage := e.post(t, `{"model":"demo","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"x"}]}`)
	evs := readSSE(t, withUsage.Body)
	var last api.ChatChunk
	_ = json.Unmarshal([]byte(evs[len(evs)-2]), &last)
	if last.Usage == nil || last.Usage.TotalTokens == 0 {
		t.Errorf("expected usage in final chunk, got %+v", last)
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, []config.Key{{Name: "test", SHA256: auth.Hash(testKey), RequestsPerMinute: 2}}, nil, nil)
	for i := 0; i < 2; i++ {
		if r := e.post(t, hello, "Cache-Control", "no-cache"); r.StatusCode != 200 {
			t.Fatalf("request %d: %d", i, r.StatusCode)
		}
	}
	r := e.post(t, hello)
	body := decode[api.ErrorBody](t, r.Body)
	if r.StatusCode != 429 || body.Error.Code != "rate_limit_exceeded" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d, body %+v, Retry-After %q", r.StatusCode, body, r.Header.Get("Retry-After"))
	}
}

func TestTokenBudget(t *testing.T) {
	e := newEnv(t, []config.Key{{Name: "test", SHA256: auth.Hash(testKey), TokensPerDay: 5}}, nil, nil)
	if r := e.post(t, hello); r.StatusCode != 200 { // uses more than 5 tokens
		t.Fatalf("first request: %d", r.StatusCode)
	}
	r := e.post(t, hello)
	body := decode[api.ErrorBody](t, r.Body)
	if r.StatusCode != 429 || body.Error.Code != "token_budget_exceeded" {
		t.Fatalf("status %d, body %+v", r.StatusCode, body)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	cases := map[string]struct {
		body   string
		status int
		code   string
	}{
		"bad json":      {`{"model":`, 400, "invalid_request"},
		"no model":      {`{"messages":[{"role":"user","content":"x"}]}`, 400, "invalid_request"},
		"no messages":   {`{"model":"demo","messages":[]}`, 400, "invalid_request"},
		"bad role":      {`{"model":"demo","messages":[{"role":"wizard","content":"x"}]}`, 400, "invalid_request"},
		"unknown model": {`{"model":"gpt-99","messages":[{"role":"user","content":"x"}]}`, 404, "model_not_found"},
	}
	for name, c := range cases {
		r := e.post(t, c.body)
		body := decode[api.ErrorBody](t, r.Body)
		if r.StatusCode != c.status || body.Error.Code != c.code {
			t.Errorf("%s: status %d code %q, want %d %q (%s)", name, r.StatusCode, body.Error.Code, c.status, c.code, body.Error.Message)
		}
	}
}

func TestFallbackToHealthyProvider(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	r := e.post(t, `{"model":"resilient","messages":[{"role":"user","content":"hi"}]}`)
	if r.StatusCode != 200 || r.Header.Get("X-Switchyard-Provider") != "good" || r.Header.Get("X-Switchyard-Attempts") != "2" {
		t.Fatalf("status %d headers %v", r.StatusCode, r.Header)
	}
	metricsBody := get(t, e.srv.URL+"/metrics")
	for _, want := range []string{
		`switchyard_upstream_attempts_total{outcome="retryable_error",provider="down"} 1`,
		`switchyard_upstream_attempts_total{outcome="ok",provider="good"} 1`,
		`switchyard_requests_total{model="resilient",status="200"} 1`,
		`switchyard_breaker_state{provider="down"}`,
	} {
		if !strings.Contains(metricsBody, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestUpstreamErrorMapping(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in api.ChatRequest
		_ = json.NewDecoder(r.Body).Decode(&in)
		switch in.Model {
		case "bad-request":
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"max_tokens is too large"}}`)
		case "bad-creds":
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":{"message":"invalid api key"}}`)
		}
	}))
	defer upstream.Close()
	e := newEnv(t, nil, map[string]provider.Provider{"oa": openai.New("oa", upstream.URL, "k", time.Second)}, nil)

	r := e.post(t, `{"model":"oa/bad-request","messages":[{"role":"user","content":"x"}]}`)
	body := decode[api.ErrorBody](t, r.Body)
	if r.StatusCode != 400 || body.Error.Message != "max_tokens is too large" {
		t.Errorf("upstream 400: status %d body %+v", r.StatusCode, body)
	}
	r = e.post(t, `{"model":"oa/bad-creds","messages":[{"role":"user","content":"x"}]}`)
	body = decode[api.ErrorBody](t, r.Body)
	if r.StatusCode != 502 || body.Error.Code != "upstream_auth_failed" || strings.Contains(body.Error.Message, "invalid api key") {
		t.Errorf("upstream 401 must become an opaque 502: status %d body %+v", r.StatusCode, body)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	e := newEnv(t, nil, nil, nil)
	for _, path := range []string{"/healthz", "/readyz"} {
		if body := get(t, e.srv.URL+path); body == "" {
			t.Errorf("%s returned empty body", path)
		}
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	return string(b)
}
