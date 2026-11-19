// Package metrics exposes Prometheus metrics for the gateway.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics groups every collector. Each gateway has its own registry, which keeps tests isolated.
type Metrics struct {
	Registry        *prometheus.Registry
	Requests        *prometheus.CounterVec   // model, status
	RequestDuration *prometheus.HistogramVec // model, stream
	Attempts        *prometheus.CounterVec   // provider, outcome
	UpstreamLatency *prometheus.HistogramVec // provider
	Tokens          *prometheus.CounterVec   // provider, kind
	Cache           *prometheus.CounterVec   // result
	RateLimited     *prometheus.CounterVec   // key, reason
}

// New registers all collectors on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "switchyard_requests_total", Help: "Chat completion requests by model alias and HTTP status.",
		}, []string{"model", "status"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "switchyard_request_duration_seconds", Help: "End-to-end request duration (full stream for streaming requests).",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}, []string{"model", "stream"}),
		Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "switchyard_upstream_attempts_total", Help: "Upstream attempts by provider and outcome (ok, retryable_error, client_error).",
		}, []string{"provider", "outcome"}),
		UpstreamLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "switchyard_upstream_latency_seconds", Help: "Time for an upstream to respond (to first byte for streams).",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"provider"}),
		Tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "switchyard_tokens_total", Help: "Tokens processed by provider and kind (prompt, completion).",
		}, []string{"provider", "kind"}),
		Cache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "switchyard_cache_requests_total", Help: "Response cache lookups by result (hit, miss).",
		}, []string{"result"}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "switchyard_rate_limited_total", Help: "Requests rejected by limits, by key and reason (rpm, budget).",
		}, []string{"key", "reason"}),
	}
	reg.MustRegister(m.Requests, m.RequestDuration, m.Attempts, m.UpstreamLatency, m.Tokens, m.Cache, m.RateLimited,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// RegisterBreakerGauge reports breaker states (0 closed, 1 half-open, 2 open), read at scrape time.
func (m *Metrics) RegisterBreakerGauge(states func() map[string]int) {
	m.Registry.MustRegister(&breakerCollector{states: states, desc: prometheus.NewDesc(
		"switchyard_breaker_state", "Circuit breaker state per provider: 0 closed, 1 half-open, 2 open.", []string{"provider"}, nil)})
}

// Handler serves the metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

type breakerCollector struct {
	states func() map[string]int
	desc   *prometheus.Desc
}

func (c *breakerCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *breakerCollector) Collect(ch chan<- prometheus.Metric) {
	for name, v := range c.states() {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(v), name)
	}
}
