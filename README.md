# Switchyard

[![CI](https://github.com/Haklifesavr/switchyard/actions/workflows/ci.yml/badge.svg)](https://github.com/Haklifesavr/switchyard/actions/workflows/ci.yml)

**A fast, self-hosted LLM gateway written in Go.** Point any OpenAI-compatible client at Switchyard, and it routes each request to OpenAI, Anthropic or a local model, falling back to another provider when one fails, while enforcing per-key rate limits and token budgets.

```
your app ──► Switchyard ──► OpenAI
 (OpenAI SDK)     │    └──► Anthropic   (translated to the Messages API, streaming included)
                  │    └──► Ollama / vLLM / any OpenAI-compatible server
                  └── retries · fallback · circuit breakers · rate limits · budgets · cache · metrics
```

## Why

Applications that depend on one LLM provider inherit its outages, rate limits and pricing. Switchyard sits in between so that:

- **An outage doesn't take you down.** Requests retry with backoff, then fall back to the next provider in the model's list. Providers that keep failing are skipped by a circuit breaker until they recover.
- **Switching providers is a config change.** Clients ask for `smart` or `fast`; the config decides what that means. Code written against the OpenAI SDK works unchanged, even when the answer comes from Anthropic.
- **Costs stay bounded.** Every client key gets a request rate limit and a daily token budget.
- **You can see what's happening.** Prometheus metrics, structured logs with request IDs, and response headers showing which provider served each request.

## Features

- OpenAI-compatible `POST /v1/chat/completions` (JSON and SSE streaming) and `GET /v1/models`
- Providers: **OpenAI** (and any OpenAI-compatible server), **Anthropic** (full request/response and stream translation), and a **mock** provider for demos and tests
- Model aliases with ordered fallback targets, or `provider/model` to address a model directly
- Retries with exponential backoff and jitter; per-provider circuit breakers (closed → open → half-open)
- API keys stored only as SHA-256 hashes; per-key token-bucket rate limits and daily token budgets
- LRU + TTL response cache for non-streaming requests (`Cache-Control: no-cache` bypasses it)
- Prometheus metrics, JSON logs, request ID propagation, `/healthz` and `/readyz`
- Graceful shutdown that drains in-flight requests (including open streams)
- Single ~10 MB static binary, ~12 MB distroless Docker image, minimal dependencies (Prometheus client, YAML)

## Quick start

Requires Go 1.26+, or Docker.

```bash
git clone https://github.com/Haklifesavr/switchyard && cd switchyard
go run ./cmd/switchyard -config config.example.yaml
```

The example config uses two mock providers (one fails 30% of the time), so you can see retries and fallback without any API keys. Send a request with the demo key:

```bash
curl -s localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer sy_demo_key_change_me" \
  -d '{"model": "smart", "messages": [{"role": "user", "content": "Hello!"}]}' -i
```

```
HTTP/1.1 200 OK
X-Switchyard-Provider: primary
X-Switchyard-Upstream-Model: mock-large
X-Switchyard-Attempts: 1
X-Switchyard-Cache: miss
...
```

Streaming works the same way: add `"stream": true`.

Or run it with Prometheus:

```bash
docker compose up --build    # gateway on :8080, Prometheus on :9090
```

### Using real providers

Uncomment the `openai` / `anthropic` providers in the config, add them as targets, and export the keys:

```bash
export OPENAI_API_KEY=sk-...  ANTHROPIC_API_KEY=sk-ant-...
```

Then use any OpenAI SDK with Switchyard as the base URL:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="sy_demo_key_change_me")
reply = client.chat.completions.create(
    model="smart",  # an alias from your config
    messages=[{"role": "user", "content": "Summarise the CAP theorem in one line."}],
)
print(reply.choices[0].message.content)
```

## How a request flows

```mermaid
flowchart LR
    C[Client] --> A{Auth}
    A -- invalid key --> E401[401]
    A --> R{Rate limit<br/>and budget}
    R -- exceeded --> E429[429 + Retry-After]
    R --> V{Valid request<br/>and known model?}
    V -- no --> E4xx[400 / 404]
    V --> K{Cache hit?}
    K -- yes --> OK[200 from cache]
    K -- no --> RT[Router]
    RT --> T1[Target 1]
    T1 -- retryable failure,<br/>after retries --> T2[Target 2]
    T1 -- success --> OK2[200 / SSE stream]
    T2 -- success --> OK2
    T2 -- all failed --> E502[502]
```

**Retry and fallback rules**

| Upstream result | What Switchyard does |
|---|---|
| Success | Return it; close the breaker |
| 429, 408, 5xx, 529, timeout, connection error | Retry the same target (with backoff), then fall back to the next target |
| 400 / 404 / 422 | Return to the client immediately: another provider won't fix a bad request |
| 401 / 403 | Return an opaque **502**: the gateway's own credentials are wrong, which isn't the client's fault, and upstream details aren't leaked |
| Client disconnects | Stop immediately; the provider isn't blamed |

For streaming requests, fallback is possible until the upstream accepts the request. Once tokens are flowing, a mid-stream failure ends the stream with an in-band error event.

## Configuration

See [`config.example.yaml`](config.example.yaml) for a complete, commented example. `${ENV_VAR}` references are expanded at load time, so secrets never need to be in the file. Unknown fields are rejected, and every validation problem is reported at once.

| Section | Key settings |
|---|---|
| `providers` | `type` (`openai`, `anthropic`, `mock`), `api_key`, `base_url`, `timeout` |
| `models` | `name` (what clients request) and an ordered list of `targets` (`provider` + upstream `model`) |
| `auth.keys` | `name`, `sha256`, `requests_per_minute`, `tokens_per_day` (0 = unlimited) |
| `cache` | `enabled`, `ttl`, `max_entries` |
| `retry` | `attempts` per target, base `backoff` |
| `breaker` | `failure_threshold` consecutive failures, `cooldown` before probing |
| `log` | `level`, `format` (`json` or `text`) |

Create client keys with the built-in helper. Only the hash goes in the config:

```bash
$ switchyard gen-key
key:    sy_3f9c...
sha256: 8a1d...
```

## Observability

| Metric | Labels |
|---|---|
| `switchyard_requests_total` | `model`, `status` |
| `switchyard_request_duration_seconds` | `model`, `stream` |
| `switchyard_upstream_attempts_total` | `provider`, `outcome` (`ok`, `retryable_error`, `client_error`) |
| `switchyard_upstream_latency_seconds` | `provider` |
| `switchyard_tokens_total` | `provider`, `kind` (`prompt`, `completion`) |
| `switchyard_cache_requests_total` | `result` (`hit`, `miss`) |
| `switchyard_rate_limited_total` | `key`, `reason` (`rpm`, `budget`) |
| `switchyard_breaker_state` | `provider` (0 closed, 1 half-open, 2 open) |

Each request logs one structured line with its request ID, client key name, model, provider, status, attempts, tokens, cache result and duration. `X-Request-Id` is propagated from the client or generated. `/readyz` reports unready only when every provider's breaker is open.

## Design notes

- **Streaming without a total timeout.** Provider HTTP clients time out waiting for *response headers*, not the whole response, so a stalled upstream fails fast while long generations aren't cut off.
- **One translation layer.** Everything inside the gateway uses OpenAI's wire format; each provider package converts at the edge. Adding a provider means implementing a three-method interface.
- **Errors carry their own retry policy.** Providers return a typed error that says whether retrying can help, so the router never has to guess from status codes.
- **Breakers per provider, not per model.** An outage usually affects a whole provider, so one failing model shouldn't keep hammering the same broken API under a different name.
- **Usage-accurate budgets for streams.** OpenAI streams are asked to include usage, Anthropic usage is read from stream events, and token counts are estimated only as a last resort.
- **In-memory state.** Rate limits, budgets and the cache live in process, which is right for a single instance. A shared store such as Redis would slot in behind the same methods for horizontal scaling (see Roadmap).

## Project layout

```
cmd/switchyard/          entry point: serve, gen-key, hash-key, graceful shutdown
internal/api/            OpenAI-compatible wire types
internal/config/         YAML loading, env expansion, validation
internal/provider/       Provider interface and error classification
  openai/  anthropic/  mock/
internal/router/         alias resolution, retries, fallback, circuit breaker
internal/auth/           hashed API keys
internal/limit/          token-bucket rate limits, daily token budgets
internal/cache/          LRU + TTL response cache
internal/server/         HTTP handlers, middleware, streaming relay
internal/metrics/        Prometheus collectors
internal/sse/            Server-Sent Events reader/writer
```

## Development

```bash
make test      # unit and integration tests
make race      # tests with the race detector (what CI runs)
make lint      # go vet + golangci-lint
make build     # static binary in ./bin
```

Tests use fake upstream servers and an injectable clock, so retries, breaker cooldowns, rate-limit refills and budget resets are tested deterministically and without real API calls.

## Roadmap

- Redis-backed limits, budgets and cache for multi-instance deployments
- Tool/function calling and multimodal content passthrough
- Weighted and latency-based routing across healthy targets
- Per-key model allow-lists and cost tracking in dollars
- OpenTelemetry tracing

## License

[MIT](LICENSE) © 2026 Haris Ahmed Khan
