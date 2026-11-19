// Package config loads and validates Switchyard's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the full gateway configuration.
type Config struct {
	Listen    string              `yaml:"listen"`
	Providers map[string]Provider `yaml:"providers"`
	Models    []Model             `yaml:"models"`
	Auth      Auth                `yaml:"auth"`
	Cache     Cache               `yaml:"cache"`
	Retry     Retry               `yaml:"retry"`
	Breaker   Breaker             `yaml:"breaker"`
	Log       Log                 `yaml:"log"`
}

// Provider configures one upstream LLM API.
type Provider struct {
	Type    string        `yaml:"type"` // openai | anthropic | mock
	BaseURL string        `yaml:"base_url"`
	APIKey  string        `yaml:"api_key"`
	Timeout time.Duration `yaml:"timeout"`
	// Mock-only knobs, handy for demos and tests.
	Latency     time.Duration `yaml:"latency"`
	FailureRate float64       `yaml:"failure_rate"`
}

// Model maps a client-facing model name to an ordered list of upstream targets.
// The first healthy target wins; the rest are fallbacks.
type Model struct {
	Name    string   `yaml:"name"`
	Targets []Target `yaml:"targets"`
}

// Target is a concrete (provider, upstream model) pair.
type Target struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
}

// Auth configures client API keys.
type Auth struct {
	// Disabled turns off authentication entirely. Only for local development.
	Disabled bool  `yaml:"disabled"`
	Keys     []Key `yaml:"keys"`
}

// Key is a client API key, stored only as its SHA-256 hash.
type Key struct {
	Name              string `yaml:"name"`
	SHA256            string `yaml:"sha256"`
	RequestsPerMinute int    `yaml:"requests_per_minute"`
	TokensPerDay      int    `yaml:"tokens_per_day"`
}

// Cache configures the response cache for non-streaming requests.
type Cache struct {
	Enabled    bool          `yaml:"enabled"`
	TTL        time.Duration `yaml:"ttl"`
	MaxEntries int           `yaml:"max_entries"`
}

// Retry configures retries against a single target before falling back.
type Retry struct {
	Attempts int           `yaml:"attempts"`
	Backoff  time.Duration `yaml:"backoff"`
}

// Breaker configures the per-provider circuit breaker.
type Breaker struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	Cooldown         time.Duration `yaml:"cooldown"`
}

// Log configures structured logging.
type Log struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | text
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces ${VAR} with the environment value so secrets stay out of the file.
func expandEnv(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		return os.Getenv(envRef.FindStringSubmatch(m)[1])
	})
}

// Load reads, expands, defaults and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse([]byte(expandEnv(string(raw))))
}

// Parse decodes YAML (already env-expanded), applies defaults and validates.
func Parse(data []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	for name, p := range c.Providers {
		if p.Timeout == 0 {
			p.Timeout = 60 * time.Second
		}
		c.Providers[name] = p
	}
	if c.Cache.TTL == 0 {
		c.Cache.TTL = 10 * time.Minute
	}
	if c.Cache.MaxEntries == 0 {
		c.Cache.MaxEntries = 1000
	}
	if c.Retry.Attempts == 0 {
		c.Retry.Attempts = 2
	}
	if c.Retry.Backoff == 0 {
		c.Retry.Backoff = 200 * time.Millisecond
	}
	if c.Breaker.FailureThreshold == 0 {
		c.Breaker.FailureThreshold = 5
	}
	if c.Breaker.Cooldown == 0 {
		c.Breaker.Cooldown = 30 * time.Second
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
}

var hexSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Validate reports every problem at once so a broken config is fixed in one pass.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if len(c.Providers) == 0 {
		add("at least one provider is required")
	}
	for name, p := range c.Providers {
		switch p.Type {
		case "openai", "anthropic":
			if p.APIKey == "" {
				add("provider %q: api_key is empty (is the environment variable set?)", name)
			}
		case "mock":
			if p.FailureRate < 0 || p.FailureRate > 1 {
				add("provider %q: failure_rate must be between 0 and 1", name)
			}
		default:
			add("provider %q: unknown type %q (want openai, anthropic or mock)", name, p.Type)
		}
	}

	if len(c.Models) == 0 {
		add("at least one model is required")
	}
	seen := map[string]bool{}
	for i, m := range c.Models {
		if m.Name == "" {
			add("models[%d]: name is required", i)
			continue
		}
		if seen[m.Name] {
			add("model %q is defined twice", m.Name)
		}
		seen[m.Name] = true
		if len(m.Targets) == 0 {
			add("model %q: at least one target is required", m.Name)
		}
		for j, t := range m.Targets {
			if _, ok := c.Providers[t.Provider]; !ok {
				add("model %q target %d: unknown provider %q", m.Name, j, t.Provider)
			}
			if t.Model == "" {
				add("model %q target %d: model is required", m.Name, j)
			}
		}
	}

	if !c.Auth.Disabled && len(c.Auth.Keys) == 0 {
		add("auth: add at least one key, or set auth.disabled: true for local development")
	}
	keyNames, keyHashes := map[string]bool{}, map[string]bool{}
	for i, k := range c.Auth.Keys {
		if k.Name == "" {
			add("auth.keys[%d]: name is required", i)
		}
		if keyNames[k.Name] {
			add("auth key name %q is used twice", k.Name)
		}
		keyNames[k.Name] = true
		h := strings.ToLower(k.SHA256)
		if !hexSHA256.MatchString(h) {
			add("auth key %q: sha256 must be 64 hex characters (run `switchyard hash-key`)", k.Name)
		}
		if keyHashes[h] {
			add("auth key %q: the same key hash is configured twice", k.Name)
		}
		keyHashes[h] = true
		if k.RequestsPerMinute < 0 || k.TokensPerDay < 0 {
			add("auth key %q: limits cannot be negative", k.Name)
		}
	}

	if c.Retry.Attempts < 1 || c.Retry.Attempts > 10 {
		add("retry.attempts must be between 1 and 10")
	}
	if c.Breaker.FailureThreshold < 1 {
		add("breaker.failure_threshold must be at least 1")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		add("log.level must be debug, info, warn or error")
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		add("log.format must be json or text")
	}
	return errors.Join(errs...)
}
