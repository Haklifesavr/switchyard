// Command switchyard runs the LLM gateway.
//
//	switchyard [-config config.yaml]   start the gateway
//	switchyard gen-key                 create a client API key and print its hash for the config
//	switchyard hash-key <key>          print the SHA-256 hash of an existing key
//	switchyard version                 print the version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Haklifesavr/switchyard/internal/auth"
	"github.com/Haklifesavr/switchyard/internal/cache"
	"github.com/Haklifesavr/switchyard/internal/config"
	"github.com/Haklifesavr/switchyard/internal/limit"
	"github.com/Haklifesavr/switchyard/internal/metrics"
	"github.com/Haklifesavr/switchyard/internal/provider"
	"github.com/Haklifesavr/switchyard/internal/provider/anthropic"
	"github.com/Haklifesavr/switchyard/internal/provider/mock"
	"github.com/Haklifesavr/switchyard/internal/provider/openai"
	"github.com/Haklifesavr/switchyard/internal/router"
	"github.com/Haklifesavr/switchyard/internal/server"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "gen-key":
			key, err := auth.Generate()
			if err != nil {
				fatal(err)
			}
			fmt.Printf("key:    %s\nsha256: %s\n\nGive the key to the client; put only the sha256 in your config.\n", key, auth.Hash(key))
			return
		case "hash-key":
			if len(os.Args) != 3 {
				fatal(errors.New("usage: switchyard hash-key <key>"))
			}
			fmt.Println(auth.Hash(os.Args[2]))
			return
		case "version":
			fmt.Println(version)
			return
		}
	}

	path := flag.String("config", "config.yaml", "path to the config file")
	flag.Parse()
	if err := run(*path); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "switchyard:", err)
	os.Exit(1)
}

func newLogger(c config.Log) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(c.Level))
	opts := &slog.HandlerOptions{Level: level}
	if c.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func buildProviders(c *config.Config) map[string]provider.Provider {
	out := make(map[string]provider.Provider, len(c.Providers))
	for name, p := range c.Providers {
		switch p.Type {
		case "openai":
			out[name] = openai.New(name, p.BaseURL, p.APIKey, p.Timeout)
		case "anthropic":
			out[name] = anthropic.New(name, p.BaseURL, p.APIKey, p.Timeout)
		case "mock":
			out[name] = mock.New(name, p.Latency, p.FailureRate)
		}
	}
	return out
}

func run(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Log)
	if cfg.Auth.Disabled {
		log.Warn("authentication is disabled: anyone who can reach this port can use your upstream credits")
	}

	routes := make(map[string][]router.Target, len(cfg.Models))
	for _, m := range cfg.Models {
		for _, t := range m.Targets {
			routes[m.Name] = append(routes[m.Name], router.Target{Provider: t.Provider, Model: t.Model})
		}
	}

	m := metrics.New()
	var srv *server.Server
	rt, err := router.New(buildProviders(cfg), routes, router.Options{
		Attempts: cfg.Retry.Attempts, Backoff: cfg.Retry.Backoff,
		Threshold: cfg.Breaker.FailureThreshold, Cooldown: cfg.Breaker.Cooldown,
		OnAttempt: func(p, model string, err error, d time.Duration) { srv.RecordAttempt(p, model, err, d) },
	})
	if err != nil {
		return err
	}
	m.RegisterBreakerGauge(func() map[string]int {
		out := map[string]int{}
		for name, st := range rt.BreakerStates() {
			out[name] = map[router.State]int{router.Closed: 0, router.HalfOpen: 1, router.Open: 2}[st]
		}
		return out
	})

	var c *cache.Cache
	if cfg.Cache.Enabled {
		c = cache.New(cfg.Cache.MaxEntries, cfg.Cache.TTL, nil)
	}
	srv = server.New(server.Options{
		Router: rt, Auth: auth.New(cfg.Auth), Rate: limit.NewRateLimiter(nil), Budget: limit.NewBudget(nil),
		Cache: c, Metrics: m, Logger: log,
	})

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// No WriteTimeout: streamed responses can legitimately run for minutes.
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() {
		log.Info("switchyard listening", "addr", cfg.Listen, "version", version, "models", rt.Models())
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down; draining in-flight requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
	}
	log.Info("stopped")
	return nil
}
