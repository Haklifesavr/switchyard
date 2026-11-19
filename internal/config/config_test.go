package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validYAML = `
providers:
  mock:
    type: mock
models:
  - name: demo
    targets:
      - provider: mock
        model: mock-1
auth:
  keys:
    - name: dev
      sha256: 2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b
`

func TestParseAppliesDefaults(t *testing.T) {
	c, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c.Listen != ":8080" {
		t.Errorf("Listen = %q, want :8080", c.Listen)
	}
	if c.Providers["mock"].Timeout != 60*time.Second {
		t.Errorf("provider timeout default = %v", c.Providers["mock"].Timeout)
	}
	if c.Retry.Attempts != 2 || c.Breaker.FailureThreshold != 5 || c.Cache.MaxEntries != 1000 {
		t.Errorf("unexpected defaults: %+v %+v %+v", c.Retry, c.Breaker, c.Cache)
	}
}

func TestLoadExpandsEnv(t *testing.T) {
	t.Setenv("SY_TEST_KEY", "sk-from-env")
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	cfg := strings.Replace(validYAML, "providers:\n", "providers:\n  oa:\n    type: openai\n    api_key: ${SY_TEST_KEY}\n", 1)
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Providers["oa"].APIKey; got != "sk-from-env" {
		t.Errorf("api_key = %q, want value from env", got)
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	bad := `
providers:
  oa:
    type: openai
  weird:
    type: nope
models:
  - name: m
    targets:
      - provider: missing
        model: ""
auth:
  keys:
    - name: k
      sha256: not-a-hash
`
	_, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("expected validation error")
	}
	for _, want := range []string{
		`provider "oa": api_key is empty`,
		`unknown type "nope"`,
		`unknown provider "missing"`,
		`model is required`,
		`sha256 must be 64 hex characters`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestRequiresKeysUnlessDisabled(t *testing.T) {
	noKeys := strings.Split(validYAML, "auth:")[0]
	if _, err := Parse([]byte(noKeys)); err == nil || !strings.Contains(err.Error(), "auth:") {
		t.Fatalf("expected auth error, got %v", err)
	}
	if _, err := Parse([]byte(noKeys + "auth:\n  disabled: true\n")); err != nil {
		t.Fatalf("auth.disabled should be accepted: %v", err)
	}
}

func TestRejectsUnknownFields(t *testing.T) {
	if _, err := Parse([]byte(validYAML + "retyr:\n  attempts: 3\n")); err == nil {
		t.Fatal("expected error for misspelled field")
	}
}
