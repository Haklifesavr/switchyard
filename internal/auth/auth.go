// Package auth authenticates client API keys. Keys are stored only as SHA-256 hashes, so a
// leaked config file does not leak usable keys.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/Haklifesavr/switchyard/internal/config"
)

// ErrMissing and ErrInvalid distinguish "no key" from "wrong key" for clearer client errors.
var (
	ErrMissing = errors.New("missing API key: send it as `Authorization: Bearer <key>`")
	ErrInvalid = errors.New("invalid API key")
)

// Key is an authenticated client and its limits.
type Key struct {
	Name              string
	RequestsPerMinute int
	TokensPerDay      int
}

// Anonymous is used when authentication is disabled.
var Anonymous = &Key{Name: "anonymous"}

// Authenticator validates bearer keys.
type Authenticator struct {
	disabled bool
	byHash   map[string]*Key
}

// New builds an authenticator from config.
func New(c config.Auth) *Authenticator {
	a := &Authenticator{disabled: c.Disabled, byHash: map[string]*Key{}}
	for _, k := range c.Keys {
		a.byHash[strings.ToLower(k.SHA256)] = &Key{Name: k.Name, RequestsPerMinute: k.RequestsPerMinute, TokensPerDay: k.TokensPerDay}
	}
	return a
}

// Authenticate returns the key for a request.
func (a *Authenticator) Authenticate(r *http.Request) (*Key, error) {
	if a.disabled {
		return Anonymous, nil
	}
	h := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(h, "Bearer ")
	token = strings.TrimSpace(token)
	if !ok || token == "" {
		return nil, ErrMissing
	}
	// Looking up the SHA-256 digest leaks nothing useful through timing: an attacker would
	// need a preimage to make progress.
	if k, found := a.byHash[Hash(token)]; found {
		return k, nil
	}
	return nil, ErrInvalid
}

// Hash returns the hex SHA-256 of a key, as stored in config.
func Hash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Generate creates a new random key ("sy_" + 48 hex chars, 192 bits of entropy).
func Generate() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "sy_" + hex.EncodeToString(b), nil
}
