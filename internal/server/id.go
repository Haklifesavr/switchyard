package server

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a random 16-hex-character request ID.
func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
