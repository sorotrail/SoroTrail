package api

import (
	"crypto/subtle"
	"errors"
	"net/http"
)

var (
	errAuthNotConfigured = errors.New("API_KEY env var must be set to access watched-contracts endpoints")
	errAuthFailed        = errors.New("invalid or missing X-API-Key header")
)

// apiKeyAuth gates a single route on an X-API-Key header that must match
// the configured API key, byte-for-byte, via constant-time comparison.
//
// Fail-closed: when no key is configured the middleware rejects every
// request with 503 and a message naming the missing env var, so writes
// are never open even if AUTH_ENABLED is false elsewhere in the binary.
//
// This is a stopgap until #17 lands; a real implementation will replace
// this file with whatever key/HMAC scheme the rest of the API uses.
func apiKeyAuth(expected string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if expected == "" {
				writeError(w, http.StatusServiceUnavailable,
					errAuthNotConfigured)
				return
			}
			provided := r.Header.Get("X-API-Key")
			if provided == "" ||
				subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
				writeError(w, http.StatusUnauthorized, errAuthFailed)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
