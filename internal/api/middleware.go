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

// corsOriginsSet is a set of allowed origins for CORS, with a boolean
// wildcards entry indicating whether "*" is permitted.
type corsOriginsSet struct {
	origins    map[string]bool
	wildcard   bool
}

// newCORSOriginsSet builds a corsOriginsSet from a slice of allowed
// origin strings. "*" is treated as a wildcard that matches any
// Origin header value.
func newCORSOriginsSet(origins []string) *corsOriginsSet {
	s := &corsOriginsSet{origins: make(map[string]bool)}
	for _, o := range origins {
		if o == "*" {
			s.wildcard = true
		} else {
			s.origins[o] = true
		}
	}
	return s
}

// allowedOrigin returns the value to set for Access-Control-Allow-Origin
// when the request's Origin matches the allowed set. If there is no
// match it returns the empty string.
func (s *corsOriginsSet) allowedOrigin(reqOrigin string) string {
	if s.origins[reqOrigin] {
		return reqOrigin
	}
	if s.wildcard {
		return "*"
	}
	return ""
}

// CORS middleware applies Access-Control-* response headers for every
// request whose Origin matches the configured CORS_ALLOWED_ORIGINS
// list (or "*"). Preflight OPTIONS requests are answered with 204
// No Content so that browsers never block them on a slow or faulty
// upstream handler.
func corsMiddleware(origins []string) func(http.Handler) http.Handler {
	set := newCORSOriginsSet(origins)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqOrigin := r.Header.Get("Origin")
			if reqOrigin == "" {
				next.ServeHTTP(w, r)
				return
			}
			allowOrigin := set.allowedOrigin(reqOrigin)
			if allowOrigin == "" {
				next.ServeHTTP(w, r)
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
			w.Header().Set("Access-Control-Max-Age", "86400")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

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
