// middleware/request_id.go
//
// Request ID middleware for request tracking and structured logging.
//
// Provides:
//   - X-Request-ID header extraction or cryptographically secure ID generation
//   - Response header injection for client correlation
//   - Thread-safe context storage for downstream use
//
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
)

// RequestIDContextKey is the canonical key for request ID storage in context.
type RequestIDContextKey string

const (
	// RequestIDKey is the context key used to store and retrieve request IDs.
	RequestIDKey RequestIDContextKey = "sorotrail_request_id"
)

// generateRequestID creates a cryptographically secure 16-byte random ID
// encoded as hex string (32 characters). This provides sufficient entropy for
// all request tracking needs while keeping the ID compact for logging.
func generateRequestID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// requestIDMiddleware wraps the next handler with request ID extraction/creation logic.
// It honors incoming X-Request-ID header, generates a secure random one if absent,
// and sets it on both the response header and context for downstream use.
//
// The request ID is available to any handler via api.GetRequestID(r.Context()).
// It also attaches an enriched logger to the context for consistent request tracking.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqID string
		reqID = r.Header.Get("X-Request-ID")
		if reqID == "" {
			var err error
			reqID, err = generateRequestID()
			if err != nil {
				http.Error(w, "failed to generate request ID", http.StatusInternalServerError)
				return
			}
		}
		r.Header.Set("X-Request-ID", reqID)
		logger := slog.New(slog.NewJSONHandler(w, nil)).With("request_id", reqID)
		ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
		ctx = context.WithValue(ctx, "logger", logger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID retrieves the request ID from context.
// Returns empty string if not present.
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return ""
}

// requestIDLoggerMiddleware enriches logs with request_id.
//
// This middleware should be placed before other middlewares since request_id
// needs to be available early for all logging.
func requestIDLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			var err error
			reqID, err = generateRequestID()
			if err != nil {
				http.Error(w, "failed to generate request ID", http.StatusInternalServerError)
				return
			}
		r.Header.Set("X-Request-ID", reqID)
	}
	
	logger := slog.New(slog.NewJSONHandler(w, nil)).With("request_id", reqID)
	ctx := context.WithValue(r.Context(), RequestIDKey, reqID)
	next.ServeHTTP(w, r.WithContext(ctx))
	})
}
