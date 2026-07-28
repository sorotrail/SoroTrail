// Package api — panic-recovery middleware with a recovered-panic counter
// and structured log entries.
package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sync/atomic"
)

// Recoverer is HTTP middleware that catches panics from downstream
// handlers, increments a counter, logs the panic (route path, method,
// panic value, stack trace) and returns 500.
type Recoverer struct {
	count atomic.Uint64
	log   *slog.Logger
}

// NewRecoverer builds a Recoverer that writes structured log entries
// through log. Panics are logged at Error level.
func NewRecoverer(log *slog.Logger) *Recoverer {
	return &Recoverer{log: log}
}

// PanicsRecovered returns the total number of panics this middleware has
// recovered since creation. Safe for concurrent access.
func (r *Recoverer) PanicsRecovered() uint64 {
	return r.count.Load()
}

// Middleware wraps next with panic recovery.
func (r *Recoverer) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if rvr := recover(); rvr != nil {
				r.count.Add(1)
				r.log.Error("http panic recovered",
					"path", req.URL.Path,
					"method", req.Method,
					"panic", fmt.Sprintf("%v", rvr),
					"stack", string(debug.Stack()),
				)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, req)
	})
}
