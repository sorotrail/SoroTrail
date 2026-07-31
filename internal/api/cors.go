package api

import (
	"net/http"
)

// corsHeaders is the default set advertised on preflight responses when the
// browser does not tell us which headers it wants via Access-Control-Request-Headers.
const corsHeaders = "Content-Type, X-API-Key, X-Request-ID"

// corsMiddleware returns middleware that emits CORS headers for browser
// requests whose Origin matches the configured allow-list.
//
// When no origins are configured (the default) it is a no-op — no CORS
// headers are ever written — so deployments that don't set
// CORS_ALLOWED_ORIGINS keep today's header-for-header behavior.
//
// Matching:
//   - The single wildcard "*" allows any origin and is echoed verbatim.
//   - Any other entry must be an explicit origin (scheme + host, e.g.
//     https://app.example.com); a matching request's Origin is echoed back
//     and Vary: Origin is added so shared caches key on it.
//   - Requests from origins not in the list pass through untouched, so the
//     API answers them exactly as it would without CORS enabled.
//   - Preflight OPTIONS requests (those carrying Access-Control-Request-Method)
//     from allowed origins short-circuit with 204 and the allow lists.
func corsMiddleware(allowed []string) func(http.Handler) http.Handler {
	allowAll := false
	for _, o := range allowed {
		if o == "*" {
			allowAll = true
			break
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" || len(allowed) == 0 {
				next.ServeHTTP(w, r)
				return
			}

			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				if !allowAll && !originAllowed(origin, allowed) {
					next.ServeHTTP(w, r)
					return
				}
				if allowAll {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Add("Vary", "Origin")
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
					w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
				} else {
					w.Header().Set("Access-Control-Allow-Headers", corsHeaders)
				}
				w.Header().Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}

			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if originAllowed(origin, allowed) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
			next.ServeHTTP(w, r)
		})
	}
}

func originAllowed(origin string, allowed []string) bool {
	for _, o := range allowed {
		if o == origin {
			return true
		}
	}
	return false
}
