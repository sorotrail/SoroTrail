// Package api — CORS middleware.
//
// Browser cross-origin requests depend on a handful of Access-Control-*
// response headers. This middleware applies an explicit allowlist
// policy: by default no origin is allowed (every cross-origin request
// is rejected by the browser), and operators opt-in by setting
// CORS_ALLOWED_ORIGINS to a comma-separated list of origins.
//
// Preflight (OPTIONS) requests are answered here without invoking the
// underlying handler: the browser's preflight is a metadata-only
// negotiation, and letting it through to the route handler would
// produce a confusing 404 or 405 from a handler that doesn't expect
// OPTIONS. We respond 204 No Content with the agreed methods/headers.
//
// Vary header: the response Varies on Origin whenever the allowlist
// distinguishes between origins. With "*" the response is identical
// for any origin, so no Vary is needed (and adding one would be
// wasting cache space). The middleware appends to an existing Vary
// header rather than overwriting it so it coexists with the
// Accept-Encoding Vary already set by compress.go / writeCacheHeaders.
package api

import (
	"net/http"
	"strings"
)

// CORSConfig captures the middleware's inputs. All fields are
// optional; the zero value is the deny-all default.
type CORSConfig struct {
	// AllowedOrigins is the explicit allowlist. "*" is a special value
	// that matches any origin (no credentials). Empty allowlist means
	// deny-all (no CORS headers emitted at all).
	AllowedOrigins []string
	// AllowedMethods is returned on the preflight OPTIONS response.
	// Defaults to "GET, POST, PUT, DELETE, OPTIONS" via config.
	AllowedMethods []string
	// AllowedHeaders is returned on the preflight OPTIONS response.
	// Defaults to "Content-Type, X-API-Key, Accept" via config.
	AllowedHeaders []string
}

// CORS returns middleware that applies the policy in cfg.
//
// Behavior:
//
//   - If cfg.AllowedOrigins is empty, the middleware is a no-op (every
//     browser call is rejected by the browser itself; no point in
//     adding response overhead).
//   - For any other request with an Origin header:
//   - If Origin matches an allowed origin (exact match), the response
//     gets Access-Control-Allow-Origin=<origin>, Vary: Origin, and
//     (for OPTIONS) Access-Control-Allow-Methods / -Headers.
//   - If Origin does NOT match any allowed origin, the middleware
//     passes the request through with no CORS headers. The browser
//     rejects the response because the required
//     Access-Control-Allow-Origin header is missing.
//   - For OPTIONS requests with a matching origin, the middleware
//     short-circuits with 204 No Content so the underlying handler
//     never sees a preflight.
func CORS(cfg CORSConfig) func(http.Handler) http.Handler {
	allowAll := false
	allowed := make(map[string]bool, len(cfg.AllowedOrigins))
	for _, o := range cfg.AllowedOrigins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		if o == "*" {
			allowAll = true
			continue
		}
		allowed[o] = true
	}
	methods := strings.Join(cfg.AllowedMethods, ", ")
	headers := strings.Join(cfg.AllowedHeaders, ", ")

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Deny-all (empty allowlist) is a strict no-op. Useful as
			// the documented "off" state: no behavior change vs. an
			// unconfigured server.
			if len(allowed) == 0 && !allowAll {
				next.ServeHTTP(w, r)
				return
			}

			origin := r.Header.Get("Origin")
			if origin == "" {
				// Same-origin requests don't carry an Origin header
				// and aren't subject to CORS. Pass through unchanged
				// so a server-to-server caller isn't accidentally
				// logged as "denied".
				next.ServeHTTP(w, r)
				return
			}

			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				// "*" responses are identical regardless of origin, so
				// do NOT add Vary: Origin — that would *break* caching
				// for shared caches that key on Vary.
				if r.Method == http.MethodOptions {
					writePreflightHeaders(w, methods, headers)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if !allowed[origin] {
				// Explicitly disallowed. Pass through without CORS
				// headers; the browser will block the response on the
				// client side. We do NOT 403 here because that turns
				// a same-origin-or-non-browser caller (curl, server
				// SDK) into an error case — better to keep the contract
				// "CORS is a browser-side concern".
				next.ServeHTTP(w, r)
				return
			}

			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			// Vary must be a single comma-joined header on the wire;
			// Header().Add appends to a slice, but Get returns only
			// the first value, so callers reading via Get would miss
			// "Origin". Read the existing Vary first, fold in
			// "Origin", and Set the merged string back.
			appendVary(h, "Origin")
			if r.Method == http.MethodOptions {
				writePreflightHeaders(w, methods, headers)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writePreflightHeaders writes the preflight-specific headers. The
// middleware has already set Origin and Vary; this only adds the per
// Allow-* headers and a content-length of 0 so a strict client can
// confirm there's no body.
func writePreflightHeaders(w http.ResponseWriter, methods, headers string) {
	if methods != "" {
		w.Header().Set("Access-Control-Allow-Methods", methods)
	}
	if headers != "" {
		w.Header().Set("Access-Control-Allow-Headers", headers)
	}
	w.Header().Set("Access-Control-Max-Age", "600")
}

// appendVary folds token into the response's Vary header, leaving any
// existing comma-separated Vary list intact. Header().Add appends to
// the underlying []string separately, which produces two Vary lines
// on the wire but only the first is visible to callers that read
// Header().Get; we want a single merged string so a downstream
// compressor or ETag-aware cache sees both variants.
func appendVary(h http.Header, token string) {
	cur := h.Get("Vary")
	for _, part := range strings.Split(cur, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			// Already present; do not double-add.
			return
		}
	}
	if cur == "" {
		h.Set("Vary", token)
		return
	}
	h.Set("Vary", cur+", "+token)
}
