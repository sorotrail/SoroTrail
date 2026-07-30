package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// okHandler is the trivial next-handler CORS tests wrap. Anything the
// middleware should NOT short-circuit ends up here.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	// Echo the request method back so tests can confirm CORS did not
	// swallow a non-OPTIONS request.
	w.Header().Set("Echo-Method", r.Method)
	w.WriteHeader(http.StatusTeapot)
})

// TestCORS_DenyAllWithEmptyAllowlist is the documented default. No
// CORS headers should be added for any request; cross-origin browser
// calls are rejected by the browser itself.
func TestCORS_DenyAllWithEmptyAllowlist(t *testing.T) {
	h := CORS(CORSConfig{})(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"),
		"deny-all means no CORS headers are emitted")
	assert.Empty(t, rr.Header().Get("Vary"))
}

// TestCORS_AllowedOriginReceivesHeaders is the happy path: an
// origin on the allowlist gets Access-Control-Allow-Origin and Vary:
// Origin. Allow-* methods/headers are NOT set on a simple GET.
func TestCORS_AllowedOriginReceivesHeaders(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Content-Type", "X-API-Key"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, rr.Header().Get("Vary"), "Origin")
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Methods"),
		"non-preflight responses don't advertise methods")
}

// TestCORS_DisallowedOriginPassesThroughWithoutHeaders is the
// "denied on the browser side" path. The middleware passes the
// request through with no CORS headers so a non-browser caller
// (curl, server SDK) still gets a normal response.
func TestCORS_DisallowedOriginPassesThroughWithoutHeaders(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, rr.Header().Get("Vary"))
}

// TestCORS_PreflightShortCircuits is the "OPTIONS" path: the
// middleware answers 204 with the negotiated methods/headers so the
// underlying handler never sees the preflight.
func TestCORS_PreflightShortCircuits(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Content-Type"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodOptions, "/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNoContent, rr.Code,
		"preflight short-circuits with 204 No Content")
	assert.Equal(t, "https://app.example.com", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, POST", rr.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "Content-Type", rr.Header().Get("Access-Control-Allow-Headers"))
	assert.Empty(t, rr.Header().Get("Echo-Method"),
		"preflight must not invoke the next handler")
}

// TestCORS_PreflightDisallowedOriginDoesNotAddHeaders confirms a
// preflight from a non-allowed origin still passes through with no
// CORS headers — the browser will reject before letting the real
// request through.
func TestCORS_PreflightDisallowedOriginDoesNotAddHeaders(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodOptions, "/x", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code,
		"preflight from disallowed origin falls through to the handler")
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"))
}

// TestCORS_Wildcard is the "*" case: any origin is allowed. The
// response does NOT add Vary: Origin because the allowed-origin
// response is identical regardless of which origin called (a shared
// cache serving two different origins under one entry is the correct
// behavior here).
func TestCORS_Wildcard(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Origin", "https://anything.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, rr.Header().Get("Vary"),
		"wildcard responses are identical for any origin, so no Vary needed")
}

// TestCORS_WildcardPreflight confirms the wildcard still produces a
// syntactically valid preflight response.
func TestCORS_WildcardPreflight(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Content-Type"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodOptions, "/x", nil)
	req.Header.Set("Origin", "https://anything.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNoContent, rr.Code)
	assert.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, POST", rr.Header().Get("Access-Control-Allow-Methods"))
}

// TestCORS_SameOriginRequestPassesThrough confirms a request with no
// Origin header is forwarded unchanged. Same-origin browser calls
// don't carry an Origin header.
func TestCORS_SameOriginRequestPassesThrough(t *testing.T) {
	h := CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
	})(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"),
		"no Origin header → no CORS machinery engaged")
}

// TestAppendVary exercises the Vary-merge helper directly: the
// middleware ordering means an inner handler that calls
// Header().Set("Vary", ...) AFTER CORS runs would clobber anything
// CORS wrote — that interaction is the caller's responsibility, not
// CORS's. What CORS does own is "I write the merged list myself,
// idempotently", and that is what this test covers.
func TestAppendVary(t *testing.T) {
	t.Run("empty header", func(t *testing.T) {
		h := http.Header{}
		appendVary(h, "Origin")
		assert.Equal(t, "Origin", h.Get("Vary"))
	})
	t.Run("preserves single existing token", func(t *testing.T) {
		h := http.Header{}
		h.Set("Vary", "Accept-Encoding")
		appendVary(h, "Origin")
		assert.Equal(t, "Accept-Encoding, Origin", h.Get("Vary"))
	})
	t.Run("preserves multi-token list", func(t *testing.T) {
		h := http.Header{}
		h.Set("Vary", "Accept-Encoding, User-Agent")
		appendVary(h, "Origin")
		assert.Equal(t, "Accept-Encoding, User-Agent, Origin", h.Get("Vary"))
	})
	t.Run("does not double-add", func(t *testing.T) {
		h := http.Header{}
		h.Set("Vary", "Accept-Encoding, Origin")
		appendVary(h, "Origin")
		assert.Equal(t, "Accept-Encoding, Origin", h.Get("Vary"),
			"Origin is folded case-insensitively, not duplicated")
	})
	t.Run("case-insensitive dup check", func(t *testing.T) {
		h := http.Header{}
		h.Set("Vary", "origin")
		appendVary(h, "ORIGIN")
		assert.Equal(t, "origin", h.Get("Vary"))
	})
}

// TestCORS_VarySetBeforeInnerHandler verifies the end-to-end path
// that mirrors production: an OUTER middleware (compress.go) sets
// Vary first, then CORS appends Origin, then the inner handler runs.
// In production this is the ordering of the wrapper-style middlewares;
// this test pins the behavior so a future refactor cannot silently
// reorder the merge.
func TestCORS_VarySetBeforeInnerHandler(t *testing.T) {
	outerSetVary := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Vary", "Accept-Encoding")
			next.ServeHTTP(w, r)
		})
	}
	h := outerSetVary(CORS(CORSConfig{
		AllowedOrigins: []string{"https://app.example.com"},
	})(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTeapot, rr.Code)
	vary := rr.Header().Get("Vary")
	assert.Contains(t, vary, "Accept-Encoding",
		"upstream Vary=Accept-Encoding is preserved")
	assert.Contains(t, vary, "Origin",
		"Origin is appended to the existing Vary")
}
