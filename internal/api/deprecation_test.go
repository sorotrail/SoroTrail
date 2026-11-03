package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenAPIDeprecationConsistency verifies bidirectional consistency between
// api/openapi.yaml, the embedded internal/api/openapi.json, and the router's
// deprecation middleware:
//  1. Every operation marked `deprecated: true` in the OpenAPI spec is
//     registered in the router, present in DefaultDeprecationPolicy(), and
//     emits valid Deprecation / Sunset headers when requested.
//  2. No route lacking `deprecated: true` in the OpenAPI spec is present in
//     DefaultDeprecationPolicy() or emits Deprecation / Sunset headers.
func TestOpenAPIDeprecationConsistency(t *testing.T) {
	yamlBytes, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	require.NoError(t, err, "reading api/openapi.yaml")

	yamlPolicy, err := ParseOpenAPIDeprecations(yamlBytes)
	require.NoError(t, err, "parsing deprecations from api/openapi.yaml")

	jsonPolicy, err := DefaultDeprecationPolicy()
	require.NoError(t, err, "parsing deprecations from embedded openapi.json")

	require.Equal(t, yamlPolicy.Entries(), jsonPolicy.Entries(),
		"api/openapi.yaml and embedded openapi.json must produce identical deprecation policies")

	s := New(nil, nil, testLogger(), "")
	routes := collectRoutes(s.router())
	registered := make(map[string]bool, len(routes))
	for _, rt := range routes {
		registered[strings.ToUpper(rt.Method)+" "+rt.Path] = true
	}

	// 1. Every deprecated operation in the spec must exist on the router and
	// have valid RFC 9745 / RFC 8594 header values.
	for _, dep := range jsonPolicy.Entries() {
		key := dep.Method + " " + dep.Path
		assert.True(t, registered[key],
			"deprecated operation %s in OpenAPI spec is not registered on the router", key)
		assert.NotEmpty(t, dep.DeprecationHeader, "deprecated operation %s must have a Deprecation header", key)
		if dep.DeprecationHeader != "true" {
			assert.True(t, strings.HasPrefix(dep.DeprecationHeader, "@"),
				"Deprecation header %q on %s must be 'true' or RFC 9745 '@<unix-timestamp>'", dep.DeprecationHeader, key)
		}
		if dep.SunsetHeader != "" {
			parsed, parseErr := http.ParseTime(dep.SunsetHeader)
			require.NoError(t, parseErr, "Sunset header %q on %s must be a valid HTTP-date", dep.SunsetHeader, key)
			assert.True(t, strings.HasSuffix(dep.SunsetHeader, " GMT"),
				"Sunset header %q on %s must be formatted in GMT", dep.SunsetHeader, key)
			assert.False(t, parsed.IsZero())
		}
	}

	// 2. Every non-deprecated route in the spec must NOT match the deprecation
	// policy, and live requests to representative non-deprecated endpoints must
	// not emit Deprecation or Sunset headers.
	for _, rt := range routes {
		if rt.Path == "/docs" || rt.Path == "/openapi.json" {
			continue
		}
		if _, isDep := jsonPolicy.Lookup(rt.Method, rt.Path); !isDep {
			samplePath := strings.NewReplacer(
				"{id}", "test-id",
				"{address}", "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
				"{contract_id}", "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				"{key_id}", "1",
			).Replace(rt.Path)
			_, matched := jsonPolicy.Match(rt.Method, samplePath)
			assert.False(t, matched,
				"non-deprecated route %s %s (sample %s) unexpectedly matched deprecation policy",
				rt.Method, rt.Path, samplePath)
		}
	}

	handler := s.Router()
	for _, path := range []string{"/livez", "/version", "/openapi.json"} {
		if _, isDep := jsonPolicy.Lookup(http.MethodGet, path); isDep {
			continue
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Empty(t, rec.Header().Get("Deprecation"),
			"non-deprecated %s must not emit Deprecation header", path)
		assert.Empty(t, rec.Header().Get("Sunset"),
			"non-deprecated %s must not emit Sunset header", path)
	}
}

func TestDeprecationMiddleware_WorkedExampleAndHeaders(t *testing.T) {
	const workedSpecYAML = `openapi: 3.1.0
info:
  title: SoroTrail API
  version: 0.2.0
paths:
  /events:
    get:
      operationId: listEvents
      summary: List indexed events
      deprecated: true
      x-deprecation-date: "2026-06-01T00:00:00Z"
      x-sunset-date: "2026-09-01T00:00:00Z"
      x-deprecation-replacement: "/api/v1/events/aggregate"
      x-deprecation-notice: "Use /api/v1/events/aggregate instead."
      responses:
        "200":
          description: OK
  /events/{id}:
    get:
      operationId: getEventById
      summary: Fetch event by ID
      deprecated: true
      x-deprecation-date: "2026-06-01"
      x-sunset-date: "2026-09-01"
      x-deprecation-replacement: "GET /contracts/{id}/events"
      responses:
        "200":
          description: OK
  /version:
    get:
      operationId: getVersion
      summary: Build version
      deprecated: true
      responses:
        "200":
          description: OK
  /livez:
    get:
      operationId: getLivez
      summary: Liveness probe
      responses:
        "200":
          description: OK
`
	policy, err := ParseOpenAPIDeprecations([]byte(workedSpecYAML))
	require.NoError(t, err)

	s := New(&stubStore{}, nil, testLogger(), "")
	s.SetDeprecationPolicy(policy)
	handler := s.Router()

	// 1. Deprecated endpoint with full RFC 3339 deprecation + sunset + replacement.
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "@1780272000", rec.Header().Get("Deprecation"))
	assert.Equal(t, "Tue, 01 Sep 2026 00:00:00 GMT", rec.Header().Get("Sunset"))
	sunsetTime, err := http.ParseTime(rec.Header().Get("Sunset"))
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), sunsetTime.UTC())
	assert.Equal(t, `</api/v1/events/aggregate>; rel="successor-version"`, rec.Header().Get("Link"))
	assert.Equal(t, uint64(1), s.DeprecatedRequestCount(http.MethodGet, "/events"))

	// 2. Parameterized deprecated endpoint on both unprefixed and /api/v1 paths.
	for _, reqPath := range []string{"/events/evt-alpha-1", "/api/v1/events/evt-beta-2"} {
		req = httptest.NewRequest(http.MethodGet, reqPath, nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, "@1780272000", rec.Header().Get("Deprecation"), "path %s", reqPath)
		assert.Equal(t, "Tue, 01 Sep 2026 00:00:00 GMT", rec.Header().Get("Sunset"), "path %s", reqPath)
		assert.Equal(t, `</contracts/{id}/events>; rel="successor-version"`, rec.Header().Get("Link"), "path %s", reqPath)
	}
	assert.Equal(t, uint64(2), s.DeprecatedRequestCount(http.MethodGet, "/events/{id}"))

	// 3. Bare deprecated endpoint without dates emits Deprecation: true and no Sunset.
	req = httptest.NewRequest(http.MethodGet, "/version", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("Deprecation"))
	assert.Empty(t, rec.Header().Get("Sunset"))
	assert.Empty(t, rec.Header().Get("Link"))
	assert.Equal(t, uint64(1), s.DeprecatedRequestCount(http.MethodGet, "/version"))

	// 4. Non-deprecated endpoint emits no deprecation headers.
	req = httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("Deprecation"))
	assert.Empty(t, rec.Header().Get("Sunset"))
	assert.Empty(t, rec.Header().Get("Link"))
	assert.Equal(t, uint64(0), s.DeprecatedRequestCount(http.MethodGet, "/livez"))

	// 5. Scrape /metrics and confirm canonical route labels are exported without
	// leaking raw path parameter values.
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	handler.ServeHTTP(metricsRec, metricsReq)
	require.Equal(t, http.StatusOK, metricsRec.Code)
	body := metricsRec.Body.String()
	assert.Contains(t, body, `sorotrail_http_deprecated_requests_total{method="GET",route="/events"}`)
	assert.Contains(t, body, `sorotrail_http_deprecated_requests_total{method="GET",route="/events/{id}"}`)
	assert.NotContains(t, body, "evt-alpha-1", "metric labels must not contain raw path parameter values")
	assert.NotContains(t, body, "evt-beta-2", "metric labels must not contain raw path parameter values")
}

func TestParseOpenAPIDeprecations_ValidationErrors(t *testing.T) {
	t.Run("sunset without deprecated flag", func(t *testing.T) {
		const spec = `openapi: 3.1.0
info:
  title: SoroTrail API
  version: 0.2.0
paths:
  /events:
    get:
      x-sunset-date: "2026-09-01T00:00:00Z"
`
		_, err := ParseOpenAPIDeprecations([]byte(spec))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "without deprecated: true")
	})

	t.Run("invalid sunset date format", func(t *testing.T) {
		const spec = `openapi: 3.1.0
info:
  title: SoroTrail API
  version: 0.2.0
paths:
  /events:
    get:
      deprecated: true
      x-sunset-date: "not-a-date"
`
		_, err := ParseOpenAPIDeprecations([]byte(spec))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid x-sunset-date")
	})

	t.Run("invalid deprecation date format", func(t *testing.T) {
		const spec = `openapi: 3.1.0
info:
  title: SoroTrail API
  version: 0.2.0
paths:
  /events:
    get:
      deprecated: true
      x-deprecation-date: "@not-unix"
`
		_, err := ParseOpenAPIDeprecations([]byte(spec))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid x-deprecation-date")
	})
}
