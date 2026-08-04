package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger returns a no-op logger.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// openapiSchema represents the parsed OpenAPI 3.1 document.
type openapiSchema struct {
	OpenAPI string                    `json:"openapi"`
	Info    struct{ Title string }    `json:"info"`
	Paths   map[string]map[string]any `json:"paths"`
}

// routeEntry holds a method + path pattern for comparison.
type routeEntry struct {
	Method string
	Path   string
}

// collectRoutes walks the chi router and extracts every registered route.
func collectRoutes(r chi.Router) []routeEntry {
	var routes []routeEntry
	chi.Walk(r, func(method, path string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, routeEntry{Method: method, Path: path})
		return nil
	})
	return routes
}

// TestOpenAPISpecIsValidJSON verifies the embedded spec is parseable JSON
// and declares openapi 3.1.x.
func TestOpenAPISpecIsValidJSON(t *testing.T) {
	var doc openapiSchema
	err := json.Unmarshal(openapiSpec, &doc)
	require.NoError(t, err, "openapi.json must be valid JSON")
	assert.True(t, strings.HasPrefix(doc.OpenAPI, "3.1"), "spec must be OpenAPI 3.1.x, got %q", doc.OpenAPI)
	assert.NotEmpty(t, doc.Paths, "spec must declare at least one path")
	assert.Equal(t, "SoroTrail API", doc.Info.Title)
}

// TestOpenAPISpecCoversAllRoutes walks the real chi router and ensures
// every registered route has a corresponding entry in the OpenAPI spec.
// Routes that serve the spec itself (/openapi.json, /docs) are excluded
// since they aren't documented in the spec.
func TestOpenAPISpecCoversAllRoutes(t *testing.T) {
	var doc openapiSchema
	require.NoError(t, json.Unmarshal(openapiSpec, &doc))

	s := &Server{log: testLogger()}
	routes := collectRoutes(s.router())

	for _, route := range routes {
		method := strings.ToLower(route.Method)
		path := route.Path

		// These routes serve the spec/Swagger UI — they aren't
		// documented within the spec itself.
		if path == "/docs" || path == "/openapi.json" {
			continue
		}

		pathItem, ok := doc.Paths[path]
		require.True(t, ok, "route %s %s is missing from openapi.json paths", route.Method, path)
		_, ok = pathItem[method]
		assert.True(t, ok, "route %s %s has no %q operation in openapi.json", route.Method, path, method)
	}
}

// TestOpenAPISpecHasNoExtraRoutes ensures every path+method in the spec
// has a registered route in the chi router (no stale documentation).
func TestOpenAPISpecHasNoExtraRoutes(t *testing.T) {
	var doc openapiSchema
	require.NoError(t, json.Unmarshal(openapiSpec, &doc))

	s := &Server{log: testLogger()}
	routes := collectRoutes(s.router())

	registered := make(map[string]bool)
	for _, route := range routes {
		registered[strings.ToLower(route.Method)+" "+route.Path] = true
	}

	for path, pathItem := range doc.Paths {
		for method := range pathItem {
			key := strings.ToLower(method) + " " + path
			assert.True(t, registered[key],
				"openapi.json has %s %s but no matching route is registered", method, path)
		}
	}
}

// TestOpenAPISpecIsServed verifies the /openapi.json endpoint returns
// the spec with the correct content type.
func TestOpenAPISpecIsServed(t *testing.T) {
	s := &Server{log: testLogger()}
	handler := s.Router()

	req := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var doc openapiSchema
	err := json.Unmarshal(rec.Body.Bytes(), &doc)
	require.NoError(t, err, "response body must be valid JSON")
	assert.True(t, strings.HasPrefix(doc.OpenAPI, "3.1"))
}

// TestDocsIsServed verifies the /docs endpoint returns an HTML page
// that loads Swagger UI pointing at /openapi.json.
func TestDocsIsServed(t *testing.T) {
	s := &Server{log: testLogger()}
	handler := s.Router()

	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.Contains(rec.Header().Get("Content-Type"), "text/html"),
		"Content-Type must be text/html, got %q", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "swagger-ui")
	assert.Contains(t, rec.Body.String(), "/openapi.json")
}
