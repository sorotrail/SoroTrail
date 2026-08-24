package docs_test

import (
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"

	"github.com/sorotrail/sorotrail/internal/api"
)

type openAPIDoc struct {
	Paths map[string]map[string]any `yaml:"paths"`
}

type routePair struct {
	Method string
	Path   string
}

func TestNoRouteDrift(t *testing.T) {
	doc := readSpec(t)
	specRoutes := specRoutePairs(doc)
	routerRoutes := routerRoutePairs(t)

	for _, r := range specRoutes {
		if !slices.ContainsFunc(routerRoutes, func(rr routePair) bool {
			return rr.Method == r.Method && rr.Path == r.Path
		}) {
			t.Fatalf("OpenAPI spec documents %s %s but no such route exists on the router", r.Method, r.Path)
		}
	}

	for _, r := range routerRoutes {
		// The routes that serve the spec and its Swagger UI are not part of
		// the documented API surface; internal/api/openapi_test.go applies
		// the same exclusion.
		if r.Path == "/openapi.json" || r.Path == "/docs" {
			continue
		}
		if !slices.ContainsFunc(specRoutes, func(sr routePair) bool {
			return sr.Method == r.Method && sr.Path == r.Path
		}) {
			t.Fatalf("Router implements %s %s but it is missing from api/openapi.yaml", r.Method, r.Path)
		}
	}
}

func readSpec(t *testing.T) openAPIDoc {
	t.Helper()
	data, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("reading openapi spec: %v", err)
	}
	var doc openAPIDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parsing openapi spec: %v", err)
	}
	return doc
}

func specRoutePairs(doc openAPIDoc) []routePair {
	var out []routePair
	for path, methods := range doc.Paths {
		for method := range methods {
			out = append(out, routePair{
				Method: strings.ToUpper(method),
				Path:   path,
			})
		}
	}
	slices.SortFunc(out, func(a, b routePair) int {
		if n := strings.Compare(a.Path, b.Path); n != 0 {
			return n
		}
		return strings.Compare(a.Method, b.Method)
	})
	return out
}

func routerRoutePairs(t *testing.T) []routePair {
	t.Helper()
	server := api.New(nil, nil, slog.Default(), "")
	handler := server.Router()
	mux, ok := handler.(*chi.Mux)
	if !ok {
		t.Fatal("Router() did not return a *chi.Mux")
	}
	var out []routePair
	_ = chi.Walk(mux, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		out = append(out, routePair{
			Method: method,
			Path:   route,
		})
		return nil
	})
	slices.SortFunc(out, func(a, b routePair) int {
		if n := strings.Compare(a.Path, b.Path); n != 0 {
			return n
		}
		return strings.Compare(a.Method, b.Method)
	})
	return out
}
