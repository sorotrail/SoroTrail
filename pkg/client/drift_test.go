package client_test

// The generated client is only load-bearing if the committed file
// matches the spec. This test regenerates from api/openapi.yaml — the
// same command `make client` runs — and compares byte-for-byte against
// the committed pkg/client/client.gen.go, then cross-checks the exposed
// SpecRoutes and SpecVersion against the live document.
//
// A spec change that is not followed by `make client` fails here with a
// diff, so a drift between the spec and the shipped client breaks CI
// rather than someone's integration.

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sorotrail/sorotrail/internal/clientgen"
	"github.com/sorotrail/sorotrail/pkg/client"
)

const (
	specPath = "../../api/openapi.yaml"
	genPath  = "client.gen.go"
)

// TestGeneratedClientIsUpToDate is the reproducibility gate: the
// committed client must be exactly what the generator produces from the
// committed spec. Regenerate with `make client`.
func TestGeneratedClientIsUpToDate(t *testing.T) {
	want, err := clientgen.GenerateFromFile(specPath)
	require.NoError(t, err)

	got, err := os.ReadFile(genPath)
	require.NoError(t, err)

	assert.Equal(t, string(want), string(got),
		"pkg/client/client.gen.go is stale; run `make client` after changing api/openapi.yaml")
}

// TestSpecRoutesMatchSpec asserts the exposed route table covers exactly
// the routes the spec documents — the same (method, path) pairs, no
// more, no fewer.
func TestSpecRoutesMatchSpec(t *testing.T) {
	version, routes := readSpec(t)
	assert.Equal(t, version, client.SpecVersion,
		"SpecVersion is stale; run `make client` after bumping info.version")

	want := make([]client.SpecRoute, 0, len(routes))
	for _, r := range routes {
		want = append(want, client.SpecRoute{Method: r.Method, Path: r.Path})
	}
	assert.ElementsMatch(t, want, client.SpecRoutes,
		"SpecRoutes must match api/openapi.yaml exactly")
}

// readSpec extracts info.version and the route set from the live spec.
func readSpec(t *testing.T) (string, []client.SpecRoute) {
	t.Helper()
	src, err := os.ReadFile(specPath)
	require.NoError(t, err)

	var doc struct {
		Info struct {
			Version string `yaml:"version"`
		} `yaml:"info"`
		Paths map[string]map[string]struct{} `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(src, &doc))

	routes := make([]client.SpecRoute, 0, len(doc.Paths))
	for path, methods := range doc.Paths {
		for method := range methods {
			routes = append(routes, client.SpecRoute{Method: strings.ToUpper(method), Path: path})
		}
	}
	return doc.Info.Version, routes
}
