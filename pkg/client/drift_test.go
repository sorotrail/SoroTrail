package client_test

// The unified client drift gate verifies that:
//   1. Every client target declared in pkg/client/targets.yaml pins an exact
//      generator version, regenerates byte-for-byte identically from
//      api/openapi.yaml, and compiles/type-checks cleanly.
//   2. Any drift failure names the affected target language and OpenAPI
//      operation (operationId + METHOD /path) rather than dumping a bare diff.
//   3. The HTTP router, embedded internal/api/openapi.json, api/openapi.yaml,
//      and generated client SpecRoutes all expose the exact same route set.
//   4. Every client target passes the shared cross-language contract test
//      against a canonical httptest server.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/clientgen"
	"github.com/sorotrail/sorotrail/internal/specgen"
	"github.com/sorotrail/sorotrail/pkg/client"
)

const (
	specPath         = "../../api/openapi.yaml"
	embeddedJSONPath = "../../internal/api/openapi.json"
	manifestPath     = "targets.yaml"
	genPath          = "client.gen.go"
)

func repoRootFromClientPkg(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// TestGeneratedClientIsUpToDate is the reproducibility gate: the
// committed client must be exactly what the generator produces from the
// committed spec. Regenerate with `make client`.
func TestGeneratedClientIsUpToDate(t *testing.T) {
	specBytes, err := os.ReadFile(specPath)
	require.NoError(t, err)

	want, err := clientgen.Generate(specBytes)
	require.NoError(t, err)

	got, err := os.ReadFile(genPath)
	require.NoError(t, err)

	diag := clientgen.DiagnoseClientDrift("go", "pkg/client/client.gen.go", specBytes, got, want)
	assert.Empty(t, diag,
		"pkg/client/client.gen.go is stale; run `make client` after changing api/openapi.yaml\n%s", diag)
}

// TestManifestTargetsArePinnedAndUpToDate drives the unified drift gate from
// pkg/client/targets.yaml, regenerating and type-checking every registered
// language client.
func TestManifestTargetsArePinnedAndUpToDate(t *testing.T) {
	m, err := clientgen.LoadManifest(manifestPath)
	require.NoError(t, err)
	require.NotEmpty(t, m.Targets)

	repoRoot := repoRootFromClientPkg(t)
	ctx := context.Background()

	for _, target := range m.Targets {
		t.Run(target.Language, func(t *testing.T) {
			err := clientgen.VerifyTarget(ctx, repoRoot, m.Spec, target)
			require.NoError(t, err)
		})
	}
}

// TestManifestRejectsUnpinnedGeneratorVersion ensures an upstream generator
// cannot be referenced with a floating tag like "latest" or "main".
func TestManifestRejectsUnpinnedGeneratorVersion(t *testing.T) {
	for _, badVersion := range []string{"", "latest", "main", "HEAD", "v1", "v1.2"} {
		_, err := clientgen.ParseManifest([]byte(`
schema_version: 1
spec: api/openapi.yaml
targets:
  - language: typescript
    generator: openapi-typescript
    version: "` + badVersion + `"
    output: sdk/ts/client.ts
    generate_cmd: ["npx", "openapi-typescript", "{spec}", "-o", "{output}"]
    typecheck_cmd: ["npx", "tsc", "--noEmit"]
`))
		require.Error(t, err, "version %q must be rejected", badVersion)
		assert.Contains(t, err.Error(), "not pinned to an exact semver release")
	}
}

// TestDiagnoseClientDrift_NamesLanguageAndOperation proves that when a spec
// change alters an operation or schema without regenerating the client, the
// diagnostic error names the target language and the affected operationId +
// HTTP route rather than emitting only a bare diff.
func TestDiagnoseClientDrift_NamesLanguageAndOperation(t *testing.T) {
	specBytes, err := os.ReadFile(specPath)
	require.NoError(t, err)

	committed, err := os.ReadFile(genPath)
	require.NoError(t, err)

	// Simulate adding a query parameter to GET /events (listEvents) in the spec
	// without regenerating the committed client.
	mutatedSpec := strings.Replace(
		string(specBytes),
		"      operationId: listEvents\n",
		"      operationId: listEvents\n      parameters:\n        - name: topic_filter\n          in: query\n          schema:\n            type: string\n",
		1,
	)
	// If listEvents already has a parameters block, inject an extra query parameter at the top of its parameters list.
	if strings.Count(mutatedSpec, "parameters:") != strings.Count(string(specBytes), "parameters:")+1 {
		mutatedSpec = strings.Replace(
			string(specBytes),
			"operationId: listEvents\n      summary:",
			"operationId: listEvents\n      summary:",
			1,
		)
	}
	mutatedSpecBytes := injectSyntheticParamIntoListEvents(t, specBytes)
	generated, err := clientgen.Generate(mutatedSpecBytes)
	require.NoError(t, err)

	msg := clientgen.DiagnoseClientDrift("go", "pkg/client/client.gen.go", mutatedSpecBytes, committed, generated)
	require.NotEmpty(t, msg)
	assert.Contains(t, msg, `language="go"`)
	assert.Contains(t, msg, "listEvents (GET /events)")
	assert.Contains(t, msg, "make client")
}

func injectSyntheticParamIntoListEvents(t *testing.T, specBytes []byte) []byte {
	t.Helper()
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(specBytes, &doc))
	paths, ok := doc["paths"].(map[string]any)
	require.True(t, ok)
	eventsPath, ok := paths["/events"].(map[string]any)
	require.True(t, ok)
	getOp, ok := eventsPath["get"].(map[string]any)
	require.True(t, ok)
	params, _ := getOp["parameters"].([]any)
	params = append(params, map[string]any{
		"name": "synthetic_drift_probe",
		"in":   "query",
		"schema": map[string]any{
			"type": "string",
		},
	})
	getOp["parameters"] = params
	out, err := yaml.Marshal(doc)
	require.NoError(t, err)
	return out
}

// TestSpecRoutesMatchSpec asserts the exposed route table and embedded OpenAPI
// JSON cover the exact routes the spec and HTTP router implement, so route
// drift and client drift are reported together by the same gate.
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

	// Also verify router-to-spec parity and embedded openapi.json freshness so
	// the client drift gate subsumes specification coverage in one run.
	routerRoutes := readRouterRoutes(t)
	for _, sr := range want {
		assert.True(t, slices.Contains(routerRoutes, sr),
			"OpenAPI spec documents %s %s but the HTTP router does not mount it", sr.Method, sr.Path)
	}
	for _, rr := range routerRoutes {
		if rr.Path == "/docs" || rr.Path == "/openapi.json" {
			continue
		}
		assert.True(t, slices.Contains(want, rr),
			"HTTP router mounts %s %s but it is missing from api/openapi.yaml and generated clients", rr.Method, rr.Path)
	}

	rawSpec, err := os.ReadFile(specPath)
	require.NoError(t, err)
	wantJSON, err := specgen.Render(rawSpec)
	require.NoError(t, err)
	gotJSON, err := os.ReadFile(embeddedJSONPath)
	require.NoError(t, err)
	assert.Equal(t, string(wantJSON), strings.ReplaceAll(string(gotJSON), "\r\n", "\n"),
		"internal/api/openapi.json is stale relative to api/openapi.yaml; run `make spec`")
}

// TestCrossLanguageContractHarness executes the same canonical API requests
// against an httptest server for every client target in pkg/client/targets.yaml
// and asserts they produce equivalent normalized contract responses.
func TestCrossLanguageContractHarness(t *testing.T) {
	m, err := clientgen.LoadManifest(manifestPath)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","latest_ledger":9001}`))
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"events":[{"id":"evt-contract-1","contract_id":"CCONTRACT1","ledger":9001,"type":"contract","tx_hash":"tx-1","topics":[{"symbol":"transfer"}],"value":{"amount":42}}],"cursor":"cursor-2"}`))
	})
	mux.HandleFunc("/events/evt-contract-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"evt-contract-1","contract_id":"CCONTRACT1","ledger":9001,"type":"contract","tx_hash":"tx-1","topics":[{"symbol":"transfer"}],"value":{"amount":42}}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	type contractSnapshot struct {
		HealthStatus string `json:"health_status"`
		EventID      string `json:"event_id"`
		ContractID   string `json:"contract_id"`
		Ledger       int64  `json:"ledger"`
		Cursor       string `json:"cursor"`
		FetchedID    string `json:"fetched_id"`
	}

	baseline := contractSnapshot{
		HealthStatus: "ok",
		EventID:      "evt-contract-1",
		ContractID:   "CCONTRACT1",
		Ledger:       9001,
		Cursor:       "cursor-2",
		FetchedID:    "evt-contract-1",
	}

	for _, target := range m.Targets {
		t.Run(target.Language, func(t *testing.T) {
			switch target.Language {
			case "go":
				c := client.New(srv.URL, client.WithAPIKey("contract-test-key"))
				ctx := context.Background()

				health, err := c.Health(ctx)
				require.NoError(t, err)

				page, err := c.ListEvents(ctx, client.ListEventsParams{
					ContractID: "CCONTRACT1",
					Limit:      10,
				})
				require.NoError(t, err)
				require.Len(t, page.Events, 1)

				ev, err := c.GetEvent(ctx, "evt-contract-1", client.GetEventParams{})
				require.NoError(t, err)

				got := contractSnapshot{
					HealthStatus: health.Status,
					EventID:      page.Events[0].ID,
					ContractID:   page.Events[0].ContractID,
					Ledger:       page.Events[0].Ledger,
					Cursor:       page.Cursor,
					FetchedID:    ev.ID,
				}
				wantBytes, _ := json.Marshal(baseline)
				gotBytes, _ := json.Marshal(got)
				assert.JSONEq(t, string(wantBytes), string(gotBytes),
					"client target %q contract response diverged from canonical baseline", target.Language)
			default:
				t.Fatalf("client target %q declared in %s has no contract adapter in TestCrossLanguageContractHarness", target.Language, manifestPath)
			}
		})
	}
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

func readRouterRoutes(t *testing.T) []client.SpecRoute {
	t.Helper()
	server := api.New(nil, nil, slog.Default(), "")
	handler := server.Router()
	mux, ok := handler.(*chi.Mux)
	require.True(t, ok, "Router() did not return a *chi.Mux")

	var out []client.SpecRoute
	_ = chi.Walk(mux, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		out = append(out, client.SpecRoute{
			Method: strings.ToUpper(method),
			Path:   route,
		})
		return nil
	})
	return out
}
