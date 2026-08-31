package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// A parameter documented with a bare type tells a caller nothing, but a
// parameter documented with the wrong bound is worse: a generated client
// refuses a legal request before it is ever sent. So these tests do not
// check that constraints exist — they read each declared constraint out of
// the shipped spec and drive the real router at its edges, requiring the
// handler to accept what the spec permits and reject what it forbids.

// addressG is a well-formed Stellar account address for the
// /addresses/{address} routes, which validate the strkey before querying.
const addressG = "GDW6AUTBXTOC7FIKUO5BOO3OGLK4SF7ZPOBLMQHMZDI45J2Z6VXRB5NR"

// specParam is one parameter declaration, either inline or behind a $ref.
type specParam struct {
	Ref         string `json:"$ref"`
	Name        string `json:"name"`
	In          string `json:"in"`
	Required    bool   `json:"required"`
	Description string `json:"description"`
	Schema      struct {
		Type      string `json:"type"`
		Enum      []any  `json:"enum"`
		Minimum   *int   `json:"minimum"`
		Maximum   *int   `json:"maximum"`
		MaxLength *int   `json:"maxLength"`
		Pattern   string `json:"pattern"`
		Default   any    `json:"default"`
	} `json:"schema"`
}

type paramDoc struct {
	Paths map[string]map[string]struct {
		Parameters []specParam `json:"parameters"`
	} `json:"paths"`
	Components struct {
		Parameters map[string]specParam `json:"parameters"`
	} `json:"components"`
}

func loadParamSpec(t *testing.T) paramDoc {
	t.Helper()
	var doc paramDoc
	require.NoError(t, json.Unmarshal(openapiSpec, &doc))
	return doc
}

// param returns the declared parameter of the given name on an operation,
// resolving a $ref into components/parameters.
func (d paramDoc) param(t *testing.T, method, route, name string) specParam {
	t.Helper()
	op, ok := d.Paths[route][method]
	require.Truef(t, ok, "spec has no %s %s", method, route)
	for _, p := range op.Parameters {
		resolved := p
		if p.Ref != "" {
			const prefix = "#/components/parameters/"
			require.True(t, strings.HasPrefix(p.Ref, prefix), "unexpected $ref %q", p.Ref)
			target, ok := d.Components.Parameters[strings.TrimPrefix(p.Ref, prefix)]
			require.Truef(t, ok, "spec references %s, which does not exist", p.Ref)
			resolved = target
		}
		if resolved.Name == name {
			return resolved
		}
	}
	t.Fatalf("%s %s declares no parameter %q", method, route, name)
	return specParam{}
}

// probe issues one request against a fresh single-tenant server and reports
// the status. Anything other than 400 counts as "the handler accepted this
// value"; the stub store answers every accepted request 200.
func probe(t *testing.T, method, target string) int {
	t.Helper()
	st := &stubStore{events: []store.Event{{ID: "ev-1", ContractID: contractA, Ledger: 1}}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-API-Key", "test-key")
	newTestServer(st, nil).Router().ServeHTTP(rec, req)
	return rec.Code
}

// withValue substitutes a parameter value into a URL template that carries
// a single %s placeholder.
func withValue(template, value string) string {
	return fmt.Sprintf(template, value)
}

// probeDeliveries drives the delivery listing against a store that really
// holds the subscription, because the handler checks existence before it
// validates the page size — against the default stub the 404 would arrive
// first and the bound would never be reached.
func probeDeliveries(t *testing.T, method, target string) int {
	t.Helper()
	st := newSubStore()
	sub, err := st.CreateSubscription(t.Context(), store.Subscription{URL: "https://example.test"})
	require.NoError(t, err)
	require.Equal(t, int64(1), sub.ID, "the template addresses subscription 1")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-API-Key", "test-key")
	newServerFromStub(st).Router().ServeHTTP(rec, req)
	return rec.Code
}

// boundedParam pairs a declared numeric bound with a request that exercises
// it, so the assertion is driven by the spec rather than restating it.
type boundedParam struct {
	name     string
	method   string
	route    string
	param    string
	template string
	// probeWith overrides the default single-tenant prober for the few
	// endpoints that need a differently-populated store.
	probeWith func(t *testing.T, method, target string) int
}

func (b boundedParam) run(t *testing.T, method, target string) int {
	t.Helper()
	if b.probeWith != nil {
		return b.probeWith(t, method, target)
	}
	return probe(t, method, target)
}

// TestDeclaredBoundsMatchTheHandlers reads each parameter's minimum and
// maximum out of the spec and requires the handler to accept the boundary
// value and reject the one immediately outside it. A spec that overstates a
// bound fails on the "just inside" probe; one that understates it fails on
// "just outside".
func TestDeclaredBoundsMatchTheHandlers(t *testing.T) {
	doc := loadParamSpec(t)

	for _, tc := range []boundedParam{
		{"limit on /events", "get", "/events", "limit", "/events?limit=%s", nil},
		{"limit on /events/{id}-adjacent listing", "get", "/contracts/{id}/events", "limit",
			"/contracts/" + contractA + "/events?limit=%s", nil},
		{"limit on /addresses/{address}/events", "get", "/addresses/{address}/events", "limit",
			"/addresses/" + addressG + "/events?limit=%s", nil},
		{"limit on /contracts", "get", "/contracts", "limit", "/contracts?limit=%s", nil},
		{"limit on /dead-letters", "get", "/dead-letters", "limit", "/dead-letters?limit=%s", nil},
		{"limit on /subscriptions/{id}/deliveries", "get", "/subscriptions/{id}/deliveries", "limit",
			"/subscriptions/1/deliveries?limit=%s", probeDeliveries},
		{"from_ledger on /events", "get", "/events", "from_ledger", "/events?from_ledger=%s", nil},
		{"to_ledger on /events", "get", "/events", "to_ledger", "/events?to_ledger=%s&from_ledger=1", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := doc.param(t, tc.method, tc.route, tc.param)
			require.NotEmpty(t, p.Description, "%s must describe what it does", tc.param)

			if p.Schema.Minimum != nil {
				min := *p.Schema.Minimum
				assert.NotEqualf(t, http.StatusBadRequest,
					tc.run(t, strings.ToUpper(tc.method), withValue(tc.template, strconv.Itoa(min))),
					"spec allows %s=%d but the handler rejects it", tc.param, min)
				assert.Equalf(t, http.StatusBadRequest,
					tc.run(t, strings.ToUpper(tc.method), withValue(tc.template, strconv.Itoa(min-1))),
					"spec forbids %s=%d but the handler accepts it", tc.param, min-1)
			}
			if p.Schema.Maximum != nil {
				max := *p.Schema.Maximum
				assert.NotEqualf(t, http.StatusBadRequest,
					tc.run(t, strings.ToUpper(tc.method), withValue(tc.template, strconv.Itoa(max))),
					"spec allows %s=%d but the handler rejects it", tc.param, max)
				assert.Equalf(t, http.StatusBadRequest,
					tc.run(t, strings.ToUpper(tc.method), withValue(tc.template, strconv.Itoa(max+1))),
					"spec forbids %s=%d but the handler accepts it", tc.param, max+1)
			}
		})
	}
}

// TestDeclaredEnumsMatchTheHandlers requires each declared enum to be the
// whole truth: exactly the set the handler accepts, no wider and no
// narrower. Checking only that declared values work would let the spec
// understate an enum — dropping "desc" would still pass — and a generated
// client built from the short list refuses a request the server honours.
func TestDeclaredEnumsMatchTheHandlers(t *testing.T) {
	doc := loadParamSpec(t)

	for _, tc := range []struct {
		name     string
		method   string
		route    string
		param    string
		template string
		// accepted is the set the parsing code allows, transcribed from
		// the switch or validator that decides it. Every entry is probed
		// against the real handler, so this list cannot drift either.
		accepted []string
		// flag marks a parameter the handler reads as a boolean switch
		// rather than validating, so an unknown value is ignored.
		flag bool
	}{
		{
			name: "order on /events", method: "get", route: "/events", param: "order",
			template: "/events?order=%s", accepted: []string{"asc", "desc"},
		},
		{
			name: "order_by on /events", method: "get", route: "/events", param: "order_by",
			template: "/events?order_by=%s", accepted: []string{"id", "ledger", "created_at"},
		},
		{
			name: "order on /contracts", method: "get", route: "/contracts", param: "order",
			template: "/contracts?order=%s", accepted: []string{"asc", "desc"},
		},
		{
			name: "sort on /contracts", method: "get", route: "/contracts", param: "sort",
			template: "/contracts?sort=%s",
			accepted: []string{"contract_id", "count", "first_ledger", "last_ledger", "last_seen"},
		},
		{
			name: "format on the contract export", method: "get",
			route: "/contracts/{id}/export", param: "format",
			template: "/contracts/" + contractA + "/export?from_ledger=1&to_ledger=2&format=%s",
			accepted: []string{"csv", "ndjson"},
		},
		{
			name: "decoded on /events", method: "get", route: "/events", param: "decoded",
			template: "/events?decoded=%s", accepted: []string{"true"}, flag: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := doc.param(t, tc.method, tc.route, tc.param)
			require.NotEmptyf(t, p.Schema.Enum, "%s takes a fixed set of values and must declare an enum", tc.param)
			require.NotEmpty(t, p.Description, "%s must describe what it does", tc.param)

			declared := make([]string, 0, len(p.Schema.Enum))
			for _, v := range p.Schema.Enum {
				declared = append(declared, fmt.Sprint(v))
			}
			assert.ElementsMatchf(t, tc.accepted, declared,
				"the declared enum for %s is not the set the handler accepts", tc.param)

			for _, value := range tc.accepted {
				assert.NotEqualf(t, http.StatusBadRequest,
					probe(t, http.MethodGet, withValue(tc.template, value)),
					"the handler must accept %s=%q", tc.param, value)
			}
			if tc.flag {
				return
			}
			assert.Equalf(t, http.StatusBadRequest,
				probe(t, http.MethodGet, withValue(tc.template, "not-a-valid-value")),
				"a value outside the enum must be rejected, so the enum is exhaustive")
		})
	}
}

// TestRequiredParametersAreRequired checks the parameters the spec marks
// required really are refused when absent — and, for the two the spec named
// wrongly, that supplying the documented one now works.
func TestRequiredParametersAreRequired(t *testing.T) {
	doc := loadParamSpec(t)

	t.Run("bucket on /events/aggregate", func(t *testing.T) {
		p := doc.param(t, "get", "/events/aggregate", "bucket")
		require.True(t, p.Required, "the handler refuses the request without it")
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events/aggregate"))
		assert.NotEqual(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events/aggregate?bucket=ledger"))
		assert.NotEqual(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events/aggregate?bucket=1h"))
	})

	t.Run("before_ledger on DELETE /events", func(t *testing.T) {
		p := doc.param(t, "delete", "/events", "before_ledger")
		require.True(t, p.Required)
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodDelete, "/events"),
			"the handler refuses a prune with no bound")
		// The spec used to name to_ledger here, which the handler never reads.
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodDelete, "/events?to_ledger=5"),
			"to_ledger alone must not satisfy the requirement")
		assert.NotEqual(t, http.StatusBadRequest, probe(t, http.MethodDelete, "/events?before_ledger=5"))
	})

	t.Run("ledger bounds on the contract export", func(t *testing.T) {
		for _, name := range []string{"from_ledger", "to_ledger"} {
			require.Truef(t, doc.param(t, "get", "/contracts/{id}/export", name).Required,
				"%s is required for an export", name)
		}
		base := "/contracts/" + contractA + "/export"
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, base))
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, base+"?from_ledger=1"))
		assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, base+"?to_ledger=2"))
		assert.NotEqual(t, http.StatusBadRequest, probe(t, http.MethodGet, base+"?from_ledger=1&to_ledger=2"))
	})
}

// TestOpaqueParametersSayTheyAreOpaque covers the cursor: a caller has to
// learn from the spec that it must not be built by hand, and the declared
// shape has to be the shape the validator accepts.
func TestOpaqueParametersSayTheyAreOpaque(t *testing.T) {
	doc := loadParamSpec(t)
	p := doc.param(t, "get", "/events", "cursor")

	desc := strings.ToLower(p.Description)
	assert.Contains(t, desc, "opaque", "the cursor must be documented as opaque")
	assert.Truef(t, strings.Contains(desc, "must not"),
		"the cursor description must tell clients not to construct one, got: %s", p.Description)

	require.NotNil(t, p.Schema.MaxLength, "the validator bounds cursor length; the spec must say so")
	max := *p.Schema.MaxLength
	assert.NotEqual(t, http.StatusBadRequest,
		probe(t, http.MethodGet, "/events?cursor="+strings.Repeat("a", max)),
		"a cursor at the declared maximum length must be accepted")
	assert.Equal(t, http.StatusBadRequest,
		probe(t, http.MethodGet, "/events?cursor="+strings.Repeat("a", max+1)),
		"a cursor past the declared maximum length must be rejected")

	require.NotEmpty(t, p.Schema.Pattern, "the validator bounds the cursor alphabet; the spec must say so")
	assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events?cursor=has%20space"),
		"a cursor outside the declared alphabet must be rejected")
}

// TestContractIDPatternMatchesTheValidator pins the alphabet. The spec used
// to declare ^C[A-Z0-9]{55}$, which admits 0, 1, 8 and 9 — digits a base32
// strkey can never contain — so a client generated from it would have
// accepted an ID the server always refuses.
func TestContractIDPatternMatchesTheValidator(t *testing.T) {
	doc := loadParamSpec(t)
	p := doc.param(t, "get", "/events", "contract_id")
	assert.NotContains(t, p.Schema.Pattern, "A-Z0-9",
		"the strkey alphabet is base32; 0, 1, 8 and 9 never appear in it")

	assert.NotEqual(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events?contract_id="+contractA))
	// contractA with one character swapped for a digit outside base32.
	withZero := "C0" + contractA[2:]
	assert.Equal(t, http.StatusBadRequest, probe(t, http.MethodGet, "/events?contract_id="+withZero),
		"a contract ID containing 0 must be rejected, as the validator does")

	// The comma-separated form the description promises really works.
	assert.NotEqual(t, http.StatusBadRequest,
		probe(t, http.MethodGet, "/events?contract_id="+contractA+","+contractB))
	assert.Equal(t, http.StatusBadRequest,
		probe(t, http.MethodGet, "/events?contract_id="+contractA+",nonsense"),
		"one bad element rejects the whole list rather than being dropped")
}

// TestUndeclaredParametersAreNotDocumented is the other half of "constraints
// match what the handlers enforce": the spec used to advertise a limit on
// /events/count and a limit and cursor on /events.csv, all three of which
// the handler overwrites before querying. Documenting a knob that does
// nothing is its own kind of wrong answer.
func TestUndeclaredParametersAreNotDocumented(t *testing.T) {
	doc := loadParamSpec(t)

	assertNotDeclared := func(method, route, name string) {
		t.Helper()
		op := doc.Paths[route][method]
		for _, p := range op.Parameters {
			resolved := p
			if p.Ref != "" {
				resolved = doc.Components.Parameters[strings.TrimPrefix(p.Ref, "#/components/parameters/")]
			}
			assert.NotEqualf(t, name, resolved.Name,
				"%s %s documents %q, but the handler discards it", method, route, name)
		}
	}

	t.Run("count discards pagination", func(t *testing.T) {
		assertNotDeclared("get", "/events/count", "limit")
		assertNotDeclared("get", "/events/count", "cursor")

		st := &stubStore{totalCount: 3}
		rec := httptest.NewRecorder()
		newTestServer(st, nil).Router().ServeHTTP(rec,
			httptest.NewRequest(http.MethodGet, "/events/count?limit=1", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Zero(t, st.lastCountFilter.Limit, "the handler must strip limit before counting")
		assert.JSONEq(t, `{"count":3}`, rec.Body.String(),
			"the count is over the whole matching set, not one page of it")
	})

	t.Run("csv export discards pagination", func(t *testing.T) {
		assertNotDeclared("get", "/events.csv", "limit")
		assertNotDeclared("get", "/events.csv", "cursor")

		st := &stubStore{events: []store.Event{{ID: "ev-1", ContractID: contractA}}}
		rec := httptest.NewRecorder()
		newTestServer(st, nil).Router().ServeHTTP(rec,
			httptest.NewRequest(http.MethodGet, "/events.csv?limit=1&cursor=abc", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, st.lastFilter.Cursor, "the export starts from the beginning regardless of cursor")
		assert.NotEqual(t, 1, st.lastFilter.Limit, "the export pages at its own batch size")
	})
}

// TestEveryDeclaredParameterHasADescription is the sweep the issue asks
// for: no parameter anywhere in the spec may be a bare type.
func TestEveryDeclaredParameterHasADescription(t *testing.T) {
	doc := loadParamSpec(t)

	for name, p := range doc.Components.Parameters {
		assert.NotEmptyf(t, p.Description, "components/parameters/%s has no description", name)
	}
	for route, pathItem := range doc.Paths {
		for method, op := range pathItem {
			for _, p := range op.Parameters {
				if p.Ref != "" {
					target, ok := doc.Components.Parameters[strings.TrimPrefix(p.Ref, "#/components/parameters/")]
					require.Truef(t, ok, "%s %s references %s, which does not exist", method, route, p.Ref)
					assert.NotEmptyf(t, target.Description, "%s resolves to a parameter with no description", p.Ref)
					continue
				}
				assert.NotEmptyf(t, p.Description,
					"%s %s declares %q with no description", method, route, p.Name)
			}
		}
	}
}
