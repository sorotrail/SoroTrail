package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api/queries"
	"github.com/sorotrail/sorotrail/internal/store"
)

// internal/api/queries is the shared parameter layer: one parser per
// parameter type, consumed by both the REST handlers and the GraphQL
// resolvers. This file checks the three properties that make sharing worth
// anything — that every endpoint accepting a parameter rejects bad values for
// it the same way, that the wording comes from the shared parser rather than a
// hand-copied string, and that the numeric bounds come from configuration
// rather than a literal at the call site.
//
// The class of bug this catches is the one the issue names: `limit` capped at
// a hardcoded 500 in one handler while another honoured the configured
// maximum, so the same request succeeded or failed depending on which endpoint
// a client happened to pick. Where a bound legitimately differs (the contract
// listing) it is named in limitBoundExceptions below, which turns drift into a
// decision a reviewer can see.

// paramEndpoint is one route that accepts a shared query parameter, plus the
// state its handler needs in place before it gets as far as validating that
// parameter.
type paramEndpoint struct {
	// name is the route as openapi.json spells it, so a failure names the
	// endpoint a reader has to go fix.
	name string
	// target is a URL template ending where the parameter goes: the caller
	// appends "<name>=%s".
	target string
	// stub supplies a store for handlers that resolve a path parameter before
	// they validate the query string, where the default stub would 404 first.
	stub func(t *testing.T) store.Store
}

// routeRegistry names every endpoint this file drives, with the URL prefix that
// reaches its parameter validation. Every entry has to be claimed by a
// parameter below, which TestParameterRoutesAreWorthComparing enforces, so the
// registry cannot accumulate routes that nothing checks.
//
// Two adjacent routes are deliberately absent. GET /events/ws upgrades to a
// websocket before it parses anything, so its rejection surfaces as a closed
// stream rather than a 400 envelope and it has transport-level coverage
// elsewhere. GET /watched-contracts returns the operator's whole watch list
// and reads no query parameters at all, so there is nothing to cross-check.
var routeRegistry = map[string]paramEndpoint{
	"GET /events": {name: "GET /events", target: "/events?"},
	"GET /contracts/{id}/events": {name: "GET /contracts/{id}/events",
		target: "/contracts/" + contractA + "/events?contract_id=" + contractA + "&"},
	"GET /events/count":     {name: "GET /events/count", target: "/events/count?"},
	"GET /events/aggregate": {name: "GET /events/aggregate", target: "/events/aggregate?bucket=ledger&"},
	"GET /events.csv":       {name: "GET /events.csv", target: "/events.csv?"},
	"GET /contracts/{id}/export": {name: "GET /contracts/{id}/export",
		target: "/contracts/" + contractA + "/export?to_ledger=9&"},
	"GET /addresses/{address}/events": {name: "GET /addresses/{address}/events",
		target: "/addresses/" + contractA + "/events?"},
	"GET /contracts":    {name: "GET /contracts", target: "/contracts?"},
	"GET /dead-letters": {name: "GET /dead-letters", target: "/dead-letters?"},
	"GET /subscriptions/{id}/deliveries": {
		name:   "GET /subscriptions/{id}/deliveries",
		target: "/subscriptions/1/deliveries?",
		// The handler checks the subscription exists before it reads the page
		// size, so the default stub would answer 404 and the bound would never
		// be reached.
		stub: func(t *testing.T) store.Store {
			st := newSubStore()
			_, err := st.CreateSubscription(t.Context(), store.Subscription{URL: "https://example.test"})
			require.NoError(t, err)
			return st
		},
	},
}

// paramRoutes are the shared parameters this file holds to one parser, and the
// routes that accept each. The map is what makes the tables below more than a
// wish: routesFor reads it, so adding an endpoint means every parameter it
// takes is cross-checked automatically, and a route listed here that
// routeRegistry does not know fails rather than being skipped.
//
// GET /contracts/{id}/export is listed under from_ledger only. It requires both
// bounds, so its registry target fixes to_ledger=9 — appending a second
// to_ledger there would leave two values for one key, and the handler would
// read the fixed one and pass whatever bad value this file supplied.
var paramRoutes = map[string][]string{
	"from_ledger": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate",
		"GET /events.csv", "GET /addresses/{address}/events", "GET /contracts/{id}/export"},
	"to_ledger": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate",
		"GET /events.csv"},
	"from_time": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate",
		"GET /events.csv"},
	"to_time": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate",
		"GET /events.csv"},
	"type": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate",
		"GET /events.csv", "GET /addresses/{address}/events"},
	"topic":          {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate", "GET /events.csv"},
	"topic0":         {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate", "GET /events.csv"},
	"topic_contains": {"GET /events", "GET /contracts/{id}/events", "GET /events/count", "GET /events/aggregate", "GET /events.csv"},
	"order":          {"GET /events", "GET /events.csv", "GET /contracts", "GET /addresses/{address}/events"},
	"order_by":       {"GET /events", "GET /contracts/{id}/events", "GET /events.csv"},
	"cursor":         {"GET /events", "GET /contracts", "GET /dead-letters", "GET /addresses/{address}/events"},
	"limit": {"GET /events", "GET /contracts/{id}/events", "GET /addresses/{address}/events",
		"GET /dead-letters", "GET /contracts", "GET /subscriptions/{id}/deliveries"},
	// recent is deliberately alone: it is a REST-only shorthand the shared
	// builder does not know about, so there is no second endpoint to agree
	// with, and its own bound is asserted below.
	"recent": {"GET /events"},
}

// routesFor resolves a parameter's route list into ready-to-drive endpoints,
// each with the parameter appended to its template.
func routesFor(t *testing.T, param string) []paramEndpoint {
	t.Helper()
	names, ok := paramRoutes[param]
	require.Truef(t, ok, "paramRoutes has no entry for %q", param)
	out := make([]paramEndpoint, 0, len(names))
	for _, name := range names {
		route, ok := routeRegistry[name]
		require.Truef(t, ok, "%q lists %s, which routeRegistry does not know", param, name)
		assert.Equalf(t, name, route.name, "routeRegistry key %q maps to an endpoint named %q", name, route.name)
		route.target += param + "=%s"
		out = append(out, route)
	}
	return out
}

// routeFor picks one endpoint out of an already-resolved route list, so a
// subtest that needs a single route still gets its parameter slot from
// routesFor rather than restating the URL.
func routeFor(t *testing.T, routes []paramEndpoint, name string) paramEndpoint {
	t.Helper()
	for _, route := range routes {
		if route.name == name {
			return route
		}
	}
	require.FailNowf(t, "route not in the list", "%s was not resolved", name)
	return paramEndpoint{}
}

func TestParameterRoutesAreWorthComparing(t *testing.T) {
	// One endpoint rejecting a bad value proves nothing about the others, so
	// the cross-endpoint claims in this file rest on the route lists being
	// wide enough to actually compare. This guards against a table quietly
	// shrinking until it passes vacuously.
	for param, names := range paramRoutes {
		param, names := param, names
		t.Run(param, func(t *testing.T) {
			seen := map[string]bool{}
			for _, name := range names {
				assert.Falsef(t, seen[name], "%s lists %s twice", param, name)
				seen[name] = true
			}
			if param == "recent" {
				return
			}
			assert.GreaterOrEqualf(t, len(names), 3,
				"%s is compared across %d endpoint(s); the claim needs at least three to mean anything",
				param, len(names))
			routes := routesFor(t, param)
			assert.Len(t, routes, len(names), "%s resolved to fewer endpoints than it lists", param)
		})
	}

	for name, route := range routeRegistry {
		assert.Truef(t, strings.HasSuffix(route.target, "?") || strings.HasSuffix(route.target, "&"),
			"%s: a target must end where a parameter goes, because routesFor appends there", name)
	}

	// The inverse of routesFor's check: a route registered here but claimed by
	// no parameter is a route nothing in this file drives, which is how a
	// shared-parser guarantee quietly stops covering an endpoint.
	claimed := map[string]bool{}
	for _, names := range paramRoutes {
		for _, name := range names {
			claimed[name] = true
		}
	}
	for name := range routeRegistry {
		assert.Truef(t, claimed[name],
			"%s is in routeRegistry but no parameter lists it — drop it or give it a parameter to share", name)
	}
}

func newParamServer(t *testing.T, ep paramEndpoint) *Server {
	t.Helper()
	if ep.stub != nil {
		return newServerFromStub(ep.stub(t))
	}
	// newTestServer wires the suite's standard API key and a store that
	// answers every accepted query with one row, so a 400 can only come from
	// validation.
	return newTestServer(&stubStore{events: []store.Event{{ID: "ev-1", ContractID: contractA, Ledger: 1}}}, nil)
}

// requestParam drives one value through one endpoint's parameter slot.
func requestParam(t *testing.T, ep paramEndpoint, value string) (int, string) {
	t.Helper()
	return requestURL(t, ep, fmt.Sprintf(ep.target, value))
}

// requestURL is requestParam for a fully-formed URL, which the multi-parameter
// cases need because a template with two fixed bounds has no slot left to fill.
// It drives the real router, so auth and scope middleware behave as they do in
// production. An accepted request's body is a result page rather than an error
// envelope, which reads back here as an empty message.
func requestURL(t *testing.T, ep paramEndpoint, url string) (int, string) {
	t.Helper()
	return requestMethodURL(t, ep, http.MethodGet, url)
}

func requestMethodURL(t *testing.T, ep paramEndpoint, method, url string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, url, nil)
	req.Header.Set("X-API-Key", "test-key")
	rec := httptest.NewRecorder()
	newParamServer(t, ep).Router().ServeHTTP(rec, req)

	if rec.Code >= http.StatusOK && rec.Code < http.StatusMultipleChoices {
		return rec.Code, ""
	}
	var envelope errorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		require.FailNowf(t, "rejection must be a JSON error envelope",
			"%s %s: status %d body %s", method, url, rec.Code, rec.Body.String())
	}
	return rec.Code, envelope.Error
}

// paramCase is one invalid value for one parameter type, with the message the
// shared layer produces for it.
type paramCase struct {
	value   string
	message string
}

// assertSharedRejection runs one bad value across every endpoint that accepts
// the parameter and asserts they all answer 400 with byte-identical text.
func assertSharedRejection(t *testing.T, param string, routes []paramEndpoint, tc paramCase) {
	t.Helper()
	var reference string
	var referenceRoute string
	for _, route := range routes {
		route := route
		t.Run(route.name, func(t *testing.T) {
			status, message := requestParam(t, route, tc.value)
			require.Equalf(t, http.StatusBadRequest, status,
				"%s accepted ?%s=%s — an endpoint that takes a parameter has to reject it when it is bad",
				route.name, param, tc.value)
			assert.Containsf(t, message, tc.message,
				"%s: ?%s=%s must be explained with the shared parser's wording", route.name, param, tc.value)
			if reference == "" {
				reference, referenceRoute = message, route.name
				return
			}
			assert.Equalf(t, reference, message,
				"%s answers ?%s=%s differently from %s: one parser per parameter type means one message",
				route.name, param, tc.value, referenceRoute)
		})
	}
}

func TestLedgerParametersShareOneParser(t *testing.T) {
	// "must be a positive integer" is queries.ParseLedgerParam's own text; the
	// parameter name is prefixed by the caller, which is the convention that
	// package documents. Every endpoint has to land on the same composition.
	_, sharedErr := queries.ParseLedgerParam("0")
	require.Error(t, sharedErr)

	routes := routesFor(t, "from_ledger")

	tests := []paramCase{
		{value: "0", message: "from_ledger " + sharedErr.Error()},
		{value: "-1", message: "from_ledger " + sharedErr.Error()},
		{value: "abc", message: "from_ledger " + sharedErr.Error()},
		{value: "1.5", message: "from_ledger " + sharedErr.Error()},
		// Out of int64 range is the same class of input error, not a 500.
		{value: "9223372036854775808", message: "from_ledger " + sharedErr.Error()},
		{value: "+7", message: "from_ledger " + sharedErr.Error()},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("from_ledger=%q", tc.value), func(t *testing.T) {
			assertSharedRejection(t, "from_ledger", routes, tc)
		})
	}

	t.Run("to_ledger", func(t *testing.T) {
		assertSharedRejection(t, "to_ledger", routesFor(t, "to_ledger"),
			paramCase{value: "abc", message: "to_ledger must be a positive integer"})
	})

	t.Run("an inverted range is refused with both values named", func(t *testing.T) {
		// The range check lives in the shared builder, so the two endpoints
		// that take a range have to word it identically.
		for _, name := range []string{"GET /events", "GET /events/count"} {
			route := routeRegistry[name]
			status, message := requestURL(t, route, route.target+"from_ledger=900&to_ledger=1")
			require.Equalf(t, http.StatusBadRequest, status, "%s accepted an inverted ledger range", route.name)
			assert.Equalf(t, "from_ledger 900 is after to_ledger 1", message, "%s", route.name)
		}
	})
}

func TestTimeParametersShareOneParser(t *testing.T) {
	routes := routesFor(t, "from_time")
	tests := []paramCase{
		{value: "2026-07-21", message: "from_time must be an RFC 3339 timestamp"},
		{value: "yesterday", message: "from_time must be an RFC 3339 timestamp"},
		// The shared parser accepts second precision only, and says so.
		{value: "2026-07-21T00:00:00.500Z", message: "from_time sub-second precision is not supported"},
		{value: "2026-07-21T00:00:00", message: "from_time must be an RFC 3339 timestamp"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("from_time=%q", tc.value), func(t *testing.T) {
			assertSharedRejection(t, "from_time", routes, tc)
		})
	}

	t.Run("to_time", func(t *testing.T) {
		assertSharedRejection(t, "to_time", routesFor(t, "to_time"),
			paramCase{value: "nope", message: "to_time must be an RFC 3339 timestamp"})
	})

	t.Run("an inverted time range names both bounds", func(t *testing.T) {
		events := routeRegistry["GET /events"]
		status, message := requestURL(t, events,
			events.target+"from_time=2026-07-22T00:00:00Z&to_time=2026-07-21T00:00:00Z")
		require.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, message, "is after to_time")
	})
}

func TestEnumParametersShareOneParser(t *testing.T) {
	t.Run("type", func(t *testing.T) {
		// ParseTypes names the offending value inside its own message, so the
		// caller adds nothing — unlike the ledger and time parsers, which
		// leave the name out for the caller to prefix.
		_, sharedErr := queries.ParseTypes("bogus")
		require.Error(t, sharedErr)
		routes := routesFor(t, "type")
		tests := []paramCase{
			{value: "bogus", message: sharedErr.Error()},
			// A partly-valid list is still a rejection, and names the offender.
			{value: "contract,bogus", message: `invalid type "bogus"`},
			{value: "Contract", message: `invalid type "Contract"`},
		}
		for _, tc := range tests {
			tc := tc
			t.Run(fmt.Sprintf("type=%q", tc.value), func(t *testing.T) {
				assertSharedRejection(t, "type", routes, tc)
			})
		}
	})

	t.Run("order", func(t *testing.T) {
		assertSharedRejection(t, "order", routesFor(t, "order"),
			paramCase{value: "sideways", message: `invalid order "sideways" (want asc or desc)`})
	})

	t.Run("order_by", func(t *testing.T) {
		// The shared builder enumerates the accepted values in its message, so
		// an endpoint with its own list would be a second source of truth.
		want := fmt.Sprintf("invalid order_by %q", "recency")
		_, sharedErr := queries.BuildEventFilter(queries.EventFilterArgs{OrderBy: "recency"})
		require.Error(t, sharedErr)
		assert.Contains(t, sharedErr.Error(), want)
		assertSharedRejection(t, "order_by", routesFor(t, "order_by"), paramCase{value: "recency", message: want})
	})
}

func TestJSONAndCursorParametersShareOneParser(t *testing.T) {
	t.Run("topic", func(t *testing.T) {
		// ParseTopic auto-quotes a bare word but refuses something that looks
		// like JSON and is not, so the two classes are worth pinning apart.
		// The topic family prefixes with "name: ", where the ledger and time
		// families use "name "; both are historical, and what matters here is
		// that every endpoint in a family words it the same way.
		_, sharedErr := queries.ParseTopic("{")
		require.Error(t, sharedErr)
		assertSharedRejection(t, "topic", routesFor(t, "topic"),
			paramCase{value: "{", message: "topic: " + sharedErr.Error()})

		t.Run("a bare word is accepted as a quoted string", func(t *testing.T) {
			status, message := requestParam(t,
				paramEndpoint{name: "GET /events", target: "/events?topic=%s"}, "transfer")
			assert.Equalf(t, http.StatusOK, status, "GET /events?topic=transfer was refused: %s", message)
		})
	})

	t.Run("topic0 keeps its own name in the message", func(t *testing.T) {
		_, sharedErr := queries.ParseTopic("[")
		require.Error(t, sharedErr)
		assertSharedRejection(t, "topic0", routesFor(t, "topic0"),
			paramCase{value: "[", message: "topic0: " + sharedErr.Error()})
	})

	t.Run("topic_contains requires JSON without auto-quoting", func(t *testing.T) {
		assertSharedRejection(t, "topic_contains", routesFor(t, "topic_contains"),
			paramCase{value: "transfer", message: "topic_contains must be valid JSON"})
	})

	t.Run("topic and topic0 cannot be combined", func(t *testing.T) {
		events := routeRegistry["GET /events"]
		status, message := requestURL(t, events, events.target+`topic=transfer&topic0="x"`)
		require.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, message, "topic and topic0..topic3 filters cannot be combined")
	})

	t.Run("cursor", func(t *testing.T) {
		// The cursor alphabet is enforced by config.ValidCursor, and the same
		// wording appears on every paginated surface.
		assertSharedRejection(t, "cursor", routesFor(t, "cursor"),
			paramCase{value: "not-a-cursor!!", message: "invalid cursor"})
	})
}

// limitBoundExceptions names the endpoints whose page-size bound is not the
// configured maxLimit, and why. Each is a decision rather than drift: the
// contract listing is expensive enough to carry its own cap, which
// maxContractsListLimit documents.
var limitBoundExceptions = map[string]int{
	"GET /contracts": maxContractsListLimit,
}

func TestLimitBoundComesFromConfiguration(t *testing.T) {
	// The point of a configured bound is that no call site restates it, so
	// this moves the bound and checks that every endpoint follows it. A
	// handler with a literal in its message fails the moment the default
	// changes — which is exactly how the 500-vs-configured divergence started.
	const configured = 137
	SetMaxLimit(configured)
	t.Cleanup(func() { SetMaxLimit(store.MaxQueryLimit) })

	// The shared layer caps at the store's own clamp, so a configured bound
	// above it would be silently truncated rather than refused. Pinning the
	// relationship here is what keeps the two from drifting.
	require.LessOrEqual(t, maxLimit, queries.MaxPageSize,
		"maxLimit must stay at or below the shared layer's bound or ?limit= passes here and fails there")

	routes := routesFor(t, "limit")

	for _, route := range routes {
		route := route
		t.Run(route.name, func(t *testing.T) {
			bound := configured
			if exception, ok := limitBoundExceptions[route.name]; ok {
				bound = exception
			}
			// One at the bound proves it is inclusive; one above proves the
			// message reports the configured number rather than a literal.
			status, message := requestParam(t, route, fmt.Sprint(bound))
			assert.Equalf(t, http.StatusOK, status, "%s refused ?limit=%d: %s", route.name, bound, message)

			status, message = requestParam(t, route, fmt.Sprint(bound+1))
			require.Equalf(t, http.StatusBadRequest, status, "%s accepted ?limit=%d above its bound", route.name, bound+1)
			assert.Containsf(t, message, fmt.Sprintf("limit must be an integer in [1,%d]", bound),
				"%s reports a bound other than its configured one — a hardcoded cap at the call site: %q",
				route.name, message)
		})
	}

	t.Run("recent follows the same bound as limit", func(t *testing.T) {
		routes := routesFor(t, "recent")
		require.Len(t, routes, 1, "recent is a REST-only shorthand on one endpoint")
		recent := routes[0]

		status, message := requestParam(t, recent, fmt.Sprint(configured+1))
		require.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, message, fmt.Sprintf("recent must be a positive integer in [1,%d]", configured))

		status, message = requestParam(t, recent, fmt.Sprint(configured))
		assert.Equalf(t, http.StatusOK, status, "?recent= at the bound must pass: %s", message)
	})

	t.Run("the contract listing cap is a constant, not the configured bound", func(t *testing.T) {
		// Known gap, pinned rather than fixed here because #712's scope is the
		// test surface: GET /contracts validates ?limit= against
		// maxContractsListLimit alone, so lowering API_MAX_LIMIT does not
		// lower it. That is the same divergence the issue describes — one
		// endpoint honouring the configuration, another honouring a literal —
		// and it is reported in the PR rather than silently rewritten.
		//
		// If the follow-up fix makes /contracts take min(maxLimit,
		// maxContractsListLimit), this subtest and limitBoundExceptions are the
		// two things to update.
		SetMaxLimit(20)
		contracts := routeFor(t, routes, "GET /contracts")
		status, message := requestParam(t, contracts, "25")
		require.Equalf(t, http.StatusOK, status,
			"/contracts no longer ignores maxLimit: %s — fold limitBoundExceptions into the main table", message)

		status, message = requestParam(t, contracts, "201")
		require.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, message, fmt.Sprintf("limit must be an integer in [1,%d]", maxContractsListLimit))
	})
}

func TestHandlersRelaySharedMessagesRatherThanRestatingThem(t *testing.T) {
	// A parameter message is the shared parser's text with the parameter name
	// attached, and nothing else. This compares each endpoint's body against
	// the parser's live output rather than a transcription of it, so rewording
	// a parser cannot leave a handler asserting the old sentence.
	//
	// How the name gets attached is a per-family convention, and both are
	// deliberate: the ledger/time parsers return an unprefixed sentence that
	// the caller labels with a trailing space ("from_ledger must be …", the
	// shape the REST API has always sent), while the topic family labels with
	// a colon ("topic: topic must be valid JSON") so the position that failed
	// is legible in front of a message written about topics generally.
	cases := []struct {
		param  string
		label  string // what the handler puts in front of the shared text
		value  string
		shared func(string) error
	}{
		{"from_ledger", "from_ledger ", "0", func(raw string) error { _, err := queries.ParseLedgerParam(raw); return err }},
		{"to_ledger", "to_ledger ", "-3", func(raw string) error { _, err := queries.ParseLedgerParam(raw); return err }},
		{"from_time", "from_time ", "yesterday", func(raw string) error { _, err := queries.ParseTimeParam(raw); return err }},
		{"to_time", "to_time ", "yesterday", func(raw string) error { _, err := queries.ParseTimeParam(raw); return err }},
		{"topic", "topic: ", "{", func(raw string) error { _, err := queries.ParseTopic(raw); return err }},
		{"topic0", "topic0: ", "{", func(raw string) error { _, err := queries.ParseTopic(raw); return err }},
		// topic_contains requires JSON rather than auto-quoting, so its parser
		// already names the parameter and the caller adds nothing.
		{"topic_contains", "", "transfer", func(raw string) error { _, err := queries.ParseTopicContains(raw); return err }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.param, func(t *testing.T) {
			err := tc.shared(tc.value)
			require.Error(t, err, "the shared parser must reject the fixture input")

			status, message := requestParam(t,
				paramEndpoint{name: "GET /events", target: "/events?" + tc.param + "=%s"}, tc.value)
			require.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.label+err.Error(), message,
				"the handler must relay the shared parser's text verbatim behind its label")
		})
	}
}
