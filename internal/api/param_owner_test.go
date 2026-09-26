package api

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cross-endpoint tests in param_validation_test.go can only compare the
// parameters they know about. This file closes the other half of the issue's
// requirement — that no handler parses a request parameter inline when the
// shared layer already has a parser for it — by enumerating every
// query-parameter read in this package's non-test sources and requiring each
// name to appear in exactly one of the two tables below.
//
// The check is deliberately about *where a value is converted*, not where it is
// read: filterFromQuery has to call q.Get("limit") to tell an absent parameter
// from an explicit ?limit=0, and that is not duplication. What it must not do
// is decide on its own what a valid limit looks like, because that is how
// `limit` came to be capped at a hardcoded 500 in some handlers while others
// honoured API_MAX_LIMIT.

// parameterProvenance records who converts one request parameter's text into a
// typed value, and why that is the right place for it.
type parameterProvenance struct {
	// parser is the function that rejects an invalid value. Naming it is the
	// point of the table: a parameter whose value is checked nowhere is a
	// parameter that silently matches nothing.
	parser string
	// note explains the decision a reviewer would otherwise have to re-derive
	// from the call sites.
	note string
}

// sharedParsedParameters are the parameters whose conversion lives in
// internal/api/queries (or, for the cursor alphabet, internal/config) so the
// REST handlers and the GraphQL resolvers validate identically.
var sharedParsedParameters = map[string]parameterProvenance{
	"from_ledger": {parser: "queries.ParseLedgerParam",
		note: "the shared layer; prefixed with the parameter name by the handler"},
	"to_ledger": {parser: "queries.ParseLedgerParam",
		note: "the shared layer; prefixed with the parameter name by the handler"},
	"from_time": {parser: "queries.ParseTimeParam",
		note: "the shared layer; prefixed with the parameter name by the handler"},
	"to_time": {parser: "queries.ParseTimeParam",
		note: "the shared layer; prefixed with the parameter name by the handler"},
	"type":               {parser: "queries.ParseTypes", note: "the shared layer, including the comma-separated form"},
	"topic":              {parser: "queries.ParseTopic", note: "the shared layer, which auto-quotes a bare word"},
	"topic0":             {parser: "queries.ParseTopic", note: "same parser as topic, one call per position"},
	"topic1":             {parser: "queries.ParseTopic", note: "same parser as topic, one call per position"},
	"topic2":             {parser: "queries.ParseTopic", note: "same parser as topic, one call per position"},
	"topic3":             {parser: "queries.ParseTopic", note: "same parser as topic, one call per position"},
	"topic_contains":     {parser: "queries.ParseTopicContains", note: "the shared layer; no auto-quoting"},
	"contract_id":        {parser: "config.ValidContractID", note: "split on commas in filterFromQuery, then shape-checked per element"},
	"order":              {parser: "queries.BuildEventFilter", note: "the enum is the shared builder's"},
	"order_by":           {parser: "store.ValidOrderBy", note: "called by the shared builder"},
	"cursor":             {parser: "config.ValidCursor", note: "the alphabet check, shared with pagination encoding"},
	"tx_index":           {parser: "queries.BuildEventFilter", note: "converted by the handler, non-negativity by the builder"},
	"op_index":           {parser: "queries.BuildEventFilter", note: "converted by the handler, non-negativity by the builder"},
	"in_successful_call": {parser: "queries.BuildEventFilter", note: "tri-state: the handler maps true/false, absence means unconstrained"},
	"has_value":          {parser: "queries.BuildEventFilter", note: "tri-state, same shape as in_successful_call"},
	"limit": {parser: "queries.BuildEventFilter",
		note: "the REST handlers reject an explicit out-of-range value so ?limit=0 stays a 400 rather than meaning 'default'"},
}

// handlerLocalParameters are the parameters deliberately parsed at the call
// site. Each entry states what validates the value, so an unvalidated one is
// visible in the table rather than buried in a handler.
var handlerLocalParameters = map[string]parameterProvenance{
	"envelope": {parser: "none",
		note: "presence flag: `== \"true\"`, so an unrecognised value reads as false"},
	"include_xdr": {parser: "none",
		note: "presence flag, as envelope"},
	"pretty": {parser: "none",
		note: "response-encoding flag, not a filter"},
	"stream": {parser: "none",
		note: "selects the streaming write path on one endpoint"},
	"confirm": {parser: "none",
		note: "a destructive write refuses to run unless this is exactly true"},
	"fields": {parser: "parseFields",
		note: "an allowlist projection of response keys; unknown names are a 400"},
	"format": {parser: "parseExportFormat", note: "csv|ndjson, rejected otherwise, on the two export endpoints"},
	"bucket": {parser: "time.ParseDuration",
		note: "aggregate bucket: 'ledger' or a Go duration, checked in the handler"},
	"decoded": {parser: "decodeModeFromQuery",
		note: "tri-state rendering switch; an unrecognised value keeps the default rendering"},
	"recent": {parser: "filterFromQuery",
		note: "REST-only shorthand for order=desc&limit=N; the shared builder has no shorthand mode"},
	"sort": {parser: "store.ValidContractsSortKey",
		note: "the contract listing's own enum, separate from order_by"},
	"days": {parser: "strconv.Atoi in writeUsage",
		note: "bounded by maxUsageDays, which is a constant rather than configuration"},
	"before_ledger": {parser: "strconv.ParseInt in handleDeleteEvents",
		note: "duplicates queries.ParseLedgerParam's rule, with its own wording"},
	"contract_id_prefix": {parser: "none",
		note: "passed to the store as a literal prefix; the shared builder only checks it does not collide with contract_id"},
	"tx_hash": {parser: "none",
		note: "passed through to the store unvalidated, unlike contract_id"},
}

// queryParametersRead walks the package's non-test sources and returns every
// literal key read off a parsed URL query: the `r.URL.Query().Get("k")` and
// `q.Get("k")` forms, where q is a url.Values held in a local named q.
func queryParametersRead(t *testing.T) map[string][]string {
	t.Helper()
	// The test binary runs with the package directory as its working
	// directory, so this reads exactly the files that declare the handlers.
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files, "no sources to inventory")

	found := map[string][]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoErrorf(t, err, "inventory cannot parse %s", name)

		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Get" {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			key, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			// Header reads share the .Get("literal") shape; only a receiver
			// that is a url.Values counts here, which means the text either
			// ends in Query() or is the local the handlers assign it to.
			var recv strings.Builder
			if err := printer.Fprint(&recv, fset, sel.X); err != nil {
				return true
			}
			text := recv.String()
			if !strings.HasSuffix(text, "Query()") && text != "q" && text != "query" {
				return true
			}
			found[key] = append(found[key], name)
			return true
		})
	}
	return found
}

func TestEveryRequestParameterHasOneOwner(t *testing.T) {
	found := queryParametersRead(t)
	require.NotEmpty(t, found, "the inventory found no query parameters, so it proves nothing")

	// Every parameter this package reads must be classified. The failure names
	// the file to look in, which is the point: an unclassified parameter is one
	// nobody has decided how it is validated.
	for key, files := range found {
		key, files := key, files
		t.Run(key, func(t *testing.T) {
			shared, isShared := sharedParsedParameters[key]
			local, isLocal := handlerLocalParameters[key]
			switch {
			case isShared && isLocal:
				t.Fatalf("%s is in both tables: %s and %s", key, shared.parser, local.parser)
			case isShared:
				assert.NotEmptyf(t, shared.note, "%s has no reason recorded for being shared", key)
				assert.NotEmptyf(t, shared.parser, "%s names no parser", key)
			case isLocal:
				assert.NotEmptyf(t, local.note, "%s is parsed at the call site with no reason recorded", key)
				assert.NotEmptyf(t, local.parser, "%s names no parser", key)
			default:
				t.Fatalf("%s is read in %s but belongs to neither table — decide who validates it and record it",
					key, strings.Join(files, ", "))
			}
		})
	}

	// The reverse direction keeps the tables honest about the code that exists.
	// A parameter that no handler reads any more is documentation for a route
	// nobody serves, and it hides the fact that the inventory shrank.
	for key := range sharedParsedParameters {
		key := key
		t.Run("claimed/"+key, func(t *testing.T) {
			assert.Containsf(t, found, key,
				"sharedParsedParameters still lists %q, which no handler reads", key)
		})
	}
	for key := range handlerLocalParameters {
		key := key
		t.Run("claimed/"+key, func(t *testing.T) {
			assert.Containsf(t, found, key,
				"handlerLocalParameters still lists %q, which no handler reads", key)
		})
	}
}

func TestSharedParametersAreTheOnesTheCrossEndpointTestsDrive(t *testing.T) {
	// param_validation_test.go compares behaviour across endpoints for a subset
	// of the shared parameters — the ones several endpoints accept. This keeps
	// that subset from drifting away from the inventory without demanding that
	// single-endpoint parameters (contract_id_prefix's siblings, the topic
	// positions) be compared against nothing.
	for param := range paramRoutes {
		param := param
		if param == "recent" {
			continue // handler-local by design, and documented as such
		}
		t.Run(param, func(t *testing.T) {
			_, shared := sharedParsedParameters[param]
			assert.Truef(t, shared,
				"%s is compared across endpoints but is not in sharedParsedParameters — is it still shared?", param)
		})
	}
}
