package metrics

// This file is the catalogue of every metric SoroTrail exports (#717):
// one entry per metric family, naming its type, its labels and their
// value sets, the help text a scraper sees, and what it means to an
// operator.
//
// The catalogue is checked against the code rather than maintained
// beside it, in both of the places a metric can drift:
//
//   - extractSourceMetrics parses metrics.go and reports every
//     prometheus.New* call with its name, help, type and labels exactly
//     as declared; TestMetricCatalogueMatchesTheSource requires the two
//     to agree both ways, so renaming or removing a declaration fails
//     until the catalogue is updated, and a stale catalogue entry fails
//     too.
//   - TestEveryCataloguedMetricIsExported touches each vector with its
//     declared label values, scrapes the global registry and the
//     per-server registry, and requires every catalogue entry to appear
//     with the declared type and help text — and every exported family
//     to be catalogued. A metric dropped from init()'s registration is
//     a family the scrape no longer serves, which this catches.
//
// The naming and label rules live in TestMetricNamesFollowPrometheusConventions
// and TestLabelNamesAreBoundedAndCarryNoIDs, and the same data renders
// docs/metrics.md (TestMetricsDocumentation), so the code, the docs and
// what /metrics actually serves cannot drift apart without a test
// noticing.

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scopes: where a metric family is registered. "global" families live on
// the default Prometheus registry, so every process that imports the
// package exports them; "server" families live on an HTTPMetrics'
// private registry and are served only through its merged handler.
const (
	scopeGlobal = "global"
	scopeServer = "server"
)

// metricLabel is one label of a metric family.
type metricLabel struct {
	// Name is the label name as served in the exposition format.
	Name string

	// Values is the closed set of values the label takes, as served.
	// Open-ended but bounded sets (a hostname per configured provider,
	// a chi route pattern) are described in prose instead of a pipe
	// list; they must still be values no client controls the shape of.
	Values string
}

// metricSpec is one catalogued metric family.
type metricSpec struct {
	// Name is the family name as served, e.g.
	// "sorotrail_events_ingested_total".
	Name string

	// Scope is scopeGlobal or scopeServer (see above).
	Scope string

	// Kind is the exposition type: counter, gauge or histogram.
	Kind string

	// Labels are the family's labels in declaration order; empty for
	// unlabelled metrics.
	Labels []metricLabel

	// Help is the exposition HELP text, byte-identical to what
	// /metrics serves.
	Help string

	// Meaning is the operator-facing description rendered into
	// docs/metrics.md: what the number means and when to look at it.
	Meaning string

	// touch, for labelled metrics, materialises one series (it is the
	// WithLabelValues call the scrape tests make before scraping).
	touch func()
}

// metricCatalogue is the catalogue itself, grouped by the order the
// sections render in docs/metrics.md.
var metricCatalogue = []metricSpec{
	// --- Ingestion ---
	{
		Name: "sorotrail_events_ingested_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total number of events ingested (including duplicates resolved by idempotent upsert).",
		Meaning: "Throughput of the ingestion pipeline. Duplicates resolved by the idempotent upsert still count, so this measures pipeline work rather than net-new rows; subtract the store's own counts when net-new is what matters. Also read by the /stats endpoint as the in-process ingest counter.",
	},
	{
		Name: "sorotrail_ingest_errors_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total number of ingestion passes that failed with an error.",
		Meaning: "Ingestion passes that ended in an error — an RPC failure, a decode failure or a database write failure. Retries are not counted, only passes that ended badly. Registered but not yet incremented by any call site, so it reads 0; it is pinned here so the wiring lands against a documented contract.",
	},
	{
		Name: "sorotrail_ingestion_lag_ledgers", Scope: scopeGlobal, Kind: "gauge",
		Help:    "Number of ledgers the indexer is behind the chain head.",
		Meaning: "Chain head minus last ingested ledger, updated after every ingestion pass that knows the chain head. The direct alert for a stalled pipeline: anything above zero for a sustained window means ingestion is falling behind.",
	},

	// --- Spec cache ---
	{
		Name: "sorotrail_spec_invalidations_received_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total number of spec cache invalidations received from other replicas.",
		Meaning: "Spec-cache invalidations this process applied because another replica said the contract's decoded shape changed. A non-zero value proves the cross-replica fan-out is working; a replica that never invalidates anything while its peers change specs is serving stale decodes. Stays at zero on a single-replica deployment, where there is nobody to hear from.",
	},
	{
		Name: "sorotrail_http_limit_rejections_total", Scope: scopeGlobal, Kind: "counter",
		Labels: []metricLabel{
			{Name: "reason", Values: "request_body | request_decompressed | response_body"},
		},
		Help:    "Total number of HTTP requests or responses rejected for exceeding a configured size limit, labelled by reason (request_body | request_decompressed | response_body).",
		Meaning: "Requests or responses refused with 413 Request Entity Too Large because they exceeded a configured size bound: request_body for raw request bytes over the endpoint or global HTTP_REQUEST_BODY_LIMIT, request_decompressed for gzip/deflate payloads whose decompressed stream expanded past the limit, and response_body for non-streaming JSON responses over HTTP_RESPONSE_BODY_LIMIT or the caller's X-Max-Response-Bytes bound.",
		touch:   func() { HTTPLimitRejectionsTotal.WithLabelValues(LimitReasonRequestBody) },
	},

	// --- RPC ---
	{
		Name: "sorotrail_rpc_call_duration_seconds", Scope: scopeGlobal, Kind: "histogram",
		Labels: []metricLabel{
			{Name: "method", Values: "getEvents | getLatestLedger | getHealth | getLedgerEntries | simulateTransaction"},
			{Name: "outcome", Values: "success | error"},
		},
		Help:    "RPC call latency in seconds (HTTP round trip + body read + parse), labelled by method and outcome.",
		Meaning: "Latency of a single JSON-RPC call. The histogram's _count for a (method, outcome) pair is that pair's call total, so one metric answers both \"how slow\" and \"how many, and how many failed\" — when ingestion falls behind, this says whether the upstream is slow or failing.",
		touch:   func() { RPCCallLatency.WithLabelValues("getEvents", "success") },
	},
	{
		Name: "sorotrail_rpc_retries_total", Scope: scopeGlobal, Kind: "counter",
		Labels: []metricLabel{
			{Name: "method", Values: "getEvents | getLatestLedger | getHealth | getLedgerEntries | simulateTransaction"},
			{Name: "reason", Values: "backoff | retry_after"},
		},
		Help:    "Total number of RPC retry attempts, labelled by method and reason (backoff | retry_after).",
		Meaning: "Every retry attempt after the first. reason says where the wait came from: the computed exponential backoff, or a provider-supplied 429 Retry-After hint — a lot of retry_after means the provider is explicitly throttling us.",
		touch:   func() { RPCRetriesTotal.WithLabelValues("getEvents", "backoff") },
	},
	{
		Name: "sorotrail_rpc_backoff_seconds_total", Scope: scopeGlobal, Kind: "counter",
		Labels: []metricLabel{
			{Name: "method", Values: "getEvents | getLatestLedger | getHealth | getLedgerEntries | simulateTransaction"},
		},
		Help:    "Total seconds spent sleeping between RPC retries, labelled by method.",
		Meaning: "Cumulative wall-clock time lost to retry waits. Priced in seconds rather than counts so a long tail is visible: sustained growth means the provider (or our own rate limiter) is throttling us.",
		touch:   func() { RPCBackoffSeconds.WithLabelValues("getEvents") },
	},
	{
		Name: "sorotrail_rpc_failovers_total", Scope: scopeGlobal, Kind: "counter",
		Labels: []metricLabel{
			{Name: "reason", Values: "switch | reanchor"},
		},
		Help:    "Total number of RPC provider failover events, labelled by reason (switch | reanchor).",
		Meaning: "Provider failovers in the multi-provider client: \"switch\" when traffic moved to a different provider, \"reanchor\" when a cursor request hit the failover-reanchor path and the caller had to discard its cursor (the expensive kind — reanchors mean lost progress, not just a new connection).",
		touch:   func() { RPCFailoversTotal.WithLabelValues("switch") },
	},
	{
		Name: "sorotrail_rpc_circuit_breaker_state", Scope: scopeGlobal, Kind: "gauge",
		Labels: []metricLabel{
			{Name: "state", Values: "closed | open | half-open"},
		},
		Help:    "Circuit breaker state as a 0/1 gauge per state (closed | open | half-open).",
		Meaning: "Exactly one state series is 1 at a time. Alert on {state=\"open\"} == 1: the breaker is refusing calls outright, which is why ingestion has stopped rather than merely slowed.",
		touch:   func() { RPCCircuitBreakerState.WithLabelValues("closed") },
	},
	{
		Name: "sorotrail_rpc_provider_state", Scope: scopeGlobal, Kind: "gauge",
		Labels: []metricLabel{
			{Name: "provider", Values: "the hostname of one RPC_URLS entry — scheme, credentials, path and query are stripped, so nothing client-identifying is exported"},
			{Name: "state", Values: "active | degraded | down"},
		},
		Help:    "Per-provider failover health state as a 0/1 gauge per state (active | degraded | down).",
		Meaning: "One 0/1 series per (provider, state). Only present in multi-provider mode (RPC_URLS). Shows which provider traffic is on and which have failed, without reading logs.",
		touch:   func() { RPCProviderState.WithLabelValues("localhost", "active") },
	},

	// --- Storage ---
	{
		Name: "sorotrail_db_write_duration_seconds", Scope: scopeGlobal, Kind: "histogram",
		Help:    "Database write latency in seconds (batch insert/upsert/repair).",
		Meaning: "Write path only: batch upserts, replace-in-range repairs. Compared against the ingester's write-latency budget it tells you whether backpressure is justified by the database or by our own accounting.",
	},
	{
		Name: "sorotrail_db_query_duration_seconds", Scope: scopeGlobal, Kind: "histogram",
		Labels: []metricLabel{
			{Name: "operation", Values: "store.<Store method> — a fixed table of method names (store.UpsertEvents, store.QueryEvents, …), one per guarded Store operation"},
		},
		Help:    "Store operation duration in seconds, labelled by operation.",
		Meaning: "Every guarded Store operation, reads and writes alike, so a slow database is attributable to a specific operation rather than showing up only as a slow API.",
		touch:   func() { DBQueryDuration.WithLabelValues("store.QueryEvents") },
	},
	{
		Name: "sorotrail_db_operations_total", Scope: scopeGlobal, Kind: "counter",
		Labels: []metricLabel{
			{Name: "operation", Values: "store.<Store method> — the same fixed table as sorotrail_db_query_duration_seconds"},
			{Name: "outcome", Values: "success | error"},
		},
		Help:    "Total store operations, labelled by operation and outcome (success | error).",
		Meaning: "Count of guarded Store calls. Errors are counted separately from successes so a failing backend is a direct query rather than something inferred by subtracting histograms.",
		touch:   func() { DBOperationsTotal.WithLabelValues("store.QueryEvents", "success") },
	},

	// --- Batching ---
	{
		Name: "sorotrail_event_batch_writes_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total number of event store writes (UpsertEvents calls) issued by the ingester.",
		Meaning: "One per non-empty page with batching disabled; with batching enabled it is how many chunks the adaptive controller split pages into. Rising against a flat events-ingested counter means writes are being shaved smaller.",
	},
	{
		Name: "sorotrail_event_batch_size", Scope: scopeGlobal, Kind: "gauge",
		Help:    "Current adaptive event batch size used for each store write.",
		Meaning: "The size the batch controller is currently feeding the store. It sits at the configured maximum while writes are inside the latency budget and steps down under pressure, so a falling gauge is the earliest signal that the store is starting to strain.",
	},
	{
		Name: "sorotrail_event_backpressure_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total number of throttle sleeps inserted between event batch writes.",
		Meaning: "How many times the ingester deliberately slept between writes because the store fell behind the latency budget. Non-zero proves backpressure is firing; paired with sorotrail_event_backpressure_seconds_total it separates brief hesitations from sustained throttling.",
	},
	{
		Name: "sorotrail_event_backpressure_seconds_total", Scope: scopeGlobal, Kind: "counter",
		Help:    "Total seconds spent throttling event writes under backpressure.",
		Meaning: "Cumulative sleep time under backpressure — the surge priced in seconds. A long tail here means the database was slow for a sustained stretch, not just a burst.",
	},

	// --- HTTP (per-server) ---
	{
		Name: "http_request_duration_seconds", Scope: scopeServer, Kind: "histogram",
		Labels: []metricLabel{
			{Name: "route", Values: "the matched chi route pattern (e.g. /events/{id}), or \"unmatched\" for a path no route registered — never the raw path, so probes cannot grow cardinality"},
			{Name: "method", Values: "the HTTP method (GET | POST | PUT | PATCH | DELETE)"},
			{Name: "status", Values: "the response status code as a decimal string (200 | 404 | 500 | …)"},
		},
		Help:    "HTTP request duration in seconds, labeled by route, method, and status code.",
		Meaning: "Per-request latency for the whole API. One instance per metrics.HTTPMetrics (i.e. per server), merged with the global registry on scrape, so tests and parallel servers do not collide on the default registry.",
		touch:   nil, // materialised by driving a request through the middleware, below
	},
}

// promMetricNameRe is the Prometheus metric-name grammar: lower-case
// words joined by single underscores.
var promMetricNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// promLabelNameRe is the same grammar for label names.
var promLabelNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// idBearingLabelRe matches label names that would carry entity identity
// into the time series — the thing Prometheus best-practice forbids and
// this codebase must never grow. Structural labels that merely *look*
// open-ended (route, provider) are allow-listed separately because their
// value shape is fixed by the server, not by the client.
var idBearingLabelRe = regexp.MustCompile(`(?i)(^|_)(id|ids|uuid|hash|digest|tx|transaction|address|account|email|token|secret|key|url|uri|path|query|ip|host|name)$`)

// allowedLabelNames is the closed set every label must belong to. A new
// label name must be added here deliberately, with its cardinality
// bounded by construction at the call site.
var allowedLabelNames = map[string]bool{
	"method":    true, // fixed enum: RPC methods, or HTTP verbs
	"outcome":   true, // success | error
	"reason":    true, // fixed enum per metric
	"state":     true, // fixed enum per metric
	"provider":  true, // one value per configured RPC URL, hostname only
	"operation": true, // one value per guarded Store method
	"route":     true, // matched chi route pattern, or "unmatched"
	"status":    true, // decimal status code of a real response
}

// catalogueByName indexes the catalogue for the reverse checks.
func catalogueByName() map[string]metricSpec {
	out := make(map[string]metricSpec, len(metricCatalogue))
	for _, e := range metricCatalogue {
		out[e.Name] = e
	}
	return out
}

// sourceMetric is one prometheus.New* call as written in metrics.go.
type sourceMetric struct {
	Kind   string
	Name   string
	Help   string
	Labels []string
}

// extractSourceMetrics parses metrics.go and returns one entry per
// prometheus.New* call, sorted. It reports the Opts literals exactly as
// declared, so a renamed metric, a edited help string or a changed
// label set all show up as a difference.
func extractSourceMetrics(t *testing.T) []sourceMetric {
	t.Helper()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "metrics.go", nil, 0)
	require.NoError(t, err, "parse metrics.go")

	kinds := map[string]string{
		"NewCounter":      "counter",
		"NewCounterVec":   "counter",
		"NewGauge":        "gauge",
		"NewGaugeVec":     "gauge",
		"NewHistogram":    "histogram",
		"NewHistogramVec": "histogram",
	}

	var out []sourceMetric
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "prometheus" {
			return true
		}
		kind, ok := kinds[sel.Sel.Name]
		if !ok {
			return true
		}

		sm := sourceMetric{Kind: kind}
		if len(call.Args) > 0 {
			if lit, ok := call.Args[0].(*ast.CompositeLit); ok {
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					key, ok := kv.Key.(*ast.Ident)
					if !ok {
						continue
					}
					switch key.Name {
					case "Name":
						sm.Name = unquote(t, kv.Value)
					case "Help":
						sm.Help = unquote(t, kv.Value)
					}
				}
			}
		}
		if len(call.Args) > 1 {
			if lit, ok := call.Args[1].(*ast.CompositeLit); ok {
				for _, elt := range lit.Elts {
					sm.Labels = append(sm.Labels, unquote(t, elt))
				}
			}
		}
		if sm.Name != "" {
			out = append(out, sm)
		}
		return true
	})

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// unquote renders a string literal's value, failing loudly rather than
// silently skipping a literal it cannot read.
func unquote(t *testing.T, e ast.Expr) string {
	t.Helper()
	bl, ok := e.(*ast.BasicLit)
	require.Truef(t, ok, "expected a string literal, got %T", e)
	s, err := strconv.Unquote(bl.Value)
	require.NoErrorf(t, err, "unquote %s", bl.Value)
	return s
}

// TestMetricCatalogueMatchesTheSource is the guard the issue asks for:
// every metric the code declares is catalogued, and every catalogued
// metric still exists in the code — names, types, help text and label
// sets included.
func TestMetricCatalogueMatchesTheSource(t *testing.T) {
	source := extractSourceMetrics(t)

	declared := map[string]sourceMetric{}
	for _, s := range source {
		declared[s.Name] = s
	}

	catalogued := map[string]metricSpec{}
	for _, e := range metricCatalogue {
		catalogued[e.Name] = e
	}

	// Code → catalogue: a metric nobody has written down.
	for _, s := range source {
		e, ok := catalogued[s.Name]
		if assert.Truef(t, ok, "%s is declared in metrics.go but missing from metricCatalogue", s.Name) {
			assert.Equalf(t, s.Kind, e.Kind, "%s: type disagrees with the declaration", s.Name)
			assert.Equalf(t, s.Help, e.Help, "%s: Help disagrees with the declaration", s.Name)
			var want []string
			for _, l := range e.Labels {
				want = append(want, l.Name)
			}
			assert.Equalf(t, s.Labels, want, "%s: labels disagree with the declaration", s.Name)
		}
	}

	// Catalogue → code: a metric the code no longer declares.
	for name := range catalogued {
		_, ok := declared[name]
		assert.Truef(t, ok, "%s is catalogued but no longer declared in metrics.go — remove or fix the entry", name)
	}
}

// exposition is the HELP/TYPE metadata of one scraped family.
type exposition struct {
	Help string
	Kind string
}

// scrapeExposition scrapes a handler and returns its families' HELP and
// TYPE metadata by name.
func scrapeExposition(t *testing.T, h http.Handler) map[string]exposition {
	t.Helper()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, w.Code, "scrape body: %s", w.Body.String())

	out := map[string]exposition{}
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if !strings.HasPrefix(line, "# ") {
			continue
		}
		fields := strings.SplitN(strings.TrimPrefix(line, "# "), " ", 3)
		if len(fields) < 3 {
			continue
		}
		switch fields[0] {
		case "HELP":
			e := out[fields[1]]
			e.Help = fields[2]
			out[fields[1]] = e
		case "TYPE":
			e := out[fields[1]]
			e.Kind = fields[2]
			out[fields[1]] = e
		}
	}
	return out
}

// TestEveryCataloguedMetricIsExported checks what /metrics actually
// serves against the catalogue, both ways: every entry must come out of
// the registries with the declared type and help text, and every family
// the registries serve must be catalogued. Unobserved vectors are
// absent from the exposition, so each is touched with its declared
// values first — which doubles as a check that the documented enums are
// values the metric accepts.
func TestEveryCataloguedMetricIsExported(t *testing.T) {
	for _, e := range metricCatalogue {
		if e.touch != nil {
			e.touch()
		}
	}

	global := scrapeExposition(t, Handler())
	byName := catalogueByName()
	for _, e := range metricCatalogue {
		if e.Scope != scopeGlobal {
			continue
		}
		got, ok := global[e.Name]
		if !assert.Truef(t, ok, "%s is catalogued as global but /metrics does not serve it (registered?)", e.Name) {
			continue
		}
		assert.Equalf(t, e.Kind, got.Kind, "%s: /metrics serves a different type", e.Name)
		assert.Equalf(t, e.Help, got.Help, "%s: /metrics serves different help text", e.Name)
	}
	for name := range global {
		if isRuntimeCollector(name) {
			continue
		}
		_, ok := byName[name]
		assert.Truef(t, ok, "%s is exported at /metrics but missing from metricCatalogue", name)
	}
	_, serverOnGlobal := global["http_request_duration_seconds"]
	assert.False(t, serverOnGlobal,
		"the per-server histogram must not leak into the global registry")

	// The per-server registry: drive one request so the histogram has a
	// series, then scrape the merged handler and check the server-scoped
	// entry (the global families ride along and are checked above).
	m := New()
	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/events/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events/0000000000000-0000000001", nil))
	require.Equal(t, http.StatusOK, w.Code)

	server := scrapeExposition(t, m.Handler())
	for _, e := range metricCatalogue {
		if e.Scope != scopeServer {
			continue
		}
		got, ok := server[e.Name]
		if !assert.Truef(t, ok, "%s is catalogued as per-server but its registry does not serve it", e.Name) {
			continue
		}
		assert.Equalf(t, e.Kind, got.Kind, "%s: the per-server registry serves a different type", e.Name)
		assert.Equalf(t, e.Help, got.Help, "%s: the per-server registry serves different help text", e.Name)
	}
	// The merged handler serves the global families alongside its own, so
	// everything it serves must be catalogued at either scope.
	for name := range server {
		if isRuntimeCollector(name) {
			continue
		}
		_, ok := byName[name]
		assert.Truef(t, ok, "%s is served at /metrics but missing from metricCatalogue", name)
	}
}

// isRuntimeCollector names the Prometheus client library's own families:
// the Go runtime and process collectors DefaultGatherer serves since
// client_golang 1.20, and the promhttp handler's self-instrumentation.
// They ride along on every scrape but are the library's metrics, not
// SoroTrail's, so no catalogue claims them — they only stay out of the
// way of the reverse check.
func isRuntimeCollector(name string) bool {
	return strings.HasPrefix(name, "go_") ||
		strings.HasPrefix(name, "process_") ||
		strings.HasPrefix(name, "promhttp_")
}

// TestMetricNamesFollowPrometheusConventions pins the naming rules a
// PromQL user expects: the sorotrail_ prefix, lower-case snake_case,
// _total on counters, and the unit carried by durations.
func TestMetricNamesFollowPrometheusConventions(t *testing.T) {
	for _, e := range metricCatalogue {
		t.Run(e.Name, func(t *testing.T) {
			assert.Regexpf(t, promMetricNameRe, e.Name,
				"%s must be lower-case snake_case", e.Name)
			assert.NotContainsf(t, e.Name, "__", "%s must not double the underscore", e.Name)

			switch e.Scope {
			case scopeGlobal:
				assert.Truef(t, strings.HasPrefix(e.Name, "sorotrail_"),
					"%s is a global metric and must carry the sorotrail_ prefix", e.Name)
			case scopeServer:
				// http_request_duration_seconds predates the prefix and is
				// quoted in runbooks and dashboards; renaming it would break
				// them, so it is pinned by the catalogue instead.
				assert.Equal(t, "http_request_duration_seconds", e.Name,
					"the only permitted unprefixed metric is the per-server HTTP histogram")
			}

			switch e.Kind {
			case "counter":
				assert.Truef(t, strings.HasSuffix(e.Name, "_total"),
					"counter %s must end in _total", e.Name)
			case "histogram":
				assert.Truef(t, strings.HasSuffix(e.Name, "_seconds"),
					"histogram %s must carry its unit (_seconds)", e.Name)
			case "gauge":
				assert.Falsef(t, strings.HasSuffix(e.Name, "_total"),
					"gauge %s must not end in _total (that suffix is for counters)", e.Name)
			default:
				t.Fatalf("unknown kind %q", e.Kind)
			}
		})
	}
}

// TestLabelNamesAreBoundedAndCarryNoIDs pins the label discipline:
// every label comes from the allow-list, none carries entity identity,
// and every documented value set is non-empty.
func TestLabelNamesAreBoundedAndCarryNoIDs(t *testing.T) {
	for _, e := range metricCatalogue {
		for _, l := range e.Labels {
			where := e.Name + "{" + l.Name + "}"
			assert.Regexpf(t, promLabelNameRe, l.Name, "%s: label names are lower-case snake_case", where)
			assert.Truef(t, allowedLabelNames[l.Name],
				"%s: label %q is not in the allow-list — new label names must be added deliberately, with bounded cardinality", where, l.Name)
			assert.Falsef(t, idBearingLabelRe.MatchString(l.Name),
				"%s: label %q looks id-bearing; entity ids must never be labels", where, l.Name)
			assert.NotEmptyf(t, l.Values, "%s: the label's value set must be documented", where)
		}
	}
}

// TestHTTPMetricsCardinalityStaysBounded drives many distinct raw paths
// through the middleware and requires the histogram's route label to
// collapse them all into the fixed "unmatched" value — the failure mode
// this guards is a client probing random paths growing series without
// bound.
func TestHTTPMetricsCardinalityStaysBounded(t *testing.T) {
	m := New()

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/events/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	for i := 0; i < 100; i++ {
		matched := []string{
			fmt.Sprintf("/events/%d", i),          // matched route, distinct parameter
			fmt.Sprintf("/events/%d?cb=%d", i, i), // distinct query string too
		}
		for _, target := range matched {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
			require.Equal(t, http.StatusOK, w.Code)
		}
		// Unmatched on purpose: the 404 is the point, so the route label
		// has to collapse these into "unmatched".
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/probe-%d", i), nil))
		require.Equal(t, http.StatusNotFound, w.Code)
	}

	body := scrapeBody(t, m)
	routes := map[string]bool{}
	re := regexp.MustCompile(`http_request_duration_seconds(?:_sum|_count)?\{[^}]*route="([^"]+)"`)
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		routes[m[1]] = true
	}
	var seen []string
	for route := range routes {
		seen = append(seen, route)
	}
	sort.Strings(seen)
	assert.Equal(t, []string{"/events/{id}", "unmatched"}, seen,
		"300 distinct requests must collapse into the matched pattern and \"unmatched\" — nothing client-shaped may reach a label")
	assert.NotContains(t, body, "probe-",
		"raw unmatched paths must never leak into labels")
}

// updateMetricsDoc regenerates docs/metrics.md from the catalogue:
//
//	go test ./internal/metrics -run TestMetricsDocumentation -update-metrics-doc
var updateMetricsDoc = flag.Bool("update-metrics-doc", false, "rewrite docs/metrics.md from metricCatalogue")

// TestMetricsDocumentation keeps the human catalogue byte-identical to
// what the test data describes: render, then compare with
// docs/metrics.md.
func TestMetricsDocumentation(t *testing.T) {
	got := renderMetricsCatalogue()
	path := filepath.Join("..", "..", "docs", "metrics.md")
	if *updateMetricsDoc {
		require.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		t.Logf("updated %s", path)
		return
	}
	want, err := os.ReadFile(path)
	require.NoErrorf(t, err,
		"docs/metrics.md does not exist; generate it with:\n"+
			"\tgo test ./internal/metrics -run TestMetricsDocumentation -update-metrics-doc")
	assert.Equal(t, string(want), got,
		"docs/metrics.md is stale; regenerate with:\n"+
			"\tgo test ./internal/metrics -run TestMetricsDocumentation -update-metrics-doc")
}

// renderMetricsCatalogue produces docs/metrics.md: header, conventions,
// one table per group, then the label sets.
func renderMetricsCatalogue() string {
	var b strings.Builder

	b.WriteString(`# Metrics catalogue

Every Prometheus metric SoroTrail exports, in one place: its type, its
labels, and what the number means. Two registries serve them, both in a
single scrape of ` + "`GET /metrics`" + ` (` + "`ENABLE_METRICS=true`" + `):

- the **global registry** — every ` + "`sorotrail_*`" + ` counter, gauge and
  histogram, registered when ` + "`internal/metrics`" + ` is imported, so every
  process that links the package exports them;
- the **per-server registry** — ` + "`http_request_duration_seconds`" + `, one
  histogram per ` + "`metrics.HTTPMetrics`" + ` instance, so tests and parallel
  servers do not collide on the global one.

A scrape also carries the Prometheus client library's own runtime
families (` + "`go_*`" + `, ` + "`process_*`" + `, ` + "`promhttp_*`" + `) — the library's
instrumentation rather than SoroTrail's, listed here only so nobody
mistakes them for undocumented project metrics.

This file is generated from the catalogue in
` + "`internal/metrics/metrics_catalogue_test.go`" + `, which is checked against
` + "`metrics.go`" + `'s declarations and against what the registries actually
serve, so a renamed, removed or newly added metric fails the build until
it is documented. Regenerate it with:

    go test ./internal/metrics -run TestMetricsDocumentation -update-metrics-doc

## Conventions

- Every metric name is lower-case snake_case; global metrics carry the
  ` + "`sorotrail_`" + ` prefix (the sole exception, ` + "`http_request_duration_seconds`" + `,
  predates the prefix and is quoted in runbooks, so it is pinned by test
  rather than renamed).
- Counters end ` + "`_total`" + `; histograms carry their unit (` + "`_seconds`" + `);
  gauges are plain unit-suffixed names.
- Labels come from a closed set (see "Label sets") and no label ever
  carries entity identity: no event id, transaction hash, address,
  credential or raw path. ` + "`route`" + ` is the matched chi route pattern (or
  ` + "`unmatched`" + `), so path probes cannot grow cardinality; ` + "`provider`" + ` is a
  configured RPC URL's hostname with scheme, credentials, path and query
  stripped.
- Histograms use the Prometheus default buckets
  (` + "`DefBuckets`" + `, 0.005s … 10s), which suit RPC round trips and database
  writes; the ` + "`_count`" + ` and ` + "`_sum`" + ` series every histogram exports are
  how call totals and mean latency are read without a separate counter.

`)

	groups := []struct {
		title    string
		prefixes []string
	}{
		{"Ingestion", []string{"sorotrail_events_", "sorotrail_ingest"}},
		{"RPC", []string{"sorotrail_rpc_"}},
		{"Storage", []string{"sorotrail_db_"}},
		{"Batching", []string{"sorotrail_event_"}},
	}
	rendered := map[string]bool{}

	writeTable := func(entries []metricSpec) {
		b.WriteString("| Metric | Type | Labels | Help (as served) | Meaning |\n")
		b.WriteString("| --- | --- | --- | --- | --- |\n")
		for _, e := range entries {
			fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s |\n",
				e.Name, e.Kind, metricCell(labelCell(e)), metricCell(e.Help), metricCell(e.Meaning))
		}
		b.WriteString("\n")
	}

	for _, g := range groups {
		var entries []metricSpec
		for _, e := range metricCatalogue {
			if rendered[e.Name] || e.Scope != scopeGlobal {
				continue
			}
			matched := false
			for _, p := range g.prefixes {
				if strings.HasPrefix(e.Name, p) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			entries = append(entries, e)
			rendered[e.Name] = true
		}
		if len(entries) == 0 {
			continue
		}
		fmt.Fprintf(&b, "## %s metrics\n\n", g.title)
		writeTable(entries)
	}

	// Anything the prefix groups did not claim (ingest errors, lag).
	var rest []metricSpec
	for _, e := range metricCatalogue {
		if !rendered[e.Name] && e.Scope == scopeGlobal {
			rest = append(rest, e)
			rendered[e.Name] = true
		}
	}
	if len(rest) > 0 {
		b.WriteString("## Other pipeline metrics\n\n")
		writeTable(rest)
	}

	var server []metricSpec
	for _, e := range metricCatalogue {
		if e.Scope == scopeServer {
			server = append(server, e)
		}
	}
	b.WriteString("## HTTP metrics (per-server registry)\n\n")
	writeTable(server)

	b.WriteString("## Label sets\n\n")
	b.WriteString("Every label value is drawn from a closed set, so series counts stay\n")
	b.WriteString("predictable and alert rules can name a value with confidence:\n\n")
	b.WriteString("| Label | Values | Used by |\n")
	b.WriteString("| --- | --- | --- |\n")
	byLabel := map[string][]string{}
	var order []string
	for _, e := range metricCatalogue {
		for _, l := range e.Labels {
			if _, ok := byLabel[l.Name]; !ok {
				order = append(order, l.Name)
			}
			byLabel[l.Name] = append(byLabel[l.Name], "`"+e.Name+"`")
		}
	}
	sort.Strings(order)
	for _, name := range order {
		// The value text: the first metric to document the label states
		// the set; repeats point at it rather than restating it.
		var values string
		for _, e := range metricCatalogue {
			for _, l := range e.Labels {
				if l.Name == name {
					values = l.Values
					break
				}
			}
			if values != "" {
				break
			}
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s |\n",
			name, metricCell(values), metricCell(strings.Join(byLabel[name], ", ")))
	}
	b.WriteString("\n")

	b.WriteString(`The ` + "`unmatched`" + ` route value and the hostname-only ` + "`provider`" + ` value
are the two sets described in prose above; both are produced by the
server, never by the client, which is what keeps their cardinality
bounded.` + "\n")

	return b.String()
}

// labelCell renders a metric's labels for the docs table: each name
// followed by its values in parentheses.
func labelCell(e metricSpec) string {
	if len(e.Labels) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(e.Labels))
	for _, l := range e.Labels {
		parts = append(parts, "`"+l.Name+"` ("+l.Values+")")
	}
	return strings.Join(parts, "; ")
}

// metricCell keeps markdown tables from breaking: pipes escaped, no raw
// newlines.
func metricCell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
