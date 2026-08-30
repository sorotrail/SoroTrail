// Package metrics exports Prometheus instrumentation for SoroTrail.
//
// Pipeline counters, histograms, and gauges (sorotrail_*) are registered with
// the default Prometheus registry so promhttp.Handler() picks them up
// automatically; HTTPMetrics owns a per-server request-duration histogram
// served at GET /metrics alongside the global metrics.
package metrics

import (
	"net/http"

	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsIngested counts every event successfully persisted (including
	// duplicates that hit the ON CONFLICT DO NOTHING path — the metric
	// reflects the throughput of the pipeline, not only net-new rows).
	EventsIngested = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_events_ingested_total",
		Help: "Total number of events ingested (including duplicates resolved by idempotent upsert).",
	})

	// IngestErrors counts terminal failures during an ingestion pass
	// (RPC errors, decode failures, DB write failures). It does not
	// count retry attempts — only passes that end in an error.
	IngestErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_ingest_errors_total",
		Help: "Total number of ingestion passes that failed with an error.",
	})

	// RPCCallLatency records the wall-clock duration of a single
	// JSON-RPC call (HTTP round trip + body read + unmarshal), labelled
	// by method (a fixed enum: getEvents, getLatestLedger, getHealth,
	// getLedgerEntries, simulateTransaction) and outcome (success |
	// error). The histogram's _count for a (method, outcome) pair is the
	// call total, so this single metric answers both "how slow" and
	// "how many, and how many failed".
	RPCCallLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sorotrail_rpc_call_duration_seconds",
		Help:    "RPC call latency in seconds (HTTP round trip + body read + parse), labelled by method and outcome.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "outcome"})

	// RPCRetriesTotal counts every retry attempt issued by RetryClient
	// (attempts after the first). reason is where the wait came from:
	// "backoff" for the computed exponential schedule, "retry_after"
	// for a provider-supplied 429 Retry-After hint.
	RPCRetriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_rpc_retries_total",
		Help: "Total number of RPC retry attempts, labelled by method and reason (backoff | retry_after).",
	}, []string{"method", "reason"})

	// RPCBackoffSeconds is the cumulative wall-clock time spent sleeping
	// between retries. A long tail here means the provider (or our own
	// rate limiter) is throttling us, which is cheap to spot next to the
	// retry count.
	RPCBackoffSeconds = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_rpc_backoff_seconds_total",
		Help: "Total seconds spent sleeping between RPC retries, labelled by method.",
	}, []string{"method"})

	// RPCFailoversTotal counts failover events in the multi-provider
	// client: "switch" when traffic moved to a different provider,
	// "reanchor" when a cursor request hit ErrFailoverReanchor and the
	// caller had to discard its cursor.
	RPCFailoversTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_rpc_failovers_total",
		Help: "Total number of RPC provider failover events, labelled by reason (switch | reanchor).",
	}, []string{"reason"})

	// RPCCircuitBreakerState exposes the circuit breaker's current state
	// as a 0/1 gauge per state (closed | open | half-open), so a stuck
	// breaker is visible at a glance and PromQL can alert on
	// sorotrail_rpc_circuit_breaker_state{state="open"} == 1.
	RPCCircuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sorotrail_rpc_circuit_breaker_state",
		Help: "Circuit breaker state as a 0/1 gauge per state (closed | open | half-open).",
	}, []string{"state"})

	// RPCProviderState exposes each failover provider's health state as
	// a 0/1 gauge per (provider, state) pair (active | degraded | down).
	// The provider label is the URL's hostname only — credentials and
	// scheme are never exposed (see rpc.providerLabel).
	RPCProviderState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sorotrail_rpc_provider_state",
		Help: "Per-provider failover health state as a 0/1 gauge per state (active | degraded | down).",
	}, []string{"provider", "state"})

	// DBWriteLatency records the wall-clock duration of a database write
	// operation (batch upsert, replace-in-range, etc.).
	DBWriteLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sorotrail_db_write_duration_seconds",
		Help:    "Database write latency in seconds (batch insert/upsert/repair).",
		Buckets: prometheus.DefBuckets,
	})

	// DBQueryDuration records the wall-clock duration of a database query
	// (SELECT operations). Labelled by operation (e.g. "list_events",
	// "count_events", "get_contract").
	DBQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sorotrail_db_query_duration_seconds",
		Help:    "Database query duration in seconds, labelled by operation.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	// IngestionLag is the number of ledgers the indexer is behind the
	// Stellar RPC chain head. Updated after every ingestion pass that
	// has access to the chain head.
	IngestionLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_ingestion_lag_ledgers",
		Help: "Number of ledgers the indexer is behind the chain head.",
	})

	// ---- Batch / backpressure instrumentation ----
	// The batch controller that splits a fetch page into many UpsertEvents
	// calls (see ingester.batchController) exposes its heartbeat here so
	// operators can tell at a glance whether ingestion is shaving its
	// batches down or throttling to protect the database at peak.

	// EventBatchWrites counts every UpsertEvents call issued by the
	// ingester with at least one row. When batching is disabled this is
	// exactly one per non-empty page; with batching enabled it reflects
	// how many chunks the adaptive controller split a page into.
	EventBatchWrites = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_event_batch_writes_total",
		Help: "Total number of event store writes (UpsertEvents calls) issued by the ingester.",
	})

	// EventBatchSize is the current batch size the controller feeds the
	// store per write. It stays at the configured maximum while writes are
	// comfortably inside the latency budget and steps down under latency
	// pressure, so a falling gauge is the earliest signal the store is
	// starting to strain.
	EventBatchSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_event_batch_size",
		Help: "Current adaptive event batch size used for each store write.",
	})

	// EventBackpressure counts how many times the ingester inserted a
	// deliberate sleep between writes because the store fell behind the
	// latency budget. A non-zero value proves backpressure is firing;
	// paired with EventBackpressureSeconds it explains how much time was
	// spent between the initial surge and the DB catching up.
	EventBackpressure = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_event_backpressure_total",
		Help: "Total number of throttle sleeps inserted between event batch writes.",
	})

	// EventBackpressureSeconds is the cumulative wall-clock time spent
	// sleeping under backpressure. The monetary impact of a surge is
	// best priced in seconds: a long tail here means the DB was slow
	// for a sustained stretch, not just a burst.
	EventBackpressureSeconds = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_event_backpressure_seconds_total",
		Help: "Total seconds spent throttling event writes under backpressure.",
	})
	// ---- End batch instrumentation ----
)

func init() {
	prometheus.MustRegister(
		EventsIngested,
		IngestErrors,
		RPCCallLatency,
		DBWriteLatency,
		DBQueryDuration,
		IngestionLag,
		EventBatchWrites,
		EventBatchSize,
		EventBackpressure,
		EventBackpressureSeconds,
		RPCRetriesTotal,
		RPCBackoffSeconds,
		RPCFailoversTotal,
		RPCCircuitBreakerState,
		RPCProviderState,
	)
}

// Handler returns an http.Handler that serves the /metrics endpoint in
// Prometheus exposition format.
func Handler() http.Handler {
	return promhttp.Handler()
}

// unmatchedRoute labels requests that never reached a registered chi route
// (404s from a totally unknown path). Falling back to r.URL.Path there
// would let clients probing random paths grow the histogram's cardinality
// without bound.
const unmatchedRoute = "unmatched"

// HTTPMetrics holds the HTTP request-duration histogram and the registry
// it's registered against. Each Server owns its own instance (rather than
// using the global Prometheus registry) so tests can construct one per
// case without collectors colliding across parallel tests.
type HTTPMetrics struct {
	registry *prometheus.Registry
	duration *prometheus.HistogramVec
}

// New creates an HTTPMetrics with a fresh registry and registers the
// request-duration histogram against it.
func New() *HTTPMetrics {
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds, labeled by route, method, and status code.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"route", "method", "status"},
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(duration)
	return &HTTPMetrics{registry: reg, duration: duration}
}

// Middleware records each request's duration in the http_request_duration_seconds
// histogram, labeled with the matched chi route pattern (e.g. "/events/{id}")
// rather than the raw URL path, so cardinality stays bounded regardless of
// how many distinct path parameter values are seen. It must be mounted via
// r.Use so it wraps chi's route matching: the route pattern is only
// resolved in the request context once next.ServeHTTP returns.
func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = unmatchedRoute
		}
		m.duration.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).
			Observe(time.Since(start).Seconds())
	})
}

// Handler serves the registered metrics in the Prometheus exposition format.
// It combines the per-server HTTP histogram with the global pipeline metrics
// (counters, latencies, and gauges such as sorotrail_ingestion_lag_ledgers)
// so a single /metrics scrape sees the whole picture.
func (m *HTTPMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{prometheus.DefaultGatherer, m.registry},
		promhttp.HandlerOpts{Registry: m.registry},
	)
}
