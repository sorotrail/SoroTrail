// Package metrics exposes SoroTrail's Prometheus instrumentation behind small
// helpers that keep metric names and label conventions out of business logic.
// All collectors are registered on a dedicated registry so the /metrics
// endpoint serves only SoroTrail metrics without leaking process-/Go-collector
// defaults.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics collects all Prometheus counters and gauges for SoroTrail.
// A nil *Metrics is safe: every helper method no-ops, so tests and
// callers that haven't wired observability are unaffected.
type Metrics struct {
	reg *prometheus.Registry

	eventsIngested prometheus.Counter
	lastIngested   prometheus.Gauge
	chainHead      prometheus.Gauge
	rpcRequests    *prometheus.CounterVec
	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
}

// New creates a Metrics instance with a dedicated Prometheus registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	eventsIngested := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_events_ingested_total",
		Help: "Total number of contract events persisted.",
	})
	lastIngested := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_last_ingested_ledger",
		Help: "Ledger sequence number of the last ingested event.",
	})
	chainHead := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_chain_head_ledger",
		Help: "Latest ledger reported by the Stellar RPC.",
	})
	rpcRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_rpc_requests_total",
		Help: "Total RPC requests by method and outcome.",
	}, []string{"method", "outcome"})
	httpRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_http_requests_total",
		Help: "Total HTTP requests by path and status code.",
	}, []string{"path", "status"})
	httpDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sorotrail_http_request_duration_seconds",
		Help:    "HTTP request duration histogram by path.",
		Buckets: prometheus.DefBuckets,
	}, []string{"path"})

	reg.MustRegister(
		eventsIngested,
		lastIngested,
		chainHead,
		rpcRequests,
		httpRequests,
		httpDuration,
	)

	return &Metrics{
		reg:            reg,
		eventsIngested: eventsIngested,
		lastIngested:   lastIngested,
		chainHead:      chainHead,
		rpcRequests:    rpcRequests,
		httpRequests:   httpRequests,
		httpDuration:   httpDuration,
	}
}

// Handler returns an http.Handler that serves the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// ---- Ingester helpers ----

// RecordEventsIngested adds n to the events-ingested counter.
func (m *Metrics) RecordEventsIngested(n int) {
	if m == nil {
		return
	}
	m.eventsIngested.Add(float64(n))
}

// SetLastIngestedLedger records the most recent ingested ledger.
func (m *Metrics) SetLastIngestedLedger(ledger int64) {
	if m == nil {
		return
	}
	m.lastIngested.Set(float64(ledger))
}

// SetChainHeadLedger records the latest chain-head ledger.
func (m *Metrics) SetChainHeadLedger(ledger uint32) {
	if m == nil {
		return
	}
	m.chainHead.Set(float64(ledger))
}

// ---- RPC helpers ----

// ObserveRPCRequest records one RPC call with its outcome. Error is nil for
// success; any non-nil error is recorded as "error".
func (m *Metrics) ObserveRPCRequest(method string, err error) {
	if m == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	m.rpcRequests.WithLabelValues(method, outcome).Inc()
}

// ---- HTTP helpers ----

// RecordHTTPRequest records one HTTP request with its path, status code, and
// duration.
func (m *Metrics) RecordHTTPRequest(path string, status int, d float64) {
	if m == nil {
		return
	}
	statusStr := strconv.Itoa(status)
	m.httpRequests.WithLabelValues(path, statusStr).Inc()
	m.httpDuration.WithLabelValues(path).Observe(d)
}
