// Package metrics exports Prometheus instrumentation for SoroTrail's
// ingestion pipeline. Every counter, histogram, and gauge is registered with
// the default Prometheus registry so promhttp.Handler() picks it up
// automatically.
//
// Usage (one-time registration done in init):
//
//	metrics.EventsIngested.Add(float64(len(events)))
//	timer := prometheus.NewTimer(metrics.DBWriteLatency)
//	defer timer.ObserveDuration()
package metrics

import (
	"net/http"

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
	// JSON-RPC call (HTTP round trip + body read + unmarshal).
	RPCCallLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sorotrail_rpc_call_duration_seconds",
		Help:    "RPC call latency in seconds (HTTP round trip + body read + parse).",
		Buckets: prometheus.DefBuckets,
	})

	// DBWriteLatency records the wall-clock duration of a database write
	// operation (batch upsert, replace-in-range, etc.).
	DBWriteLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sorotrail_db_write_duration_seconds",
		Help:    "Database write latency in seconds (batch insert/upsert/repair).",
		Buckets: prometheus.DefBuckets,
	})

	// IngestionLag is the number of ledgers the indexer is behind the
	// Stellar RPC chain head. Updated after every ingestion pass that
	// has access to the chain head.
	IngestionLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_ingestion_lag_ledgers",
		Help: "Number of ledgers the indexer is behind the chain head.",
	})
)

func init() {
	prometheus.MustRegister(
		EventsIngested,
		IngestErrors,
		RPCCallLatency,
		DBWriteLatency,
		IngestionLag,
	)
}

// Handler returns an http.Handler that serves the /metrics endpoint in
// Prometheus exposition format.
func Handler() http.Handler {
	return promhttp.Handler()
}
