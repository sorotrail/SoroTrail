package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    EventsIngested = promauto.NewCounter(prometheus.CounterOpts{
        Name: "sorotrail_events_ingested_total",
        Help: "Total number of contract events ingested into Postgres.",
    })
    IngestErrors = promauto.NewCounter(prometheus.CounterOpts{
        Name: "sorotrail_ingest_errors_total",
        Help: "Total number of ingestion failures.",
    })
    RPCCallDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "sorotrail_rpc_call_duration_seconds",
        Help:    "Latency of Stellar RPC calls in seconds.",
        Buckets: prometheus.DefBuckets,
    }, []string{"method"})
    DBWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "sorotrail_db_write_duration_seconds",
        Help:    "Postgres write latency in seconds.",
        Buckets: prometheus.DefBuckets,
    })
    IngestLag = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "sorotrail_ingest_lag_ledgers",
        Help: "Number of ledgers the ingester is behind the chain head.",
    })
)
