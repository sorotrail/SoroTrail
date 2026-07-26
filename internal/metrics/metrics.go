// Package metrics exposes Prometheus gauges for SoroTrail operational
// health. The ingestion-lag gauge tracks the difference between the
// latest ledger known to the Stellar RPC and the last ledger the
// ingester persisted, surfacing how far behind the indexer is.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector holds all SoroTrail Prometheus metrics and provides an
// HTTP handler for the /metrics endpoint. Create exactly one
// Collector per process via New.
type Collector struct {
	registry      *prometheus.Registry
	ingestionLag  prometheus.Gauge
}

// New creates a Collector with its own Prometheus registry so tests
// can create isolated instances without conflicting with the default
// global registry.
func New() *Collector {
	reg := prometheus.NewRegistry()
	lag := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_ingestion_lag_ledgers",
		Help: "Difference between the latest RPC ledger and the last ingested ledger.",
	})
	reg.MustRegister(lag)
	return &Collector{registry: reg, ingestionLag: lag}
}

// SetIngestionLag computes latestRPC - lastIngested and records the
// result in the Prometheus gauge. A positive value means the ingester
// is behind; a negative value (which should not happen in steady state)
// is still recorded so operators can detect anomalies.
func (c *Collector) SetIngestionLag(latestRPC, lastIngested int64) {
	c.ingestionLag.Set(float64(latestRPC - lastIngested))
}

// Handler returns an HTTP handler that serves all registered
// Prometheus metrics in the exposition format.
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}
