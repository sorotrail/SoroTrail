package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Bounded per-tenant metrics instrumentation.
// Cardinality is bounded by active tenants.
var (
	TenantRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "requests_total",
			Help:      "Total requests by tenant and outcome.",
		},
		[]string{"tenant_id", "outcome"},
	)

	TenantQueryDurationMsSum = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "query_duration_ms_sum",
			Help:      "Sum of query duration in milliseconds by tenant.",
		},
		[]string{"tenant_id"},
	)

	TenantEventsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "events_ingested_total",
			Help:      "Events ingested on behalf of the tenant.",
		},
		[]string{"tenant_id"},
	)

	TenantRowsStoredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "rows_stored_total",
			Help:      "Rows stored attributed to the tenant.",
		},
		[]string{"tenant_id"},
	)

	TenantSubscriptionDeliveriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "subscription_deliveries_total",
			Help:      "Subscription deliveries for the tenant.",
		},
		[]string{"tenant_id"},
	)

	TenantBytesExportedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "sorotrail",
			Subsystem: "tenant",
			Name:      "bytes_exported_total",
			Help:      "Bytes exported for the tenant.",
		},
		[]string{"tenant_id"},
	)
)
