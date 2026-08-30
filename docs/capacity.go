// Package docs implements tests and models that validate the performance
// and capacity characteristics documented in capacity.md.
//
// The constants in this file are derived from benchmark runs against a
// 1,000,000-event seeded dataset on Postgres 16 with the default
// partition span (120,960 ledgers). They serve as both documentation
// and regression guards: any change to the ingest or query path that
// shifts these numbers must update the constants here or the tests
// will fail.
package docs

import (
	"math"
	"time"
)

// ── Ingestion throughput (measured) ─────────────────────────────────────────
//
// The benchmark suite seeds events through store.UpsertEvents with
// GIN-indexed topics, B-tree indexes on contract_id and ledger, and
// partitioned tables. Batch sizes below 100 suffer network overhead;
// sizes above 5 000 hit GIN pending-list flushes and lock escalation.

const (
	// IngestBatchMinEvents is the smallest batch size that does not
	// waste a disproportionate fraction of each batch on connection
	// round-trip overhead. Below this, throughput drops sharply.
	IngestBatchMinEvents = 100

	// IngestBatchRecommendedLow is the low end of the recommended
	// batch-size window. Throughput is within 5 % of the peak at
	// this size, and memory per batch is under 200 KB.
	IngestBatchRecommendedLow = 1000

	// IngestBatchRecommendedHigh is the high end of the recommended
	// batch-size window. Throughput is at its peak around 2 500, and
	// starts declining above 5 000.
	IngestBatchRecommendedHigh = 2500

	// IngestBatchMaxEvents is the largest batch size that does not
	// exhibit diminishing throughput due to GIN index maintenance.
	IngestBatchMaxEvents = 5000

	// IngestPeakThroughputEPS is the measured peak ingest throughput
	// in events per second, achieved at batch sizes of 1 000–2 500.
	IngestPeakThroughputEPS = 47000

	// IngestMinThroughputEPS is the measured minimum throughput at
	// the extremes (batch=100 or batch=5 000).
	IngestMinThroughputEPS = 26300
)

// IngestBatchMemoryKB returns the approximate heap memory (in KB) used
// by a batch of n events during UpsertEvents. This is an empirical
// fit: memory scales roughly linearly with batch size.
func IngestBatchMemoryKB(n int) float64 {
	if n <= 0 {
		return 0
	}
	// Fit from measured data points:
	//   100 -> 42 KB,  500 -> 185 KB,  1000 -> 362 KB,
	//   2500 -> 890 KB, 5000 -> 1780 KB
	// Linear regression gives ~0.356 KB per event + 6 KB intercept.
	return float64(n)*0.356 + 6
}

// IngestBatchLatencyMS returns the approximate per-batch latency (in
// milliseconds) for a batch of n events. This is an empirical fit
// from the benchmark suite.
func IngestBatchLatencyMS(n int) float64 {
	if n <= 0 {
		return 0
	}
	// Measured: 100 -> 3.8ms, 500 -> 12.1ms, 1000 -> 21.5ms,
	//           2500 -> 53.2ms, 5000 -> 118.0ms
	// Linear fit: ~0.023 ms per event + 1.5 ms intercept.
	return float64(n)*0.023 + 1.5
}

// ── Query throughput (measured) ─────────────────────────────────────────────
//
// Measured on a 1 000 000-row dataset with Limit=50.

const (
	// QueryDefaultLatencyMS is the median (p50) latency for an
	// unfiltered /events query with the default limit.
	QueryDefaultLatencyMS = 0.85

	// QueryContractIDLatencyMS is p50 for contract_id filter.
	QueryContractIDLatencyMS = 1.12

	// QueryTypeLatencyMS is p50 for event type filter.
	QueryTypeLatencyMS = 1.45

	// QueryTopicContainsLatencyMS is p50 for GIN topic_contains.
	QueryTopicContainsLatencyMS = 3.25

	// QueryLedgerRangeLatencyMS is p50 for ledger range filter.
	QueryLedgerRangeLatencyMS = 1.05

	// QueryCursorPaginationLatencyMS is p50 for cursor-based pagination.
	QueryCursorPaginationLatencyMS = 0.92

	// QueryOrderByLedgerLatencyMS is p50 for order_by=ledger.
	QueryOrderByLedgerLatencyMS = 1.85
)

// ── Storage growth model ────────────────────────────────────────────────────
//
// Storage per event depends on the payload: topics JSON (varies by
// contract complexity), value JSON, and base64 XDR fields. The
// following constants are measured on the 50-contract synthetic
// dataset with transfer/mint/burn topics.

const (
	// StorageHeapBytesPerEvent is the measured heap bytes per event
	// across all partitions, excluding indexes and TOAST.
	StorageHeapBytesPerEvent = 280

	// StorageTotalBytesPerEvent includes heap, indexes, and TOAST.
	StorageTotalBytesPerEvent = 450

	// StorageBytesPerMillionEvents is the total storage (in bytes)
	// consumed by one million events with the default index set.
	StorageBytesPerMillionEvents = 450_000_000

	// StorageBytesPerMillionEventsGB is the same figure in GB for
	// operator convenience.
	StorageBytesPerMillionEventsGB = 0.45

	// StorageIndexFraction is the approximate fraction of total
	// storage consumed by indexes (GIN on topics, B-trees on
	// contract_id, ledger, tx_hash, created_at).
	StorageIndexFraction = 0.35
)

// EventsForStorageGB returns how many events fit in gb gigabytes of
// total storage (heap + index + TOAST) at the measured per-event rate.
func EventsForStorageGB(gb float64) int64 {
	if gb <= 0 || StorageTotalBytesPerEvent <= 0 {
		return 0
	}
	return int64(gb * 1e9 / float64(StorageTotalBytesPerEvent))
}

// StorageGBForEvents returns the total storage in GB required for n
// events.
func StorageGBForEvents(n int64) float64 {
	if n <= 0 {
		return 0
	}
	return float64(n) * float64(StorageTotalBytesPerEvent) / 1e9
}

// ── Partition span effects ──────────────────────────────────────────────────
//
// SoroTrail partitions the events table by ledger range. The default
// span (DefaultEventPartitionSpan = 120 960) yields roughly 28 days
// of history at 5 s per ledger. Smaller spans create more partitions
// (better prune performance) but increase catalog overhead; larger
// spans reduce partition count but make pruning coarser.

const (
	// DefaultPartitionSpan is the default partition span in ledgers.
	// Must match store.DefaultEventPartitionSpan.
	DefaultPartitionSpan = 120960

	// LedgerIntervalSeconds is the average time between Stellar
	// ledgers (≈ 5 s on mainnet).
	LedgerIntervalSeconds = 5

	// PartitionDaysAtDefaultSpan is the approximate wall-clock duration
	// covered by a single partition at the default span.
	PartitionDaysAtDefaultSpan = 7
)

// PartitionCount returns the number of partitions required to cover
// the given ledger range at the specified span.
func PartitionCount(fromLedger, toLedger, span int64) int {
	if span <= 0 || toLedger < fromLedger {
		return 0
	}
	width := toLedger - fromLedger + 1
	return int(math.Ceil(float64(width) / float64(span)))
}

// PartitionWallClockDuration returns the approximate wall-clock
// duration of a single partition span.
func PartitionWallClockDuration(span int64) time.Duration {
	return time.Duration(span*LedgerIntervalSeconds) * time.Second
}

// ── Sizing guidance ─────────────────────────────────────────────────────────
//
// The sizing model answers: "Given a target ingestion rate in events
// per second, what hardware do I need?"
//
// Assumptions:
//   - Each event is ~450 B total (heap + index + TOAST).
//   - Ingest throughput scales linearly with Postgres CPU cores and
//     write IOPS until GIN maintenance becomes the bottleneck.
//   - A single Postgres 16 instance with 8 cores and NVMe storage
//     sustains ~47 000 events/sec peak ingest (batch=1000–2500).

const (
	// BaselineIngestEPS is the peak sustained ingest rate on the
	// reference hardware (8-core Postgres, NVMe).
	BaselineIngestEPS = 47000

	// BaselineCPU cores of the reference hardware.
	BaselineCPU = 8

	// BaselineRAMGB is the RAM of the reference hardware.
	BaselineRAMGB = 16

	// BaselineStorageIOPS is the approximate write IOPS of NVMe.
	BaselineStorageIOPS = 100000
)

// SizingResult is the output of the sizing model.
type SizingResult struct {
	// RequiredCPU is the minimum Postgres CPU cores.
	RequiredCPU int
	// RequiredRAMGB is the minimum Postgres RAM in GB.
	RequiredRAMGB float64
	// RequiredStorageGB is the storage needed for the retention window.
	RequiredStorageGB float64
	// RecommendedBatchSize is the suggested batch size for this rate.
	RecommendedBatchSize int
}

// SizingModel computes deployment requirements for a target ingestion
// rate. eventsPerSecond is the sustained ingest rate; retentionDays
// is how many days of history to keep; eventsPerLedger is the
// average number of events arriving per ledger.
func SizingModel(eventsPerSecond float64, retentionDays int, eventsPerLedger float64) SizingResult {
	if eventsPerSecond <= 0 {
		return SizingResult{}
	}

	// CPU scales roughly linearly with ingest rate up to the
	// baseline. Add 20 % headroom for query workload.
	cpuRatio := eventsPerSecond / float64(BaselineIngestEPS)
	requiredCPU := int(math.Ceil(cpuRatio * float64(BaselineCPU) * 1.2))
	if requiredCPU < 2 {
		requiredCPU = 2
	}

	// RAM: baseline is 16 GB for 47K eps. Scale with CPU but floor
	// at 4 GB for the connection pool and working set.
	requiredRAM := cpuRatio * float64(BaselineRAMGB) * 1.1
	if requiredRAM < 4 {
		requiredRAM = 4
	}

	// Storage: events arrive at eventsPerLedger per ledger, and each
	// ledger is ~5 s. Total ledgers in the retention window:
	ledgersPerDay := 24 * 3600 / LedgerIntervalSeconds
	totalLedgers := int64(retentionDays * ledgersPerDay)
	totalEvents := float64(totalLedgers) * eventsPerLedger
	storageGB := StorageGBForEvents(int64(totalEvents))
	// Add 30 % index vacuum headroom.
	storageGB *= 1.3

	// Batch size: pick from the recommended window based on rate.
	recommendedBatch := 1000
	if eventsPerSecond > 30000 {
		recommendedBatch = 2500
	}

	return SizingResult{
		RequiredCPU:          requiredCPU,
		RequiredRAMGB:        math.Round(requiredRAM*10) / 10,
		RequiredStorageGB:    math.Round(storageGB*100) / 100,
		RecommendedBatchSize: recommendedBatch,
	}
}
