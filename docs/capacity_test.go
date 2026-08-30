package docs

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ── Ingestion batch constants ───────────────────────────────────────────────

func TestIngestBatchConstants(t *testing.T) {
	t.Run("recommended window is within absolute bounds", func(t *testing.T) {
		assert.GreaterOrEqual(t, IngestBatchRecommendedLow, IngestBatchMinEvents,
			"recommended low must be at least the minimum")
		assert.LessOrEqual(t, IngestBatchRecommendedHigh, IngestBatchMaxEvents,
			"recommended high must not exceed the maximum")
	})

	t.Run("peak throughput exceeds minimum throughput", func(t *testing.T) {
		assert.Greater(t, IngestPeakThroughputEPS, IngestMinThroughputEPS,
			"peak must be strictly greater than minimum")
	})

	t.Run("all batch thresholds are positive", func(t *testing.T) {
		for _, v := range []int{
			IngestBatchMinEvents,
			IngestBatchRecommendedLow,
			IngestBatchRecommendedHigh,
			IngestBatchMaxEvents,
		} {
			assert.Positive(t, v)
		}
	})
}

func TestIngestBatchMemoryKB(t *testing.T) {
	tests := []struct {
		name   string
		n      int
		wantKB float64
		upper  float64
	}{
		{"zero batch returns 0", 0, 0, 0},
		{"negative batch returns 0", -10, 0, 0},
		{"100 events ≈ 42 KB", 100, 42, 10},
		{"500 events ≈ 185 KB", 500, 185, 20},
		{"1000 events ≈ 362 KB", 1000, 362, 30},
		{"2500 events ≈ 890 KB", 2500, 890, 60},
		{"5000 events ≈ 1780 KB", 5000, 1780, 120},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IngestBatchMemoryKB(tt.n)
			assert.InDelta(t, tt.wantKB, got, tt.upper,
				"IngestBatchMemoryKB(%d) = %.1f, want ≈ %.1f ± %.1f", tt.n, got, tt.wantKB, tt.upper)
		})
	}

	t.Run("monotonically increasing", func(t *testing.T) {
		sizes := []int{100, 500, 1000, 2500, 5000}
		var prev float64
		for _, s := range sizes {
			cur := IngestBatchMemoryKB(s)
			assert.Greater(t, cur, prev, "memory must increase with batch size")
			prev = cur
		}
	})
}

func TestIngestBatchLatencyMS(t *testing.T) {
	tests := []struct {
		name   string
		n      int
		wantMS float64
		upper  float64
	}{
		{"zero batch returns 0", 0, 0, 0},
		{"negative batch returns 0", -5, 0, 0},
		{"100 events ≈ 3.8 ms", 100, 3.8, 1.0},
		{"500 events ≈ 12.1 ms", 500, 12.1, 2.0},
		{"1000 events ≈ 21.5 ms", 1000, 21.5, 3.0},
		{"2500 events ≈ 53.2 ms", 2500, 53.2, 6.0},
		{"5000 events ≈ 118.0 ms", 5000, 118.0, 12.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IngestBatchLatencyMS(tt.n)
			assert.InDelta(t, tt.wantMS, got, tt.upper,
				"IngestBatchLatencyMS(%d) = %.1f, want ≈ %.1f ± %.1f", tt.n, got, tt.wantMS, tt.upper)
		})
	}
}

// ── Query latency constants ─────────────────────────────────────────────────

func TestQueryLatencyOrdering(t *testing.T) {
	// The unfiltered default query must be the fastest path; topic
	// contains (GIN) must be the slowest. Intermediate filters
	// should fall between.
	t.Run("default is fastest", func(t *testing.T) {
		assert.LessOrEqual(t, QueryDefaultLatencyMS, QueryContractIDLatencyMS)
		assert.LessOrEqual(t, QueryDefaultLatencyMS, QueryTypeLatencyMS)
		assert.LessOrEqual(t, QueryDefaultLatencyMS, QueryLedgerRangeLatencyMS)
		assert.LessOrEqual(t, QueryDefaultLatencyMS, QueryCursorPaginationLatencyMS)
	})

	t.Run("topic contains is slowest", func(t *testing.T) {
		assert.GreaterOrEqual(t, QueryTopicContainsLatencyMS, QueryDefaultLatencyMS)
		assert.GreaterOrEqual(t, QueryTopicContainsLatencyMS, QueryContractIDLatencyMS)
		assert.GreaterOrEqual(t, QueryTopicContainsLatencyMS, QueryTypeLatencyMS)
		assert.GreaterOrEqual(t, QueryTopicContainsLatencyMS, QueryOrderByLedgerLatencyMS)
	})

	t.Run("all latencies are sub-10ms", func(t *testing.T) {
		latencies := []struct {
			name string
			ms   float64
		}{
			{"default", QueryDefaultLatencyMS},
			{"contract_id", QueryContractIDLatencyMS},
			{"type", QueryTypeLatencyMS},
			{"topic_contains", QueryTopicContainsLatencyMS},
			{"ledger_range", QueryLedgerRangeLatencyMS},
			{"cursor", QueryCursorPaginationLatencyMS},
			{"order_by_ledger", QueryOrderByLedgerLatencyMS},
		}
		for _, l := range latencies {
			assert.Less(t, l.ms, 10.0, "%s latency must be under 10 ms", l.name)
		}
	})
}

// ── Storage growth ──────────────────────────────────────────────────────────

func TestStorageConstants(t *testing.T) {
	t.Run("heap is less than total", func(t *testing.T) {
		assert.Less(t, StorageHeapBytesPerEvent, StorageTotalBytesPerEvent,
			"heap must be less than total (total includes index + TOAST)")
	})

	t.Run("index fraction is between 20% and 50%", func(t *testing.T) {
		assert.InDelta(t, 0.35, StorageIndexFraction, 0.15,
			"index fraction should be between 20%% and 50%%")
	})

	t.Run("per-million matches per-event × 1M", func(t *testing.T) {
		computed := int(StorageTotalBytesPerEvent) * 1_000_000
		assert.Equal(t, computed, StorageBytesPerMillionEvents,
			"per-million should equal per-event × 1,000,000")
	})

	t.Run("GB constant matches byte constant", func(t *testing.T) {
		gotGB := float64(StorageBytesPerMillionEvents) / 1e9
		assert.InDelta(t, StorageBytesPerMillionEventsGB, gotGB, 0.01,
			"GB constant should equal bytes / 1e9")
	})
}

func TestEventsForStorageGB(t *testing.T) {
	tests := []struct {
		name string
		gb   float64
		want int64
	}{
		{"zero returns 0", 0, 0},
		{"negative returns 0", -1, 0},
		{"1 GB fits roughly 2.4M events", 1, 2_222_222},
		{"10 GB fits roughly 22M events", 10, 22_222_222},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventsForStorageGB(tt.gb)
			if tt.want == 0 {
				assert.Equal(t, int64(0), got)
			} else {
				assert.InDelta(t, tt.want, got, float64(tt.want)*0.01,
					"EventsForStorageGB(%.1f) = %d, want ≈ %d", tt.gb, got, tt.want)
			}
		})
	}
}

func TestStorageGBForEvents(t *testing.T) {
	t.Run("round-trip consistency with EventsForStorageGB", func(t *testing.T) {
		events := int64(5_000_000)
		gb := StorageGBForEvents(events)
		recovered := EventsForStorageGB(gb)
		// Allow 2 % round-trip error due to float64 precision.
		assert.InDelta(t, events, recovered, float64(events)*0.02,
			"round-trip should preserve event count within 2 %%")
	})

	t.Run("zero and negative inputs return 0", func(t *testing.T) {
		assert.Equal(t, 0.0, StorageGBForEvents(0))
		assert.Equal(t, 0.0, StorageGBForEvents(-100))
	})

	t.Run("linear scaling", func(t *testing.T) {
		gb1 := StorageGBForEvents(1_000_000)
		gb2 := StorageGBForEvents(2_000_000)
		assert.InDelta(t, gb1*2, gb2, 0.001,
			"double events should yield double storage")
	})
}

// ── Partition span ──────────────────────────────────────────────────────────

func TestPartitionCount(t *testing.T) {
	tests := []struct {
		name           string
		from, to, span int64
		wantPartitions int
	}{
		{"single partition fits", 1, 100, 100, 1},
		{"exactly one partition", 1, 120960, 120960, 1},
		{"two partitions needed", 1, 120961, 120960, 2},
		{"zero span returns 0", 1, 100, 0, 0},
		{"inverted range returns 0", 100, 1, 50, 0},
		{"default span covers one week", 1, DefaultPartitionSpan, DefaultPartitionSpan, 1},
		{"two weeks of ledgers", 1, DefaultPartitionSpan * 2, DefaultPartitionSpan, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PartitionCount(tt.from, tt.to, tt.span)
			assert.Equal(t, tt.wantPartitions, got)
		})
	}
}

func TestPartitionWallClockDuration(t *testing.T) {
	t.Run("default span ≈ 7 days", func(t *testing.T) {
		d := PartitionWallClockDuration(DefaultPartitionSpan)
		assert.Equal(t, 7*24*time.Hour, d,
			"default partition span should cover exactly 7 days")
	})

	t.Run("half span ≈ 3.5 days", func(t *testing.T) {
		d := PartitionWallClockDuration(DefaultPartitionSpan / 2)
		assert.Equal(t, 3*24*time.Hour+12*time.Hour, d)
	})

	t.Run("scaling is linear", func(t *testing.T) {
		d1 := PartitionWallClockDuration(1000)
		d2 := PartitionWallClockDuration(2000)
		assert.Equal(t, d1*2, d2)
	})
}

// ── Sizing model ────────────────────────────────────────────────────────────

func TestSizingModel(t *testing.T) {
	t.Run("zero rate returns zero result", func(t *testing.T) {
		r := SizingModel(0, 30, 10)
		assert.Equal(t, SizingResult{}, r)
	})

	t.Run("baseline rate returns baseline hardware", func(t *testing.T) {
		r := SizingModel(float64(BaselineIngestEPS), 30, 10)
		// At baseline rate with 20% headroom: ceil(8 * 1.2) = 10
		assert.GreaterOrEqual(t, r.RequiredCPU, BaselineCPU,
			"baseline rate should need at least baseline CPU")
		assert.GreaterOrEqual(t, r.RequiredRAMGB, float64(BaselineRAMGB),
			"baseline rate should need at least baseline RAM")
		assert.Greater(t, r.RequiredStorageGB, 0.0,
			"baseline rate must need some storage")
	})

	t.Run("higher rate requires more CPU", func(t *testing.T) {
		r1 := SizingModel(10000, 30, 10)
		r2 := SizingModel(40000, 30, 10)
		assert.Greater(t, r2.RequiredCPU, r1.RequiredCPU,
			"4x rate should need more CPU")
		assert.Greater(t, r2.RequiredRAMGB, r1.RequiredRAMGB,
			"4x rate should need more RAM")
	})

	t.Run("longer retention requires more storage", func(t *testing.T) {
		r1 := SizingModel(10000, 7, 10)
		r2 := SizingModel(10000, 90, 10)
		assert.Greater(t, r2.RequiredStorageGB, r1.RequiredStorageGB,
			"longer retention should need more storage")
	})

	t.Run("higher events per ledger requires more storage", func(t *testing.T) {
		r1 := SizingModel(10000, 30, 5)
		r2 := SizingModel(10000, 30, 50)
		assert.Greater(t, r2.RequiredStorageGB, r1.RequiredStorageGB,
			"more events per ledger should need more storage")
	})

	t.Run("minimum CPU is 2 cores", func(t *testing.T) {
		r := SizingModel(1, 30, 1)
		assert.GreaterOrEqual(t, r.RequiredCPU, 2,
			"even minimal rate should need at least 2 cores")
	})

	t.Run("minimum RAM is 4 GB", func(t *testing.T) {
		r := SizingModel(1, 30, 1)
		assert.GreaterOrEqual(t, r.RequiredRAMGB, 4.0,
			"even minimal rate should need at least 4 GB RAM")
	})

	t.Run("recommended batch size is in valid range", func(t *testing.T) {
		for _, rate := range []float64{1000, 10000, 30000, 50000, 100000} {
			r := SizingModel(rate, 30, 10)
			assert.GreaterOrEqual(t, r.RecommendedBatchSize, IngestBatchRecommendedLow,
				"batch size must be at least recommended low at rate %.0f", rate)
			assert.LessOrEqual(t, r.RecommendedBatchSize, IngestBatchRecommendedHigh,
				"batch size must not exceed recommended high at rate %.0f", rate)
		}
	})
}

// ── Cross-cutting invariants ────────────────────────────────────────────────

func TestSizingConsistency(t *testing.T) {
	t.Run("sizing model output should handle extreme values", func(t *testing.T) {
		// Very high rate
		r := SizingModel(1_000_000, 365, 100)
		assert.Greater(t, r.RequiredCPU, 0)
		assert.Greater(t, r.RequiredRAMGB, 0.0)
		assert.Greater(t, r.RequiredStorageGB, 0.0)
	})

	t.Run("partition count formula is consistent with wall clock", func(t *testing.T) {
		days := 30
		ledgersPerDay := 24 * 3600 / LedgerIntervalSeconds
		totalLedgers := int64(days * ledgersPerDay)
		parts := PartitionCount(1, totalLedgers, DefaultPartitionSpan)
		dur := PartitionWallClockDuration(DefaultPartitionSpan)
		totalDays := time.Duration(days) * 24 * time.Hour
		expectedParts := int(math.Ceil(float64(totalDays) / float64(dur)))
		assert.Equal(t, expectedParts, parts,
			"partition count should match wall-clock calculation")
	})
}
