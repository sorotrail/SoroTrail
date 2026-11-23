package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleXDRSymbol is a valid base64-encoded Soroban ScVal symbol ("transfer")
// used to benchmark real XDR decoding against JSON passthrough.
const sampleXDRSymbol = "AAAADwAAAAh0cmFuc2Zlcg=="

func syntheticContractID(idx int) string {
	return fmt.Sprintf("C%055d", idx)
}

func makeCapacityRPCEvents(n int, contracts int, withXDR bool) []rpc.Event {
	if contracts < 1 {
		contracts = 1
	}
	events := make([]rpc.Event, n)
	valJSON := json.RawMessage(`{"u64":1000000}`)
	topicJSON := []json.RawMessage{
		json.RawMessage(`{"symbol":"transfer"}`),
		json.RawMessage(`{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"}`),
	}
	for i := 0; i < n; i++ {
		ledger := uint32(100 + i/50)
		ev := rpc.Event{
			ID:         fmt.Sprintf("%010d-%09d", ledger, i),
			Ledger:     ledger,
			Type:       "contract",
			ContractID: syntheticContractID(i % contracts),
			TxHash:     fmt.Sprintf("%064x", i+1),
		}
		if withXDR {
			ev.Topic = []string{sampleXDRSymbol, sampleXDRSymbol}
			ev.Value = sampleXDRSymbol
		} else {
			ev.TopicJSON = topicJSON
			ev.ValueJSON = valJSON
		}
		events[i] = ev
	}
	return events
}

// ── 1. Per-Stage Pipeline Breakdown Benchmarks ──────────────────────────────

func BenchmarkIngestionStage_RPCFetch(b *testing.B) {
	syn := NewSyntheticRPC(LoadTestConfig{
		EventsPerLedger: 100,
		Ledgers:         20,
		PageLimit:       1000,
		Seed:            42,
	})
	ctx := context.Background()
	req := rpc.GetEventsRequest{
		StartLedger: 2,
		Pagination:  &rpc.Pagination{Limit: 1000},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := syn.GetEvents(ctx, req)
		if err != nil || len(resp.Events) == 0 {
			b.Fatalf("GetEvents failed: %v", err)
		}
		b.ReportMetric(float64(len(resp.Events)), "events/op")
	}
}

func BenchmarkIngestionStage_XDRDecode(b *testing.B) {
	dec := decode.XDRDecoder{}
	events := makeCapacityRPCEvents(100, 10, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ev := range events {
			for _, t := range ev.Topic {
				if _, err := dec.DecodeScVal(t); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := dec.DecodeScVal(ev.Value); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(events)), "events/op")
	}
}

func BenchmarkIngestionStage_JSONPassthroughDecode(b *testing.B) {
	dec := &passthroughDecoder{}
	events := makeCapacityRPCEvents(100, 10, false)
	ing := New(nil, newMockStore(), dec, loadTestLogger(), Options{StartLedger: 100})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, ev := range events {
			if _, err := ing.toStoreEvent(ev); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(events)), "events/op")
	}
}

func BenchmarkIngestionStage_FilterAndTransform(b *testing.B) {
	st := newMockStore()
	for c := 0; c < 50; c++ {
		st.watched = append(st.watched, store.WatchedContract{ContractID: syntheticContractID(c)})
	}
	ing := New(nil, st, decode.XDRDecoder{}, loadTestLogger(), Options{StartLedger: 100})
	events := makeCapacityRPCEvents(200, 50, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := make([]store.Event, 0, len(events))
		for _, raw := range events {
			ev, err := ing.toStoreEvent(raw)
			if err != nil {
				b.Fatal(err)
			}
			batch = append(batch, ev)
		}
		if len(batch) != len(events) {
			b.Fatalf("expected %d events, got %d", len(events), len(batch))
		}
		b.ReportMetric(float64(len(batch)), "events/op")
	}
}

func BenchmarkIngestionStage_StoreWrite(b *testing.B) {
	ing := New(nil, newMockStore(), &passthroughDecoder{}, loadTestLogger(), Options{StartLedger: 100})
	rawEvents := makeCapacityRPCEvents(1000, 20, false)
	storeBatch := make([]store.Event, len(rawEvents))
	for i, r := range rawEvents {
		ev, err := ing.toStoreEvent(r)
		if err != nil {
			b.Fatal(err)
		}
		storeBatch[i] = ev
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := newMockStore()
		if _, err := st.UpsertEvents(ctx, storeBatch); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(storeBatch)), "events/op")
	}
}

// ── 2. Multi-Dimensional Capacity Scaling Benchmarks ────────────────────────

func BenchmarkIngestionCapacity_ByWatchedContracts(b *testing.B) {
	for _, numContracts := range []int{1, 10, 50, 250} {
		b.Run(fmt.Sprintf("contracts_%d", numContracts), func(b *testing.B) {
			rawEvents := makeCapacityRPCEvents(500, numContracts, false)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st := newMockStore()
				for c := 0; c < numContracts; c++ {
					st.watched = append(st.watched, store.WatchedContract{ContractID: syntheticContractID(c)})
				}
				client := &mockRPC{
					health: rpc.Health{LatestLedger: 120, OldestLedger: 90},
					eventsResps: []rpc.GetEventsResponse{{
						Events:       rawEvents,
						LatestLedger: 120,
					}},
				}
				ing := New(client, st, &passthroughDecoder{}, loadTestLogger(), Options{
					StartLedger:    100,
					PageLimit:      1000,
					BatchSize:      1000,
					WriteBatchSize: 1000,
				})
				if _, err := ing.RunOnceForTest(ctx); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(rawEvents)), "events/op")
			}
		})
	}
}

func BenchmarkIngestionCapacity_ByEventsPerLedger(b *testing.B) {
	for _, epl := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("events_per_ledger_%d", epl), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st := &loadtestStore{mockStore: *newMockStore()}
				cfg := LoadTestConfig{
					EventsPerLedger: epl,
					Ledgers:         5,
					PageLimit:       1000,
					BatchSize:       1000,
					WriteBatchSize:  1000,
					Seed:            1,
				}
				report, err := RunLoadTest(ctx, st, &passthroughDecoder{}, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(report.Events), "events/op")
			}
		})
	}
}

func BenchmarkIngestionCapacity_ByBatchSize(b *testing.B) {
	for _, batchSize := range []uint{100, 500, 1000, 2500, 5000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st := &loadtestStore{mockStore: *newMockStore()}
				cfg := LoadTestConfig{
					EventsPerLedger: 100,
					Ledgers:         10,
					PageLimit:       batchSize,
					BatchSize:       batchSize,
					WriteBatchSize:  batchSize,
					Seed:            7,
				}
				report, err := RunLoadTest(ctx, st, &passthroughDecoder{}, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(report.Events), "events/op")
			}
		})
	}
}

func BenchmarkIngestionCapacity_SteadyStateVsCatchUp(b *testing.B) {
	ctx := context.Background()

	b.Run("steady_state_1_ledger_100_events", func(b *testing.B) {
		rawEvents := makeCapacityRPCEvents(100, 10, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			st := newMockStore()
			client := &mockRPC{
				health: rpc.Health{LatestLedger: 101, OldestLedger: 90},
				eventsResps: []rpc.GetEventsResponse{{
					Events:       rawEvents,
					LatestLedger: 101,
				}},
			}
			ing := New(client, st, &passthroughDecoder{}, loadTestLogger(), Options{
				StartLedger:    100,
				PageLimit:      1000,
				BatchSize:      1000,
				WriteBatchSize: 1000,
			})
			if _, err := ing.RunOnceForTest(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(rawEvents)), "events/op")
		}
	})

	b.Run("catch_up_20_ledgers_2000_events", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			st := &loadtestStore{mockStore: *newMockStore()}
			cfg := LoadTestConfig{
				EventsPerLedger: 100,
				Ledgers:         20,
				PageLimit:       1000,
				BatchSize:       1000,
				WriteBatchSize:  1000,
				Seed:            11,
			}
			report, err := RunLoadTest(ctx, st, &passthroughDecoder{}, cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(report.Events), "events/op")
		}
	})
}

func TestIngestionCapacityHarness(t *testing.T) {
	ctx := context.Background()

	// Verify XDR vs JSON passthrough event builders and transform stage
	xdrEvents := makeCapacityRPCEvents(25, 5, true)
	require.Len(t, xdrEvents, 25)
	ingXDR := New(nil, newMockStore(), decode.XDRDecoder{}, loadTestLogger(), Options{StartLedger: 100})
	for _, raw := range xdrEvents {
		ev, err := ingXDR.toStoreEvent(raw)
		require.NoError(t, err)
		assert.NotEmpty(t, ev.ID)
		assert.NotEmpty(t, ev.Topics)
		assert.NotEmpty(t, ev.Value)
	}

	// Verify steady-state vs catch-up load test reports
	st := &loadtestStore{mockStore: *newMockStore()}
	report, err := RunLoadTest(ctx, st, &passthroughDecoder{}, LoadTestConfig{
		EventsPerLedger: 50,
		Ledgers:         10,
		PageLimit:       250,
		BatchSize:       250,
		WriteBatchSize:  250,
		Seed:            99,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(500), report.Events)
	assert.LessOrEqual(t, report.FinalLagLedgers, uint32(1))
	assert.Greater(t, report.EventsPerSecond, 0.0)
}
