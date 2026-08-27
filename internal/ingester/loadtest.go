package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// LoadTestConfig controls a synthetic ingestion run.
type LoadTestConfig struct {
	// EventsPerLedger is the number of events emitted for each ledger.
	EventsPerLedger int
	// Ledgers is the number of ledgers made available to the ingester.
	Ledgers uint32
	// PageLimit controls synthetic RPC pagination.
	PageLimit uint
	// Duration optionally stops the run after this duration. Zero runs until caught up.
	Duration time.Duration
}

// LoadTestReport summarizes a synthetic ingestion run.
type LoadTestReport struct {
	Events          int64
	Duration        time.Duration
	EventsPerSecond float64
	MaxLagLedgers   uint32
	FinalLagLedgers uint32
}

// RunLoadTest drives the ingester against a configurable in-memory RPC.
// It is intended for local soak tests and never contacts a network or database.
func RunLoadTest(ctx context.Context, st StoreForLoadTest, dec DecoderForLoadTest, cfg LoadTestConfig) (LoadTestReport, error) {
	if cfg.EventsPerLedger < 1 || cfg.Ledgers < 1 {
		return LoadTestReport{}, fmt.Errorf("events per ledger and ledgers must be positive")
	}
	if cfg.PageLimit == 0 {
		cfg.PageLimit = 1000
	}
	client := NewSyntheticRPC(cfg)
	start := time.Now()
	logCtx := ctx
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		logCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}
	ing := New(client, st, dec, loadTestLogger(), Options{StartLedger: 2, PageLimit: cfg.PageLimit})
	var total int64
	var maxLag uint32
	for {
		before, err := st.LoadTestEventCount(logCtx)
		if err != nil {
			return LoadTestReport{}, err
		}
		caughtUp, err := ing.RunOnceForTest(logCtx)
		if err != nil {
			if logCtx.Err() != nil {
				break
			}
			return LoadTestReport{}, err
		}
		after, err := st.LoadTestEventCount(logCtx)
		if err != nil {
			return LoadTestReport{}, err
		}
		if after > before {
			atomic.StoreInt64(&total, after)
		}
		state, err := st.LoadTestState(logCtx)
		if err == nil {
			lag := client.LatestLedger() - uint32(maxInt64(state.LastIngestedLedger, 0))
			if lag > maxLag {
				maxLag = lag
			}
			if caughtUp {
				d := time.Since(start)
				return LoadTestReport{Events: total, Duration: d, EventsPerSecond: float64(total) / d.Seconds(), MaxLagLedgers: maxLag, FinalLagLedgers: lag}, nil
			}
		}
	}
	d := time.Since(start)
	return LoadTestReport{Events: total, Duration: d, EventsPerSecond: float64(total) / maxFloat(d.Seconds(), 1), MaxLagLedgers: maxLag}, logCtx.Err()
}

func maxInt64(v, floor int64) int64 {
	if v > floor {
		return v
	}
	return floor
}
func maxFloat(v, floor float64) float64 {
	if v > floor {
		return v
	}
	return floor
}

// StoreForLoadTest is the small store surface needed by RunLoadTest.
type StoreForLoadTest interface {
	store.Store
	LoadTestEventCount(context.Context) (int64, error)
	LoadTestState(context.Context) (store.IngestionState, error)
}

// DecoderForLoadTest aliases the decoder dependency without exposing test-only plumbing.
type DecoderForLoadTest interface {
	DecodeScVal(string) (json.RawMessage, error)
}

// SyntheticRPC emits deterministic events and records no external state.
type SyntheticRPC struct {
	mu     sync.Mutex
	cfg    LoadTestConfig
	latest uint32
	events []rpc.Event
}

func NewSyntheticRPC(cfg LoadTestConfig) *SyntheticRPC {
	r := &SyntheticRPC{cfg: cfg, latest: cfg.Ledgers + 1}
	for ledger := uint32(2); ledger <= cfg.Ledgers+1; ledger++ {
		for n := 0; n < cfg.EventsPerLedger; n++ {
			r.events = append(r.events, rpc.Event{ID: fmt.Sprintf("%010d-%09d", ledger, n), Ledger: ledger, Type: "contract", ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ValueJSON: json.RawMessage(`{"u64":1}`)})
		}
	}
	return r
}
func (r *SyntheticRPC) LatestLedger() uint32 { return r.latest }
func (r *SyntheticRPC) GetHealth(context.Context) (rpc.Health, error) {
	return rpc.Health{LatestLedger: r.latest, OldestLedger: 2}, nil
}
func (r *SyntheticRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{Sequence: r.latest}, nil
}
func (r *SyntheticRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}
func (r *SyntheticRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}
func (r *SyntheticRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	start := req.StartLedger
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		start = 0
	}
	idx := 0
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		for idx < len(r.events) && r.events[idx].ID <= req.Pagination.Cursor {
			idx++
		}
	}
	for idx < len(r.events) && start > 0 && r.events[idx].Ledger < start {
		idx++
	}
	limit := cfgLimit(req, r.cfg.PageLimit)
	end := idx + limit
	if end > len(r.events) {
		end = len(r.events)
	}
	page := append([]rpc.Event(nil), r.events[idx:end]...)
	cursor := ""
	if len(page) > 0 {
		cursor = page[len(page)-1].ID
	}
	return rpc.GetEventsResponse{Events: page, LatestLedger: r.latest, Cursor: cursor}, nil
}
func cfgLimit(req rpc.GetEventsRequest, fallback uint) int {
	if req.Pagination != nil && req.Pagination.Limit > 0 {
		return int(req.Pagination.Limit)
	}
	return int(fallback)
}
func loadTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
