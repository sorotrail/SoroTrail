package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type recordingReorgNotifier struct {
	mu      sync.Mutex
	notices []ReorgRetractionNotice
}

func (r *recordingReorgNotifier) NotifyReorgRetraction(_ context.Context, notice ReorgRetractionNotice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notices = append(r.notices, notice)
}

func (r *recordingReorgNotifier) snapshot() []ReorgRetractionNotice {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ReorgRetractionNotice, len(r.notices))
	copy(out, r.notices)
	return out
}

func TestReorgTracker_LedgerHashWindowAndDepthLimit(t *testing.T) {
	rt := NewReorgTracker(4)
	assert.Equal(t, uint32(4), rt.Window())

	mismatch, prev := rt.RecordLedgerHash(10, "hash-10-a")
	assert.False(t, mismatch)
	assert.Empty(t, prev)

	// Re-recording the identical hash is a no-op.
	mismatch, _ = rt.RecordLedgerHash(10, "hash-10-a")
	assert.False(t, mismatch)

	// Recording a different hash for ledger 10 flags a reorg mismatch.
	mismatch, prev = rt.RecordLedgerHash(10, "hash-10-b")
	assert.True(t, mismatch)
	assert.Equal(t, "hash-10-a", prev)

	// Advance window to ledger 15; ledger 10 (10 < 15-4=11) is pruned.
	rt.RecordLedgerHash(12, "hash-12")
	rt.RecordLedgerHash(15, "hash-15")
	_, ok := rt.LookupLedgerHash(10)
	assert.False(t, ok, "ledger outside confirmation window must be pruned")
	h12, ok := rt.LookupLedgerHash(12)
	assert.True(t, ok)
	assert.Equal(t, "hash-12", h12)

	// Depth evaluation within window (depth 3 <= 4).
	depth, ratio, exceeded := rt.EvaluateDepth(13, 15, 15)
	assert.Equal(t, uint32(3), depth)
	assert.InDelta(t, 0.75, ratio, 0.001)
	assert.False(t, exceeded)

	// Depth evaluation exceeding window (depth 5 > 4).
	depth, ratio, exceeded = rt.EvaluateDepth(11, 15, 15)
	assert.Equal(t, uint32(5), depth)
	assert.InDelta(t, 1.25, ratio, 0.001)
	assert.True(t, exceeded)
}

func TestIngester_HandleReorgRange_DepthExceededAlarm(t *testing.T) {
	st := newMockStore()
	require.NoError(t, st.SaveIngestionState(context.Background(), store.IngestionState{
		LastIngestedLedger: 100,
	}))
	st.events["e90"] = store.Event{ID: "e90", ContractID: "C1", Ledger: 90, Type: "contract"}

	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 100, OldestLedger: 1},
	}
	ing := newTestIngester(client, st, Options{
		ReorgConfirmationWindow: 10,
	})

	beforeExceeded := testutil.ToFloat64(ReorgWindowExceededTotal)
	_, err := ing.HandleReorgRange(context.Background(), client, 85, 100, 100)
	require.ErrorIs(t, err, ErrReorgDepthExceeded)
	assert.Equal(t, beforeExceeded+1, testutil.ToFloat64(ReorgWindowExceededTotal))
	assert.Contains(t, st.events, "e90", "exceeded-window reorg must abort before mutating stored events")
}

func TestIngester_HandleReorgRange_ArchiveBoundaryAlarm(t *testing.T) {
	st := newMockStore()
	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 200, OldestLedger: 1},
	}
	ing := newTestIngester(client, st, Options{
		ReorgConfirmationWindow: 64,
	})
	ing.SetArchivedThroughLedger(120)

	beforeArchived := testutil.ToFloat64(ReorgArchivedBoundaryViolationsTotal)
	_, err := ing.HandleReorgRange(context.Background(), client, 115, 125, 200)
	require.ErrorIs(t, err, ErrReorgTouchesArchivedRange)
	assert.Equal(t, beforeArchived+1, testutil.ToFloat64(ReorgArchivedBoundaryViolationsTotal))
}

func TestIngester_HandleReorgRange_RetractionNoticeAndMultiBatchSafety(t *testing.T) {
	ctx := context.Background()
	st := newMockStore()
	// Watch 30 contracts so BuildFilterBatches produces 2 separate RPC filter batches (>25).
	for i := 0; i < 30; i++ {
		st.watched = append(st.watched, store.WatchedContract{
			ContractID: fmt.Sprintf("C%055d", i),
		})
	}
	contractBatch1 := fmt.Sprintf("C%055d", 0)
	contractBatch2 := fmt.Sprintf("C%055d", 26)

	// Seed existing events in [100, 102]:
	// - e100-b1 (batch 1, unchanged)
	// - e101-b1-retracted (batch 1, retracted by reorg)
	// - e102-b2 (batch 2, modified payload by reorg)
	st.events["e100-b1"] = store.Event{
		ID: "e100-b1", ContractID: contractBatch1, Ledger: 100, Type: "contract",
		TxHash: "tx-100", Topics: json.RawMessage(`[{"symbol":"keep"}]`), Value: json.RawMessage(`{"u64":1}`),
	}
	st.events["e101-b1-retracted"] = store.Event{
		ID: "e101-b1-retracted", ContractID: contractBatch1, Ledger: 101, Type: "contract",
		TxHash: "tx-101-old", Topics: json.RawMessage(`[{"symbol":"drop"}]`), Value: json.RawMessage(`{"u64":2}`),
	}
	st.events["e102-b2"] = store.Event{
		ID: "e102-b2", ContractID: contractBatch2, Ledger: 102, Type: "contract",
		TxHash: "tx-102-old", Topics: json.RawMessage(`[{"symbol":"mod"}]`), Value: json.RawMessage(`{"u64":3}`),
	}

	// QueryEvents on ingester mockStore needs to return st.events in range so
	// ReconcileReorgRange can snapshot existing events. Let's wrap mockStore.
	reorgStore := &queryableMockStore{mockStore: st}

	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 105, OldestLedger: 1},
		eventsResps: []rpc.GetEventsResponse{
			// Batch 1 response: e100-b1 survives, e101-b1-retracted is gone.
			{
				Events: []rpc.Event{
					{
						ID: "e100-b1", ContractID: contractBatch1, Ledger: 100, Type: "contract",
						TxHash: "tx-100", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"keep"}`)},
						ValueJSON: json.RawMessage(`{"u64":1}`),
					},
				},
				LatestLedger: 105,
			},
			// Batch 2 response: e102-b2 has updated value, plus e102-b2-new.
			{
				Events: []rpc.Event{
					{
						ID: "e102-b2", ContractID: contractBatch2, Ledger: 102, Type: "contract",
						TxHash: "tx-102-new", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"mod"}`)},
						ValueJSON: json.RawMessage(`{"u64":300}`),
					},
					{
						ID: "e102-b2-new", ContractID: contractBatch2, Ledger: 102, Type: "contract",
						TxHash: "tx-102-new2", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"new"}`)},
						ValueJSON: json.RawMessage(`{"u64":400}`),
					},
				},
				LatestLedger: 105,
			},
		},
	}

	ing := newTestIngester(client, reorgStore, Options{
		ReorgConfirmationWindow: 32,
		PageLimit:               100,
	})
	notifier := &recordingReorgNotifier{}
	ing.SetReorgNotifier(notifier)

	res, err := ing.HandleReorgRange(ctx, client, 100, 102, 105)
	require.NoError(t, err)

	// Both batches' canonical events must be present in the store!
	assert.Contains(t, st.events, "e100-b1", "batch 1 event must not be wiped by batch 2")
	assert.Contains(t, st.events, "e102-b2")
	assert.Contains(t, st.events, "e102-b2-new")
	assert.NotContains(t, st.events, "e101-b1-retracted")

	// Retracted IDs: e101-b1-retracted (deleted) and e102-b2 (modified tx/value).
	assert.Equal(t, []string{"e101-b1-retracted", "e102-b2"}, res.RetractedEventIDs)

	notices := notifier.snapshot()
	require.Len(t, notices, 1)
	assert.Equal(t, uint32(100), notices[0].FromLedger)
	assert.Equal(t, uint32(102), notices[0].ToLedger)
	assert.Equal(t, []string{"e101-b1-retracted", "e102-b2"}, notices[0].RetractedEventIDs)
	assert.Equal(t, []string{"e100-b1", "e102-b2", "e102-b2-new"}, notices[0].ReplacementEventIDs)
}

func TestIngester_HandleReorgRange_ConcurrentOverlappingReorgCoalescing(t *testing.T) {
	ctx := context.Background()
	st := &queryableMockStore{mockStore: newMockStore()}
	st.events["e100-old"] = store.Event{ID: "e100-old", ContractID: "C1", Ledger: 100, Type: "contract"}
	st.events["e102-old"] = store.Event{ID: "e102-old", ContractID: "C1", Ledger: 102, Type: "contract"}

	firstCallEntered := make(chan struct{})
	releaseFirstCall := make(chan struct{})
	var callCount int
	var mu sync.Mutex

	blockingClient := &callbackRPC{
		getEventsFn: func(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
			mu.Lock()
			callCount++
			c := callCount
			mu.Unlock()

			if c == 1 {
				close(firstCallEntered)
				<-releaseFirstCall
				return rpc.GetEventsResponse{
					Events: []rpc.Event{
						{
							ID: "e100-new", ContractID: "C1", Ledger: 100, Type: "contract",
							TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"a"}`)},
							ValueJSON: json.RawMessage(`{"u64":1}`),
						},
					},
					LatestLedger: 105,
				}, nil
			}
			return rpc.GetEventsResponse{
				Events: []rpc.Event{
					{
						ID: "e100-new", ContractID: "C1", Ledger: 100, Type: "contract",
						TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"a"}`)},
						ValueJSON: json.RawMessage(`{"u64":1}`),
					},
					{
						ID: "e103-new", ContractID: "C1", Ledger: 103, Type: "contract",
						TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"b"}`)},
						ValueJSON: json.RawMessage(`{"u64":2}`),
					},
				},
				LatestLedger: 105,
			}, nil
		},
	}

	ing := newTestIngester(blockingClient, st, Options{
		ReorgConfirmationWindow: 32,
		PageLimit:               100,
	})

	var wg sync.WaitGroup
	var res1, res2 store.ReorgReconcileResult
	var err1, err2 error

	wg.Add(1)
	go func() {
		defer wg.Done()
		res1, err1 = ing.HandleReorgRange(ctx, blockingClient, 100, 101, 105)
	}()

	<-firstCallEntered

	wg.Add(1)
	go func() {
		defer wg.Done()
		res2, err2 = ing.HandleReorgRange(ctx, blockingClient, 101, 103, 105)
	}()

	// Wait until the second caller has registered in pending before releasing first call.
	for {
		ing.reorgCoord.mu.Lock()
		hasPending := ing.reorgCoord.pending
		ing.reorgCoord.mu.Unlock()
		if hasPending {
			break
		}
	}
	close(releaseFirstCall)
	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, res1.RetractedEventIDs, res2.RetractedEventIDs)
	assert.Contains(t, st.events, "e100-new")
	assert.Contains(t, st.events, "e103-new")
	assert.NotContains(t, st.events, "e100-old")
	assert.NotContains(t, st.events, "e102-old")
}

type queryableMockStore struct {
	*mockStore
}

func (q *queryableMockStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []store.Event
	for _, ev := range q.events {
		if f.FromLedger > 0 && ev.Ledger < f.FromLedger {
			continue
		}
		if f.ToLedger > 0 && ev.Ledger > f.ToLedger {
			continue
		}
		if f.Cursor != "" && ev.ID <= f.Cursor {
			continue
		}
		out = append(out, ev)
	}
	return out, "", nil
}

type callbackRPC struct {
	mockRPC
	getEventsFn func(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error)
}

func (c *callbackRPC) GetEvents(ctx context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return c.getEventsFn(ctx, req)
}

func TestIngester_PollLoop_AutoReconcilesTipRegressionAndFingerprintDivergence(t *testing.T) {
	ctx := context.Background()
	st := &queryableMockStore{mockStore: newMockStore()}

	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 50, OldestLedger: 1},
		eventsResps: []rpc.GetEventsResponse{
			// Cycle 1: ingest ledgers 48, 49, 50 at chain tip 50.
			{
				Events: []rpc.Event{
					{
						ID: "e48-orig", ContractID: "C1", Ledger: 48, Type: "contract",
						TxHash: "tx48", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"a"}`)},
						ValueJSON: json.RawMessage(`{"u64":48}`),
					},
					{
						ID: "e49-orig", ContractID: "C1", Ledger: 49, Type: "contract",
						TxHash: "tx49", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"b"}`)},
						ValueJSON: json.RawMessage(`{"u64":49}`),
					},
					{
						ID: "e50-orig", ContractID: "C1", Ledger: 50, Type: "contract",
						TxHash: "tx50", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"c"}`)},
						ValueJSON: json.RawMessage(`{"u64":50}`),
					},
				},
				LatestLedger: 50,
			},
			// Cycle 2 (poll): RPC reports LatestLedger=48 (tip regressed from 50 -> 48!).
			{
				Events:       nil,
				LatestLedger: 48,
			},
			// Cycle 2 (HandleReorgRange fetch for [49, 50] with newHead=48): empty above 48.
			{
				Events:       nil,
				LatestLedger: 48,
			},
			// Cycle 3 (poll after rewinding cursor to 47): re-observes ledger 48 with a different payload!
			{
				Events: []rpc.Event{
					{
						ID: "e48-fork", ContractID: "C1", Ledger: 48, Type: "contract",
						TxHash: "tx48-fork", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"fork"}`)},
						ValueJSON: json.RawMessage(`{"u64":4800}`),
					},
				},
				LatestLedger: 48,
			},
			// Cycle 3 (HandleReorgRange fetch for [48, 48] triggered by fingerprint divergence):
			{
				Events: []rpc.Event{
					{
						ID: "e48-fork", ContractID: "C1", Ledger: 48, Type: "contract",
						TxHash: "tx48-fork", TopicJSON: []json.RawMessage{json.RawMessage(`{"symbol":"fork"}`)},
						ValueJSON: json.RawMessage(`{"u64":4800}`),
					},
				},
				LatestLedger: 48,
			},
		},
	}

	ing := newTestIngester(client, st, Options{
		StartLedger:             48,
		PageLimit:               100,
		ReorgConfirmationWindow: DefaultReorgConfirmationWindow,
	})
	notifier := &recordingReorgNotifier{}
	ing.SetReorgNotifier(notifier)

	// Cycle 1: normal ingest of 48..50.
	caughtUp, err := ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	assert.True(t, caughtUp)
	assert.Contains(t, st.events, "e48-orig")
	assert.Contains(t, st.events, "e49-orig")
	assert.Contains(t, st.events, "e50-orig")

	// Cycle 2: tip regression (LatestLedger=48 < 50) automatically reconciles [49, 50] and rewinds state to 48.
	caughtUp, err = ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	assert.True(t, caughtUp)
	assert.Contains(t, st.events, "e48-orig")
	assert.NotContains(t, st.events, "e49-orig", "tip regression in poll loop must retract e49-orig")
	assert.NotContains(t, st.events, "e50-orig", "tip regression in poll loop must retract e50-orig")

	ingState, err := st.GetIngestionState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(48), ingState.LastIngestedLedger)

	// Rewind cursor to 47 so Cycle 3 re-polls ledger 48 and observes fingerprint divergence.
	require.NoError(t, st.SaveIngestionState(ctx, store.IngestionState{LastIngestedLedger: 47}))
	caughtUp, err = ing.RunOnceForTest(ctx)
	require.NoError(t, err)
	assert.True(t, caughtUp)
	assert.NotContains(t, st.events, "e48-orig", "fingerprint divergence in poll loop must retract e48-orig")
	assert.Contains(t, st.events, "e48-fork", "fingerprint divergence in poll loop must persist e48-fork")
	assert.GreaterOrEqual(t, len(notifier.snapshot()), 2)
}

func TestIngester_HandleReorgRange_OutOfRangeAbortsWithoutDeletingStoredEvents(t *testing.T) {
	ctx := context.Background()
	st := &queryableMockStore{mockStore: newMockStore()}
	st.events["e100-keep"] = store.Event{
		ID: "e100-keep", ContractID: "C1", Ledger: 100, Type: "contract",
		TxHash: "tx100", Topics: json.RawMessage(`[{"symbol":"keep"}]`), Value: json.RawMessage(`{"u64":100}`),
	}

	client := &mockRPC{
		health: rpc.Health{Status: "healthy", LatestLedger: 200, OldestLedger: 150},
		eventsErrs: []error{
			&rpc.Error{Code: -32600, Message: "startLedger must be within the ledger range: 150 - 200"},
		},
	}

	ing := newTestIngester(client, st, Options{
		ReorgConfirmationWindow: 120,
		PageLimit:               100,
	})
	notifier := &recordingReorgNotifier{}
	ing.SetReorgNotifier(notifier)

	_, err := ing.HandleReorgRange(ctx, client, 100, 105, 200)
	require.Error(t, err, "out-of-range RPC error must abort HandleReorgRange with an error")
	assert.True(t, rpc.IsLedgerOutOfRange(err))
	assert.Contains(t, st.events, "e100-keep", "stored events must not be deleted when RPC returns out-of-range")
	assert.Empty(t, notifier.snapshot())
}
