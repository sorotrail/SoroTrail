package backfill

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/store"
)

// helperErrorStore reuses fakeStore's state and deduplication behavior while
// allowing focused helper tests to force one persistence operation to fail.
type helperErrorStore struct {
	*fakeStore
	stateErr    error
	startErr    error
	upsertErr   error
	updateErr   error
	completeErr error
}

func (s *helperErrorStore) GetBackfillState(ctx context.Context) (store.BackfillState, error) {
	if s.stateErr != nil {
		return store.BackfillState{}, s.stateErr
	}
	return s.fakeStore.GetBackfillState(ctx)
}

func (s *helperErrorStore) StartBackfillState(ctx context.Context, contractID string, fromLedger, toLedger int64) error {
	if s.startErr != nil {
		return s.startErr
	}
	return s.fakeStore.StartBackfillState(ctx, contractID, fromLedger, toLedger)
}

func (s *helperErrorStore) UpsertEvents(ctx context.Context, events []store.Event) (int64, error) {
	if s.upsertErr != nil {
		return 0, s.upsertErr
	}
	return s.fakeStore.UpsertEvents(ctx, events)
}

func (s *helperErrorStore) UpdateBackfillState(ctx context.Context, lastLedger int64) error {
	if s.updateErr != nil {
		return s.updateErr
	}
	return s.fakeStore.UpdateBackfillState(ctx, lastLedger)
}

func (s *helperErrorStore) CompleteBackfillState(ctx context.Context) error {
	if s.completeErr != nil {
		return s.completeErr
	}
	return s.fakeStore.CompleteBackfillState(ctx)
}

func TestBackfillerFetch(t *testing.T) {
	tests := []struct {
		name          string
		cursor        string
		batchSize     int
		includeFailed bool
	}{
		{name: "passes the initial cursor and options", cursor: "", batchSize: 25, includeFailed: false},
		{name: "passes a continuation cursor and failed flag", cursor: "next-page", batchSize: 50, includeFailed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &fakeHorizon{pageResponses: [][]horizon.Transaction{{{ID: "tx-1"}}}}
			contractID := contractIDFromSeed("fetch")
			b := New(h, &fakeStore{}, decode.XDRDecoder{}, dummyLog(), Options{
				ContractID: contractID, BatchSize: tt.batchSize, IncludeFailed: tt.includeFailed,
			})

			resp, err := b.fetch(context.Background(), tt.cursor)

			require.NoError(t, err)
			assert.Len(t, resp.Embedded.Records, 1)
			assert.Equal(t, contractID, h.lastContract)
			assert.Equal(t, tt.cursor, h.lastCursor)
			assert.Equal(t, tt.batchSize, h.lastLimit)
			assert.Equal(t, tt.includeFailed, h.lastFailed)
		})
	}
}

func TestBackfillerExtractPage(t *testing.T) {
	contractID := contractIDFromSeed("extract")
	meta := buildSimpleMeta(t, "extract", scSymbol("transfer"))

	tests := []struct {
		name             string
		startLedger      int64
		toLedger         int64
		rows             []horizon.Transaction
		wantEvents       int
		wantLastLedger   int64
		wantTransactions int64
		wantSkipped      int64
	}{
		{
			name:        "extracts rows at and above the resume ledger",
			startLedger: 100,
			rows: []horizon.Transaction{
				{Hash: "before", Ledger: 99, ResultMetaXDR: meta},
				{Hash: "at", Ledger: 100, ResultMetaXDR: meta},
				{Hash: "after", Ledger: 101, ResultMetaXDR: meta},
			},
			wantEvents:       2,
			wantLastLedger:   101,
			wantTransactions: 2,
		},
		{
			name:        "counts but does not extract rows above the upper bound",
			startLedger: 100,
			toLedger:    100,
			rows: []horizon.Transaction{
				{Hash: "in-range", Ledger: 100, ResultMetaXDR: meta},
				{Hash: "out-of-range", Ledger: 101, ResultMetaXDR: meta},
			},
			wantEvents:       1,
			wantLastLedger:   101,
			wantTransactions: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New(&fakeHorizon{}, &fakeStore{}, decode.XDRDecoder{}, dummyLog(), Options{
				ContractID: contractID, ToLedger: tt.toLedger,
			})

			events, lastLedger, counters := b.extractPage(tt.rows, tt.startLedger)

			assert.Len(t, events, tt.wantEvents)
			assert.Equal(t, tt.wantLastLedger, lastLedger)
			assert.Equal(t, tt.wantTransactions, counters.Transactions)
			assert.Equal(t, tt.wantSkipped, counters.Skipped)
		})
	}
}

func TestBackfillerCommitPage(t *testing.T) {
	events := []store.Event{{ID: "event-1", Ledger: 123}}
	fs := &fakeStore{}
	b := New(&fakeHorizon{}, fs, decode.XDRDecoder{}, dummyLog(), Options{ContractID: contractIDFromSeed("commit")})

	inserted, err := b.commitPage(context.Background(), events, 123)

	require.NoError(t, err)
	assert.EqualValues(t, 1, inserted)
	assert.Equal(t, []int64{123}, fs.updateLedgers)
	assert.Equal(t, int64(123), fs.state.LastLedger)

	// The same page is safe to commit again: the fake mirrors the database's
	// ON CONFLICT behavior and the persisted position remains unchanged.
	inserted, err = b.commitPage(context.Background(), events, 123)
	require.NoError(t, err)
	assert.Zero(t, inserted)
	assert.Equal(t, []int64{123, 123}, fs.updateLedgers)
	assert.Len(t, fs.rows, 1)
}

func TestBackfillerCommitPage_InterruptedCommitLeavesProgressUnchanged(t *testing.T) {
	updateErr := errors.New("progress store unavailable")
	fs := &helperErrorStore{fakeStore: &fakeStore{}, updateErr: updateErr}
	b := New(&fakeHorizon{}, fs, decode.XDRDecoder{}, dummyLog(), Options{ContractID: contractIDFromSeed("commit-error")})

	inserted, err := b.commitPage(context.Background(), []store.Event{{ID: "event-1", Ledger: 123}}, 123)

	require.Error(t, err)
	assert.ErrorIs(t, err, updateErr)
	// commitPage reports what the upsert actually wrote and only then fails on
	// the progress write, so the count is the one row the fake confirms below.
	// Callers discard it because they return on the error.
	assert.EqualValues(t, 1, inserted)
	assert.Len(t, fs.rows, 1, "the upsert completed before the progress write failed")
	assert.Zero(t, fs.state.LastLedger, "failed progress must not claim the page")
}

func TestBackfillerMarkTerminal(t *testing.T) {
	completeErr := errors.New("completion store unavailable")
	tests := []struct {
		name          string
		sum           Summary
		completeErr   error
		wantCompleted bool
		wantErr       error
	}{
		{name: "completed run marks state terminal", sum: Summary{Completed: true}, wantCompleted: true},
		{name: "interrupted run leaves state resumable", sum: Summary{}, wantCompleted: false},
		{name: "terminal failure is returned", sum: Summary{Completed: true}, completeErr: completeErr, wantErr: completeErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &helperErrorStore{fakeStore: &fakeStore{}, completeErr: tt.completeErr}
			b := New(&fakeHorizon{}, fs, decode.XDRDecoder{}, dummyLog(), Options{ContractID: contractIDFromSeed("terminal")})

			err := b.markTerminal(context.Background(), tt.sum)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCompleted, fs.completeCalled)
		})
	}
}

func TestBackfillerResume(t *testing.T) {
	contractID := contractIDFromSeed("resume")
	tests := []struct {
		name          string
		state         store.BackfillState
		stateErr      error
		wantStart     int64
		wantResumed   bool
		wantStartCall bool
	}{
		{
			name:      "matching unfinished state resumes after persisted ledger",
			state:     store.BackfillState{ContractID: contractID, FromLedger: 100, ToLedger: 200, LastLedger: 149},
			wantStart: 149 + 1, wantResumed: true,
		},
		{
			name:      "missing state starts a fresh run",
			stateErr:  store.ErrNotFound,
			wantStart: 100, wantStartCall: true,
		},
		{
			name:      "different bounds start a fresh run",
			state:     store.BackfillState{ContractID: contractID, FromLedger: 101, ToLedger: 200, LastLedger: 149},
			wantStart: 100, wantStartCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &helperErrorStore{fakeStore: &fakeStore{state: tt.state}, stateErr: tt.stateErr}
			b := New(&fakeHorizon{}, fs, decode.XDRDecoder{}, dummyLog(), Options{
				ContractID: contractID, FromLedger: 100, ToLedger: 200,
			})

			start, sum, err := b.resume(context.Background())

			require.NoError(t, err)
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantResumed, sum.Resumed)
			assert.Equal(t, tt.wantStartCall, fs.startCalled)
		})
	}
}

func TestBackfillerResume_StoreFailureIsNotRetried(t *testing.T) {
	storeErr := errors.New("state lookup failed")
	fs := &helperErrorStore{fakeStore: &fakeStore{}, stateErr: storeErr}
	b := New(&fakeHorizon{}, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: contractIDFromSeed("resume-error"), FromLedger: 100,
	})

	_, _, err := b.resume(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, storeErr)
	assert.False(t, fs.startCalled, "a state lookup failure must not be treated as a missing row")
}

var _ = time.Second
