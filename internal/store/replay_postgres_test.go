package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Raw XDR survives a round trip, and rows without it come back as "no raw
// XDR" rather than as empty strings that replay would try to decode.
func TestPostgres_RawXDRRoundTrip(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	withXDR := testEvent(eventID(1), 100, contractA)
	withXDR.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg==", "AAAAEAAAAA=="}
	withXDR.RawValueXDR = "AAAACgAAAAAAAAAB"

	legacy := testEvent(eventID(2), 100, contractA) // ingested before raw XDR

	_, err := p.UpsertEvents(ctx, []Event{withXDR, legacy})
	require.NoError(t, err)

	got, err := p.GetEvent(ctx, withXDR.ID, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, withXDR.RawTopicXDR, got.RawTopicXDR)
	assert.Equal(t, withXDR.RawValueXDR, got.RawValueXDR)

	gotLegacy, err := p.GetEvent(ctx, legacy.ID, SystemScope())
	require.NoError(t, err)
	assert.Empty(t, gotLegacy.RawTopicXDR)
	assert.Empty(t, gotLegacy.RawValueXDR)

	batch, err := p.NextReplayBatch(ctx, 1, 1000, "", 10)
	require.NoError(t, err)
	require.Len(t, batch, 2)
	assert.True(t, batch[0].HasRawXDR())
	assert.False(t, batch[1].HasRawXDR(), "legacy row is replay-skippable, not an error")
}

// Auditor repair must not cost a row its replayability: ReplaceEventsInRange
// carries raw XDR through, and never blanks out XDR already stored when the
// repair fetch itself came back without any.
func TestPostgres_ReplaceEventsInRangeKeepsRawXDR(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	original := testEvent(eventID(1), 100, contractA)
	original.RawTopicXDR = []string{"AAAADwAAAAh0cmFuc2Zlcg=="}
	original.RawValueXDR = "AAAACgAAAAAAAAAB"
	_, err := p.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)

	// A repair that re-fetched the XDR refreshes it.
	repaired := original
	repaired.RawValueXDR = "AAAACgAAAAAAAAAC"
	require.NoError(t, p.ReplaceEventsInRange(ctx, []Event{repaired}, 100, 100))

	got, err := p.GetEvent(ctx, original.ID, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, "AAAACgAAAAAAAAAC", got.RawValueXDR)

	// A repair whose fetch carried no XDR (the RPC answered with JSON) must
	// leave the stored XDR alone rather than destroying it.
	noXDR := original
	noXDR.RawTopicXDR, noXDR.RawValueXDR = nil, ""
	require.NoError(t, p.ReplaceEventsInRange(ctx, []Event{noXDR}, 100, 100))

	got, err = p.GetEvent(ctx, original.ID, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, []string{"AAAADwAAAAh0cmFuc2Zlcg=="}, got.RawTopicXDR,
		"a JSON-only repair must not strip stored raw XDR")
	assert.Equal(t, "AAAACgAAAAAAAAAC", got.RawValueXDR)
}

// NextReplayBatch pages the ledger range in ID order and respects its bounds.
func TestPostgres_NextReplayBatchPaging(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	var events []Event
	for i := 1; i <= 5; i++ {
		events = append(events, testEvent(eventID(i), int64(100+i), contractA))
	}
	events = append(events, testEvent(eventID(9), 5000, contractA)) // out of range
	_, err := p.UpsertEvents(ctx, events)
	require.NoError(t, err)

	var seen []string
	cursor := ""
	for {
		batch, err := p.NextReplayBatch(ctx, 101, 105, cursor, 2)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			seen = append(seen, e.ID)
		}
		cursor = batch[len(batch)-1].ID
	}
	assert.Equal(t, []string{eventID(1), eventID(2), eventID(3), eventID(4), eventID(5)}, seen)
}

// A batch commit rewrites the decoded columns and advances progress together.
func TestPostgres_CommitReplayBatch(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	e := testEvent(eventID(1), 100, contractA)
	_, err := p.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	require.NoError(t, p.StartReplayState(ctx, 1, 1000))

	err = p.CommitReplayBatch(ctx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: json.RawMessage(`[{"symbol":"mint"}]`),
			Value:  json.RawMessage(`{"i128":"42"}`),
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 1, Changed: 1},
	})
	require.NoError(t, err)

	got, err := p.GetEvent(ctx, e.ID, SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"mint"}]`, string(got.Topics))
	assert.JSONEq(t, `{"i128":"42"}`, string(got.Value))

	state, err := p.GetReplayState(ctx)
	require.NoError(t, err)
	assert.Equal(t, e.ID, state.LastEventID)
	assert.EqualValues(t, 1, state.Processed)
	assert.EqualValues(t, 1, state.Changed)
	assert.False(t, state.Done(), "no completion marker until the range finishes")
}

// StartReplayState resets a previous run's progress rather than accumulating
// onto it.
func TestPostgres_StartReplayStateResets(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	_, err := p.GetReplayState(ctx)
	assert.ErrorIs(t, err, ErrNotFound, "no state before the first replay")

	require.NoError(t, p.StartReplayState(ctx, 1, 100))
	require.NoError(t, p.CommitReplayBatch(ctx, ReplayBatch{
		State: ReplayState{LastEventID: eventID(7), Processed: 7, Skipped: 2},
	}))

	require.NoError(t, p.StartReplayState(ctx, 200, 300))
	state, err := p.GetReplayState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "", state.LastEventID)
	assert.EqualValues(t, 0, state.Processed)
	assert.EqualValues(t, 0, state.Skipped)
	assert.EqualValues(t, 200, state.FromLedger)
	assert.EqualValues(t, 300, state.ToLedger)
}

// The concurrent-replay guard: the advisory lock is exclusive across
// sessions, and releasing it makes it immediately available again.
func TestPostgres_ReplayAdvisoryLockIsExclusive(t *testing.T) {
	p := testStore(t)
	ctx := context.Background()

	first, err := p.AcquireReplayLock(ctx)
	require.NoError(t, err)

	_, err = p.AcquireReplayLock(ctx)
	assert.ErrorIs(t, err, ErrReplayLocked, "a second replay must be refused, not queued")

	first.Release()

	second, err := p.AcquireReplayLock(ctx)
	require.NoError(t, err, "lock is available again once released")
	second.Release()
	second.Release() // idempotent
}
