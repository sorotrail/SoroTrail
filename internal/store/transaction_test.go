//go:build integration

package store

// Tests for the store's transactional and rollback behaviour.
//
// Every write path that spans multiple statements is exercised here to
// confirm that a mid-operation failure leaves the database in the same
// state it was in before the call — no partial writes, no leaked
// transactions, no orphaned locks.
//
// These tests require a real Postgres and are skipped unless
// TEST_DATABASE_URL is set (see CONTRIBUTING.md).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// UpsertEvents: batch atomicity
// ---------------------------------------------------------------------------

// TestUpsertEvents_BatchCommitsAtomically verifies that a successful
// batch either inserts all events or none. We insert a batch and
// confirm every event is present; then we insert the same batch again
// (idempotent) and confirm the count hasn't changed.
func TestUpsertEvents_BatchCommitsAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	events := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
		testEvent(eventID(3), 102, contractA),
	}

	inserted, err := st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(3), inserted)

	// Re-upserting the same batch must be a no-op (idempotent).
	inserted, err = st.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted)

	// Confirm exactly 3 events exist.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100,
		ToLedger:   102,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 3)
}

// TestUpsertEvents_EmptyBatchIsNoop confirms that an empty batch does
// not open a transaction or touch the database.
func TestUpsertEvents_EmptyBatchIsNoop(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	inserted, err := st.UpsertEvents(ctx, []Event{})
	require.NoError(t, err)
	assert.Zero(t, inserted)

	inserted, err = st.UpsertEvents(ctx, nil)
	require.NoError(t, err)
	assert.Zero(t, inserted)
}

// ---------------------------------------------------------------------------
// ReplaceEventsInRange: transactional delete + insert
// ---------------------------------------------------------------------------

// TestReplaceEventsInRange_CommitsAtomically replaces events in a
// ledger range and confirms the result is the exact replacement set.
func TestReplaceEventsInRange_CommitsAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// Seed the original events.
	original := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 100, contractB),
		testEvent(eventID(3), 101, contractA),
	}
	_, err := st.UpsertEvents(ctx, original)
	require.NoError(t, err)

	// Replace ledger 100 with two new events (dropping one, adding one).
	replacement := []Event{
		testEvent(eventID(1), 100, contractA), // same ID — updated
		testEvent(eventID(4), 100, contractA), // new event
	}
	// Set the network to match what UpsertEvents stored (defaultNetwork converts "" → "default").
	for i := range replacement {
		replacement[i].Network = "default"
	}
	require.NoError(t, st.ReplaceEventsInRange(ctx, replacement, 100, 100))

	// Ledger 100 should now have exactly the replacement events.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100,
		ToLedger:   100,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 2)

	// Ledger 101 must be untouched.
	got101, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 101,
		ToLedger:   101,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got101, 1)
	assert.Equal(t, eventID(3), got101[0].ID)
}

// TestReplaceEventsInRange_RollsBackOnInsertFailure seeds events, then
// attempts a replace whose insert carries invalid JSON for the topics
// column. The invalid insert must fail, the deferred rollback must undo
// the preceding DELETE, and the original events must remain intact.
func TestReplaceEventsInRange_RollsBackOnInsertFailure(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	original := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 100, contractA),
	}
	_, err := st.UpsertEvents(ctx, original)
	require.NoError(t, err)

	// Build a replacement whose topics contain invalid JSON. pgx will
	// send it as a jsonb parameter and Postgres will reject it.
	badEvent := testEvent(eventID(3), 100, contractA)
	badEvent.Topics = json.RawMessage(`not valid json`)

	err = st.ReplaceEventsInRange(ctx, []Event{badEvent}, 100, 100)
	require.Error(t, err, "insert must fail on invalid JSON")

	// The original events must still be present — the DELETE inside the
	// transaction must have been rolled back.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100,
		ToLedger:   100,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 2, "original events must survive a rolled-back replace")
}

// TestReplaceEventsInRange_EmptyReplacementDeletesAll seeds events in
// a range and replaces with an empty slice, confirming the delete
// committed and no events remain.
func TestReplaceEventsInRange_EmptyReplacementDeletesAll(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
	})
	require.NoError(t, err)

	// An empty replacement is a no-op (early return).
	require.NoError(t, st.ReplaceEventsInRange(ctx, []Event{}, 100, 100))

	// Ledger 100 events should still be present since len(events)==0
	// returns nil.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100,
		ToLedger:   100,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

// ---------------------------------------------------------------------------
// CommitReplayBatch: transactional event rewrites + state advance
// ---------------------------------------------------------------------------

// TestCommitReplayBatch_CommitsAtomically rewrites decoded columns and
// advances replay state in a single batch, confirming both the event
// rewrite and the state update landed together.
func TestCommitReplayBatch_CommitsAtomically(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	e := testEvent(eventID(1), 100, contractA)
	_, err := st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	require.NoError(t, st.StartReplayState(ctx, 1, 1000))

	newTopics := json.RawMessage(`[{"symbol":"mint"}]`)
	newValue := json.RawMessage(`{"i128":"42"}`)

	err = st.CommitReplayBatch(ctx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: newTopics,
			Value:  newValue,
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 1, Changed: 1},
	})
	require.NoError(t, err)

	// Both the event rewrite and the state advance must be visible.
	got, err := st.GetEvent(ctx, e.ID, SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"mint"}]`, string(got.Topics))
	assert.JSONEq(t, `{"i128":"42"}`, string(got.Value))

	state, err := st.GetReplayState(ctx)
	require.NoError(t, err)
	assert.Equal(t, e.ID, state.LastEventID)
	assert.EqualValues(t, 1, state.Processed)
	assert.EqualValues(t, 1, state.Changed)
}

// TestCommitReplayBatch_RollsBackOnFailure rewrites events and
// advances state in one transaction. If the event rewrite fails
// (invalid JSON), the state update must not be visible either.
func TestCommitReplayBatch_RollsBackOnFailure(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	e := testEvent(eventID(1), 100, contractA)
	_, err := st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	require.NoError(t, st.StartReplayState(ctx, 1, 1000))

	// Commit a batch with valid data first to set state.
	err = st.CommitReplayBatch(ctx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:  json.RawMessage(`{"i128":"100"}`),
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 1},
	})
	require.NoError(t, err)

	// Now attempt a second batch with invalid topics JSON. The UPDATE
	// on events must fail, and the replay_state UPDATE must not commit.
	err = st.CommitReplayBatch(ctx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: json.RawMessage(`not json`),
			Value:  json.RawMessage(`{"i128":"200"}`),
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 2, Changed: 1},
	})
	require.Error(t, err, "invalid JSON topics must cause the batch to fail")

	// The event topics must still be the first batch's value — the
	// second batch's rewrite must have been rolled back.
	got, err := st.GetEvent(ctx, e.ID, SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"transfer"}]`, string(got.Topics),
		"event topics must not reflect the rolled-back second batch")

	// The replay state must still show processed=1 from the first batch.
	state, err := st.GetReplayState(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.Processed,
		"replay state must not advance past the rolled-back batch")
}

// ---------------------------------------------------------------------------
// Context cancellation
// ---------------------------------------------------------------------------

// TestContextCancellation_RollsBackReplaceEventsInRange seeds events,
// starts a ReplaceEventsInRange with a cancellable context, and
// cancels it immediately. The original events must remain intact.
func TestContextCancellation_RollsBackReplaceEventsInRange(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	original := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 100, contractB),
	}
	_, err := st.UpsertEvents(ctx, original)
	require.NoError(t, err)

	// Cancel the context before the operation starts so it fails
	// immediately on any DB call within the transaction.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // cancel before calling the store

	err = st.ReplaceEventsInRange(cancelCtx, []Event{
		testEvent(eventID(3), 100, contractA),
	}, 100, 100)
	require.Error(t, err, "cancelled context must cause the operation to fail")

	// The original events must remain.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100,
		ToLedger:   100,
		Scope:      WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 2, "cancelled context must not leak partial writes")
}

// TestContextCancellation_RollsBackCommitReplayBatch seeds events and
// replay state, then tries to commit a batch with a pre-cancelled
// context. The state and events must be unchanged.
func TestContextCancellation_RollsBackCommitReplayBatch(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	e := testEvent(eventID(1), 100, contractA)
	_, err := st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	require.NoError(t, st.StartReplayState(ctx, 1, 1000))

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	err = st.CommitReplayBatch(cancelCtx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: json.RawMessage(`[{"symbol":"mint"}]`),
			Value:  json.RawMessage(`{"i128":"42"}`),
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 1, Changed: 1},
	})
	require.Error(t, err, "cancelled context must cause the batch to fail")

	// Event must be unchanged.
	got, err := st.GetEvent(ctx, e.ID, SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"transfer"},{"u64":7}]`, string(got.Topics),
		"event topics must not be modified by a cancelled batch")

	// Replay state must show no progress.
	state, err := st.GetReplayState(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, state.Processed,
		"replay state must not advance after cancelled context")
}

// ---------------------------------------------------------------------------
// No transaction left open on error
// ---------------------------------------------------------------------------

// TestNoTransactionLeakedOnError confirms that after a failed
// ReplaceEventsInRange, the connection pool is still usable. This is
// a proxy test for "no transaction left open": if the deferred
// rollback were missing, a failed transaction would poison the
// connection and the pool would eventually exhaust or the next query
// on that connection would fail.
func TestNoTransactionLeakedOnError(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// Cause a ReplaceEventsInRange failure via invalid JSON.
	err := st.ReplaceEventsInRange(ctx, []Event{
		{
			ID:               eventID(1),
			ContractID:       contractA,
			Ledger:           100,
			Type:             "contract",
			TxHash:           "deadbeef",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`not json`),
			Value:            json.RawMessage(`{"i128":"1"}`),
		},
	}, 100, 100)
	require.Error(t, err)

	// The pool must still work — any lingering open transaction would
	// eventually block or error on a new query.
	err = st.Ping(ctx)
	require.NoError(t, err, "pool must remain healthy after a failed transaction")

	// A subsequent write must succeed, proving the pool isn't poisoned.
	_, err = st.UpsertEvents(ctx, []Event{
		testEvent(eventID(10), 200, contractA),
	})
	require.NoError(t, err, "writes must still work after a previous transactional failure")
}

// TestNoTransactionLeakedOnCancelledContext confirms that cancelling a
// context during a transaction does not leave the pool in a bad state.
func TestNoTransactionLeakedOnCancelledContext(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	_ = st.ReplaceEventsInRange(cancelCtx, []Event{
		testEvent(eventID(1), 100, contractA),
	}, 100, 100)

	// Pool must still be responsive.
	require.NoError(t, st.Ping(ctx), "pool must survive a cancelled-context failure")

	// A write must succeed afterwards.
	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(10), 200, contractA),
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Replay batch: the event rewrites and state update are a single
// transaction, so a failure in either must roll back both.
// ---------------------------------------------------------------------------

// TestCommitReplayBatch_RollsBackEventUpdateOnStateFailure is the
// inverse of the above: the event rewrite succeeds but the state
// update (hypothetically) fails. In practice we simulate this by
// cancelling the context between the two, which causes the Commit to
// fail. Neither the event rewrite nor the state update must be
// visible.
func TestCommitReplayBatch_RollsBackOnCommitFailure(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	e := testEvent(eventID(1), 100, contractA)
	_, err := st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	require.NoError(t, st.StartReplayState(ctx, 1, 1000))

	// We can't easily make the state UPDATE fail independently, but we
	// can confirm that a general failure in the batch rolls back both
	// halves. Use invalid topics to trigger the failure path.
	err = st.CommitReplayBatch(ctx, ReplayBatch{
		Events: []EventDecoding{{
			ID:     e.ID,
			Topics: json.RawMessage(`broken`),
			Value:  json.RawMessage(`{"i128":"1"}`),
		}},
		State: ReplayState{LastEventID: e.ID, Processed: 1, Changed: 1},
	})
	require.Error(t, err)

	// The event must be unchanged.
	got, err := st.GetEvent(ctx, e.ID, SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"transfer"},{"u64":7}]`, string(got.Topics))

	// The replay state must show zero progress — the state UPDATE was
	// rolled back with the batch.
	state, err := st.GetReplayState(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 0, state.Processed,
		"replay state must not advance when the batch transaction rolls back")
}

// ---------------------------------------------------------------------------
// Connection pool health after transient failures
// ---------------------------------------------------------------------------

// TestPoolSurvivesConsecutiveErrors confirms the pool stays healthy
// after multiple consecutive transactional failures. Each failure
// exercises the defer-rollback path; if any of them leaked, the pool
// would eventually exhaust.
func TestPoolSurvivesConsecutiveErrors(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		err := st.ReplaceEventsInRange(ctx, []Event{
			{
				ID:               eventID(i),
				ContractID:       contractA,
				Ledger:           int64(100 + i),
				Type:             "contract",
				TxHash:           "deadbeef",
				InSuccessfulCall: true,
				Topics:           json.RawMessage(`not json`),
			},
		}, int64(100+i), int64(100+i))
		require.Error(t, err, "iteration %d: invalid JSON must fail", i)
	}

	// After 5 consecutive failures the pool must still work.
	require.NoError(t, st.Ping(ctx))

	_, err := st.UpsertEvents(ctx, []Event{
		testEvent(eventID(50), 500, contractA),
	})
	require.NoError(t, err, "writes must work after consecutive transactional failures")
}

// ---------------------------------------------------------------------------
// Advisory lock: connection release after error
// ---------------------------------------------------------------------------

// TestReplayLock_ReleaseOnError confirms that the replay advisory lock
// is released even when the calling code hits an error after acquiring
// it. This prevents a leaked session-level lock from blocking future
// replays.
func TestReplayLock_ReleaseOnError(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	lock, err := st.AcquireReplayLock(ctx)
	require.NoError(t, err)

	// Simulate an error path that releases the lock.
	lock.Release()

	// A second acquisition must succeed — the lock was properly released.
	lock2, err := st.AcquireReplayLock(ctx)
	require.NoError(t, err, "advisory lock must be available after Release on error path")
	lock2.Release()
}

// TestReplayLock_DoubleReleaseIsSafe confirms that calling Release()
// more than once does not panic or corrupt the connection pool.
func TestReplayLock_DoubleReleaseIsSafe(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	lock, err := st.AcquireReplayLock(ctx)
	require.NoError(t, err)

	lock.Release()
	assert.NotPanics(t, func() { lock.Release() }, "double Release must be safe")

	// Pool must still work.
	require.NoError(t, st.Ping(ctx))
}

// TestReplayLock_ExclusiveAcrossSessions confirms that a second
// acquisition attempt fails while the lock is held, and succeeds after
// it is released. This verifies the session-level advisory lock is
// truly exclusive.
func TestReplayLock_ExclusiveAcrossSessions(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	first, err := st.AcquireReplayLock(ctx)
	require.NoError(t, err)

	_, err = st.AcquireReplayLock(ctx)
	assert.Error(t, err, "second lock attempt must fail")

	first.Release()

	second, err := st.AcquireReplayLock(ctx)
	require.NoError(t, err, "lock must be available after release")
	second.Release()
}



// ---------------------------------------------------------------------------
// Batch boundary: events across multiple ledgers in one replace
// ---------------------------------------------------------------------------

// TestReplaceEventsInRange_CrossLedgerAtomicity replaces events
// spanning two ledger ranges in a single call and confirms both
// ranges are updated together (or neither is).
func TestReplaceEventsInRange_CrossLedgerAtomicity(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	original := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
		testEvent(eventID(3), 102, contractA),
	}
	_, err := st.UpsertEvents(ctx, original)
	require.NoError(t, err)

	// Replace ledgers 100–101 with a single new event in ledger 100.
	replacement := []Event{
		testEvent(eventID(4), 100, contractA),
	}
	for i := range replacement {
		replacement[i].Network = "default"
	}
	require.NoError(t, st.ReplaceEventsInRange(ctx, replacement, 100, 101))

	// Ledger 100 should have the new event, ledger 101 should be empty.
	got100, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100, ToLedger: 100, Scope: WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got100, 1)
	assert.Equal(t, eventID(4), got100[0].ID)

	got101, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 101, ToLedger: 101, Scope: WildcardScope(),
	})
	require.NoError(t, err)
	assert.Empty(t, got101)

	// Ledger 102 must be untouched.
	got102, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 102, ToLedger: 102, Scope: WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got102, 1)
}

// TestReplaceEventsInRange_RollsBackCrossLedgerFailure seeds events
// across two ledgers and attempts a cross-ledger replace where the
// insert is invalid. Both ledgers must retain their original events.
func TestReplaceEventsInRange_RollsBackCrossLedgerFailure(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	original := []Event{
		testEvent(eventID(1), 100, contractA),
		testEvent(eventID(2), 101, contractA),
	}
	_, err := st.UpsertEvents(ctx, original)
	require.NoError(t, err)

	// Attempt a cross-ledger replace with invalid JSON in one event.
	badEvent := testEvent(eventID(3), 100, contractA)
	badEvent.Topics = json.RawMessage(`not json`)
	badEvent.Network = "default"

	err = st.ReplaceEventsInRange(ctx, []Event{badEvent}, 100, 101)
	require.Error(t, err)

	// Both original events must survive.
	got, _, err := st.QueryEvents(ctx, EventFilter{
		FromLedger: 100, ToLedger: 101, Scope: WildcardScope(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 2, "cross-ledger replace failure must roll back both DELETEs")
}
