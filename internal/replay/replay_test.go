package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// eventID builds IDs whose lexicographic order matches insertion order, like
// real TOIDs.
func eventID(n int) string { return fmt.Sprintf("%016d-%010d", n, 0) }

// seedEvent is an event stored with raw XDR and an old-decoder decoding.
func seedEvent(n int, ledger int64) store.DecodedEvent {
	return store.DecodedEvent{
		ID:          eventID(n),
		ContractID:  contractA,
		Ledger:      ledger,
		RawTopicXDR: []string{"topic-xdr"},
		RawValueXDR: "value-xdr",
		Topics:      json.RawMessage(`[{"unknown":{"type":"scvFoo"}}]`),
		Value:       json.RawMessage(`{"unknown":{"type":"scvFoo"}}`),
	}
}

const contractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// improvedDecoder stands for a decoder that learned to handle a type it
// previously wrapped in the lossless {"unknown": ...} fallback.
func improvedDecoder() staticDecoder {
	return staticDecoder{out: map[string]string{
		"topic-xdr": `{"symbol":"transfer"}`,
		"value-xdr": `{"i128":"1000"}`,
	}}
}

func newTestReplayer(st Store, dec staticDecoder, opts Options) *Replayer {
	return New(st, dec, testLogger(), opts)
}

// A deliberately-changed decoder must rewrite the decoded columns of every
// row in range, leaving rows outside the range untouched.
func TestRun_RewritesDecodedColumns(t *testing.T) {
	st := newFakeStore(
		seedEvent(1, 100),
		seedEvent(2, 150),
		seedEvent(3, 400), // outside the range
	)
	r := newTestReplayer(st, improvedDecoder(), Options{FromLedger: 100, ToLedger: 200, BatchSize: 1})

	sum, err := r.Run(context.Background())
	require.NoError(t, err)

	assert.True(t, sum.Completed)
	assert.EqualValues(t, 2, sum.Processed)
	assert.EqualValues(t, 2, sum.Changed)
	assert.Zero(t, sum.Skipped)
	assert.Zero(t, sum.Failed)

	got := st.snapshot()
	assert.Equal(t, `[{"symbol":"transfer"}]|{"i128":"1000"}`, got[eventID(1)])
	assert.Equal(t, `[{"symbol":"transfer"}]|{"i128":"1000"}`, got[eventID(2)])
	assert.Equal(t, `[{"unknown":{"type":"scvFoo"}}]|{"unknown":{"type":"scvFoo"}}`,
		got[eventID(3)], "event outside the ledger range must not be touched")
}

// Idempotency: running the same replay twice leaves the identical end state,
// and the second run rewrites nothing because nothing differs.
func TestRun_Idempotent(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 101), seedEvent(3, 102))
	opts := Options{FromLedger: 1, ToLedger: 1000, BatchSize: 2}

	first, err := newTestReplayer(st, improvedDecoder(), opts).Run(context.Background())
	require.NoError(t, err)
	afterFirst := st.snapshot()

	second, err := newTestReplayer(st, improvedDecoder(), opts).Run(context.Background())
	require.NoError(t, err)

	assert.Equal(t, afterFirst, st.snapshot(), "second run must not change the end state")
	assert.EqualValues(t, 3, first.Changed)
	assert.EqualValues(t, 3, second.Processed)
	assert.EqualValues(t, 0, second.Changed, "already-current rows are not rewritten")
	assert.True(t, second.Completed)
}

// Rows stored before raw XDR was retained are skipped and counted, never
// errored, and their decoded columns are left alone.
func TestRun_SkipsRowsWithoutRawXDR(t *testing.T) {
	legacy := seedEvent(2, 101)
	legacy.RawTopicXDR, legacy.RawValueXDR = nil, ""

	st := newFakeStore(seedEvent(1, 100), legacy, seedEvent(3, 102))
	r := newTestReplayer(st, improvedDecoder(), Options{FromLedger: 1, ToLedger: 1000})

	sum, err := r.Run(context.Background())
	require.NoError(t, err)

	assert.EqualValues(t, 3, sum.Processed)
	assert.EqualValues(t, 2, sum.Changed)
	assert.EqualValues(t, 1, sum.Skipped)
	assert.Zero(t, sum.Failed)
	assert.Equal(t, `[{"unknown":{"type":"scvFoo"}}]|{"unknown":{"type":"scvFoo"}}`,
		st.snapshot()[eventID(2)], "a skipped row keeps its stored decoding")
}

// A row whose stored XDR cannot be decoded is counted and left untouched, so
// one bad row can't wedge a replay forever.
func TestRun_CountsDecodeFailures(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 101))
	dec := staticDecoder{out: map[string]string{}} // every decode fails

	sum, err := newTestReplayer(st, dec, Options{FromLedger: 1, ToLedger: 1000}).Run(context.Background())
	require.NoError(t, err)

	assert.EqualValues(t, 2, sum.Processed)
	assert.EqualValues(t, 2, sum.Failed)
	assert.Zero(t, sum.Changed)
	assert.True(t, sum.Completed)
}

// Interrupt-and-resume: a run stopped partway must resume from the last
// committed batch and reach the same end state as an uninterrupted run,
// counting each row exactly once.
func TestRun_ResumesAfterInterrupt(t *testing.T) {
	events := []store.DecodedEvent{
		seedEvent(1, 100), seedEvent(2, 101), seedEvent(3, 102), seedEvent(4, 103),
	}
	opts := Options{FromLedger: 1, ToLedger: 1000, BatchSize: 2}

	// Reference: one uninterrupted run.
	reference := newFakeStore(events...)
	refSum, err := newTestReplayer(reference, improvedDecoder(), opts).Run(context.Background())
	require.NoError(t, err)

	// Interrupted: the second batch's commit fails, as a crash would.
	st := newFakeStore(events...)
	st.failCommitAfter, st.commitErr = 1, errCrash

	_, err = newTestReplayer(st, improvedDecoder(), opts).Run(context.Background())
	require.ErrorIs(t, err, errCrash)

	mid, err := st.GetReplayState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, eventID(2), mid.LastEventID, "progress stops at the last committed batch")
	assert.False(t, mid.Done())
	assert.EqualValues(t, 2, mid.Processed)

	// Re-run the same command: it resumes rather than starting over.
	st.failCommitAfter, st.commits = 0, 0
	resumed, err := newTestReplayer(st, improvedDecoder(), opts).Run(context.Background())
	require.NoError(t, err)

	assert.True(t, resumed.Resumed)
	assert.True(t, resumed.Completed)
	assert.EqualValues(t, refSum.Processed, resumed.Processed, "each row counted exactly once across the two runs")
	assert.EqualValues(t, refSum.Changed, resumed.Changed)
	assert.Equal(t, reference.snapshot(), st.snapshot(), "resumed run reaches the uninterrupted end state")
}

// Cancelling the context (Ctrl-C) stops cleanly between batches: no error,
// progress preserved, and the run reported as not completed.
func TestRun_ContextCancellationStopsCleanly(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 101), seedEvent(3, 102))
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel once the first batch has committed.
	st.onCommit = func() {
		if st.commits == 1 {
			cancel()
		}
	}

	sum, err := newTestReplayer(st, improvedDecoder(), Options{FromLedger: 1, ToLedger: 1000, BatchSize: 1}).Run(ctx)
	require.NoError(t, err, "an interrupt is not a failure")
	defer cancel()

	assert.False(t, sum.Completed)
	assert.EqualValues(t, 1, sum.Processed)

	state, err := st.GetReplayState(context.Background())
	require.NoError(t, err)
	assert.Equal(t, eventID(1), state.LastEventID)
	assert.False(t, state.Done())
}

// A range different from the saved one starts fresh instead of resuming into
// it, which would silently skip rows.
func TestRun_DifferentRangeStartsFresh(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 200))
	require.NoError(t, st.StartReplayState(context.Background(), 1, 50))
	st.state.LastEventID = eventID(1)

	sum, err := newTestReplayer(st, improvedDecoder(), Options{FromLedger: 1, ToLedger: 1000}).Run(context.Background())
	require.NoError(t, err)

	assert.False(t, sum.Resumed)
	assert.EqualValues(t, 2, sum.Processed, "both events replayed, not just the one after the stale cursor")
}

// --restart discards saved progress for the same range.
func TestRun_RestartIgnoresSavedProgress(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 101))
	require.NoError(t, st.StartReplayState(context.Background(), 1, 1000))
	st.state.LastEventID = eventID(1)
	st.state.Processed = 1

	sum, err := newTestReplayer(st, improvedDecoder(),
		Options{FromLedger: 1, ToLedger: 1000, Restart: true}).Run(context.Background())
	require.NoError(t, err)

	assert.False(t, sum.Resumed)
	assert.EqualValues(t, 2, sum.Processed)
}

// The concurrent-replay guard: while one run holds the lock, a second run
// fails fast instead of interleaving rewrites and corrupting shared progress.
func TestRun_ConcurrentReplayIsRefused(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100))

	held, err := st.AcquireReplayLock(context.Background())
	require.NoError(t, err)

	_, err = newTestReplayer(st, improvedDecoder(), Options{FromLedger: 1, ToLedger: 1000}).Run(context.Background())
	require.ErrorIs(t, err, store.ErrReplayLocked)

	// Once the holder releases, a replay runs normally — the guard doesn't
	// leave the lock stuck.
	held.Release()
	sum, err := newTestReplayer(st, improvedDecoder(), Options{FromLedger: 1, ToLedger: 1000}).Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
}

// A dry run reports what would change without writing rows or progress.
func TestRun_DryRunWritesNothing(t *testing.T) {
	st := newFakeStore(seedEvent(1, 100), seedEvent(2, 101))
	before := st.snapshot()

	sum, err := newTestReplayer(st, improvedDecoder(),
		Options{FromLedger: 1, ToLedger: 1000, DryRun: true}).Run(context.Background())
	require.NoError(t, err)

	assert.EqualValues(t, 2, sum.Processed)
	assert.EqualValues(t, 2, sum.Changed, "dry run still reports what would change")
	assert.Equal(t, before, st.snapshot(), "dry run must not rewrite rows")

	_, err = st.GetReplayState(context.Background())
	assert.ErrorIs(t, err, store.ErrNotFound, "dry run must not persist progress")
}

func TestJSONEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"identical", `{"symbol":"transfer"}`, `{"symbol":"transfer"}`, true},
		{"key order and whitespace normalized", `{"a":1,"b":2}`, `{ "b": 2, "a": 1 }`, true},
		{"different values", `{"u64":1}`, `{"u64":2}`, false},
		{"array order matters", `[1,2]`, `[2,1]`, false},
		{"both empty", ``, ``, true},
		{"one empty", `{}`, ``, false},
		{"invalid json", `{`, `{}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, jsonEqual(json.RawMessage(tt.a), json.RawMessage(tt.b)))
		})
	}
}
