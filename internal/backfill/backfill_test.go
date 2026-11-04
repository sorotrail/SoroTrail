package backfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/horizon"
	"github.com/sorotrail/sorotrail/internal/store"
)

// fakeHorizon implements horizon.Client. It returns up to three pages of
// fixture data and then an empty response.
type fakeHorizon struct {
	pageResponses [][]horizon.Transaction
	calls         int
	lastCursor    string
	lastLimit     int
	lastContract  string
	lastFailed    bool
}

func (f *fakeHorizon) ListContractTransactions(_ context.Context, contractID, cursor string, limit int, includeFailed bool) (horizon.TransactionsResponse, error) {
	f.calls++
	f.lastContract = contractID
	f.lastCursor = cursor
	f.lastLimit = limit
	f.lastFailed = includeFailed
	if f.calls > len(f.pageResponses) {
		return horizon.TransactionsResponse{}, nil
	}
	resp := horizon.TransactionsResponse{}
	resp.Embedded.Records = f.pageResponses[f.calls-1]
	return resp, nil
}

// buildSimpleMeta produces a V3 meta containing a single ContractEvent
// for the given contract seed/body. Round-tripped to base64 so
// ExtractContractEvents sees a real wire shape.
func buildSimpleMeta(t *testing.T, contractSeed string, body xdr.ScVal) string {
	t.Helper()
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = contractSeed[i%len(contractSeed)]
	}
	cid := xdr.ContractId(xdr.Hash(hash))
	bodyXDR, err := xdr.NewContractEventBody(0, xdr.ContractEventV0{Topics: nil, Data: body})
	if err != nil {
		panic(err)
	}
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body:       bodyXDR,
	}
	meta := xdr.TransactionMeta{
		V: 3,
		V3: &xdr.TransactionMetaV3{
			SorobanMeta: &xdr.SorobanTransactionMeta{
				Events:      []xdr.ContractEvent{ev},
				ReturnValue: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
	b, err := xdr.MarshalBase64(meta)
	require.NoError(t, err)
	return b
}

// scSymbol / scU64 produce minimal ScVal types for tests.
func scSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func scU64(n uint64) xdr.ScVal {
	u := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func scVec(items ...xdr.ScVal) xdr.ScVal {
	v := xdr.ScValTypeScvVec
	vec := xdr.ScVec(items)
	p := &vec
	return xdr.ScVal{Type: v, Vec: &p}
}

func contractIDFromSeed(seed string) string {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = seed[i%len(seed)]
	}
	var cid xdr.ContractId
	copy(cid[:], hash)
	return xdr.Hash(cid).HexString()
}

// dummyLog returns a logger that drops everything.
func dummyLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeStore implements the backfill Store interface and captures writes.
//
// UpsertEvents mirrors the real store.Postgres.UpsertEvents dedupe key —
// `ON CONFLICT (ledger, id) DO NOTHING` (see internal/store/postgres.go's
// insertEventsBatch) — by keeping a rows map keyed on (ledger, id) and
// only counting a write as "inserted" the first time that key is seen.
// Without this, a fake that just returns len(events) on every call
// can't distinguish a genuine double-run duplicate from an idempotent
// no-op, which is exactly the property TestRun_RunningTwiceDoesNotDuplicateRows
// and TestRun_ResumeDiscardsOverlapWithoutDuplicating need to observe.
type fakeStore struct {
	mu              sync.Mutex
	state           store.BackfillState
	startCalled     bool
	updateCallCount int
	updateLedgers   []int64
	upsertCallCount int
	upsertBatches   [][]store.Event
	completeCalled  bool
	rows            map[string]store.Event
}

// rowKey mirrors the real store's ON CONFLICT (ledger, id) target.
func rowKey(e store.Event) string {
	return fmt.Sprintf("%d|%s", e.Ledger, e.ID)
}

func (f *fakeStore) UpsertEvents(_ context.Context, events []store.Event) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCallCount++
	f.upsertBatches = append(f.upsertBatches, events)
	if f.rows == nil {
		f.rows = make(map[string]store.Event)
	}
	var inserted int64
	for _, e := range events {
		key := rowKey(e)
		if _, exists := f.rows[key]; exists {
			continue // DO NOTHING: same (ledger, id) already stored.
		}
		f.rows[key] = e
		inserted++
	}
	return inserted, nil
}
func (f *fakeStore) GetBackfillState(_ context.Context) (store.BackfillState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.startCalled && !f.completeCalled && f.state.ContractID == "" {
		return store.BackfillState{}, store.ErrNotFound
	}
	s := f.state
	s.Ranges = cloneRanges(f.state.Ranges)
	return s, nil
}
func (f *fakeStore) StartBackfillState(_ context.Context, contractID string, fromLedger, toLedger int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startCalled = true
	f.state = store.BackfillState{
		ContractID: contractID, FromLedger: fromLedger, ToLedger: toLedger,
		LastLedger: 0, StartedAt: time.Now(), UpdatedAt: time.Now(),
	}
	return nil
}
func (f *fakeStore) UpdateBackfillState(_ context.Context, lastLedger int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCallCount++
	f.updateLedgers = append(f.updateLedgers, lastLedger)
	if lastLedger > f.state.LastLedger {
		f.state.LastLedger = lastLedger
	}
	return nil
}
func (f *fakeStore) SaveBackfillRanges(_ context.Context, ranges []store.BackfillRange) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Ranges = cloneRanges(ranges)
	f.state.UpdatedAt = time.Now()
	if len(ranges) > 0 {
		wm := store.ContiguousWatermark(ranges[0].FromLedger, ranges)
		if wm > f.state.LastLedger {
			f.state.LastLedger = wm
		}
	}
	return nil
}
func (f *fakeStore) CompleteBackfillState(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCalled = true
	ts := time.Now()
	f.state.CompletedAt = &ts
	return nil
}

// TestRun_HappyPath: three pages of events are upserted in order,
// progress advances, and CompleteBackfillState fires when all pages
// have been consumed.
func TestRun_HappyPath(t *testing.T) {
	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("transfer"), scU64(7))
	meta := buildSimpleMeta(t, "alpha", body)

	h := &fakeHorizon{
		pageResponses: [][]horizon.Transaction{
			{
				{ID: "t1", Hash: "h1", Ledger: 100, CreatedAt: "2026-07-24T00:00:00Z", ResultCode: "txSuccess", ResultMetaXDR: meta},
				{ID: "t2", Hash: "h2", Ledger: 100, CreatedAt: "2026-07-24T00:00:01Z", ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{
				{ID: "t3", Hash: "h3", Ledger: 101, CreatedAt: "2026-07-24T00:00:02Z", ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{}, // empty page → stop
		},
	}
	fs := &fakeStore{}

	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract, FromLedger: 100, BatchSize: 2,
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	assert.EqualValues(t, 2, sum.PagesFetched)
	assert.EqualValues(t, 3, sum.Transactions)
	assert.EqualValues(t, 3, sum.Extracted)
	assert.EqualValues(t, 3, sum.Inserted)
	assert.EqualValues(t, 101, sum.ThroughLedger)
	assert.True(t, fs.completeCalled)
	// Cursor chain was respected; the final page is empty so no new cursor is advanced.
	assert.Equal(t, "t2", h.lastCursor)
	require.EqualValues(t, 2, fs.upsertCallCount)
}

// TestRun_ResumesFromSavedProgress: when GetBackfillState returns a
// matching unfinished row, Run picks up at lastLedger+1 and only
// processes rows with ledger >= that.
func TestRun_ResumesFromSavedProgress(t *testing.T) {
	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)

	h := &fakeHorizon{
		pageResponses: [][]horizon.Transaction{
			{
				{ID: "tx_late", Hash: "late_hash", Ledger: 110, ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
		},
	}
	ts := time.Now().Add(-time.Hour)
	fs := &fakeStore{state: store.BackfillState{
		ContractID: targetContract, FromLedger: 100, ToLedger: 200,
		LastLedger: 109, StartedAt: ts, UpdatedAt: ts,
	}}

	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract, FromLedger: 100, ToLedger: 200, BatchSize: 1,
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Resumed)
	assert.True(t, sum.Completed)
	// Only the late tx survives the resume filter.
	require.EqualValues(t, 1, sum.Transactions)
	assert.EqualValues(t, 1, sum.Extracted)
	// Progress picks up at 109 (saved) → advances to 110 (new last).
	assert.EqualValues(t, 109+1, fs.state.LastLedger-StateUpdate(fs))
}

// StateUpdate is a tiny test helper that returns zero — used above to
// avoid dead-code lints for unused f.state.LastLedger.
func StateUpdate(fs *fakeStore) int64 { return int64(0) }

// TestRun_ResumeRefusesForDifferentContract: resuming with a different
// --contract starts fresh; the reason is "resuming into a row whose
// contract moved would silently skip rows."
func TestRun_ResumeRefusesForDifferentContract(t *testing.T) {
	ts := time.Now().Add(-time.Hour)
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)
	fs := &fakeStore{state: store.BackfillState{
		ContractID: contractIDFromSeed("ORIGINAL"), FromLedger: 100, ToLedger: 200,
		LastLedger: 150, StartedAt: ts, UpdatedAt: ts,
	}}
	h := &fakeHorizon{pageResponses: [][]horizon.Transaction{
		{{ID: "x", Hash: "y", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta}},
	}}

	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: contractIDFromSeed("NEW"), FromLedger: 100, ToLedger: 200, BatchSize: 1,
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.False(t, sum.Resumed, "mismatched contract must not be picked up as a resume")
	assert.EqualValues(t, 1, sum.Transactions)
}

// TestRun_DryRunNeverUpserts: --dry-run walks the range but commits
// nothing and never marks the row complete in a way that future runs
// could resume from (the state row gets reset to ledger=0).
func TestRun_DryRunNeverUpserts(t *testing.T) {
	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)
	h := &fakeHorizon{pageResponses: [][]horizon.Transaction{
		{{ID: "t", Hash: "h", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta}},
	}}
	fs := &fakeStore{}

	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract, FromLedger: 100, BatchSize: 1, DryRun: true,
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	assert.EqualValues(t, 0, fs.upsertCallCount, "dry run must not upsert")
	assert.False(t, fs.completeCalled, "dry run must not commit completion")
	assert.EqualValues(t, 0, fs.state.LastLedger, "dry run must reset last_ledger")
}

// TestRun_RespectsToLedger: pages past the upper bound end the run
// after committing what was already extracted from in-range rows.
func TestRun_RespectsToLedger(t *testing.T) {
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)
	h := &fakeHorizon{pageResponses: [][]horizon.Transaction{
		{
			{ID: "t1", Hash: "h1", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta},
			// Above the bound — extension stops the run after committing
			// the in-range row.
			{ID: "t2", Hash: "h2", Ledger: 110, ResultCode: "txSuccess", ResultMetaXDR: meta},
		},
	}}
	fs := &fakeStore{}
	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: contractIDFromSeed("alpha"),
		FromLedger: 100, ToLedger: 105, BatchSize: 2, MaxBackoff: time.Millisecond,
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	// Only the in-range row counts as Extracted; the out-of-range one
	// is tallied but not extracted.
	require.EqualValues(t, 1, sum.Extracted)
}

// TestRun_AbortsOnMissingContract: an empty --contract is rejected up
// front. Operators routinely typo this and the surfaced error is
// easier to debug than a successful empty run.
func TestRun_AbortsOnMissingContract(t *testing.T) {
	b := New(&fakeHorizon{}, &fakeStore{}, decode.XDRDecoder{}, dummyLog(),
		Options{ContractID: "", FromLedger: 1, BatchSize: 1})
	_, err := b.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--contract")
}

// TestRun_AbortsOnMissingFromLedger: an invalid --from-ledger is
// rejected up front.
func TestRun_AbortsOnMissingFromLedger(t *testing.T) {
	b := New(&fakeHorizon{}, &fakeStore{}, decode.XDRDecoder{}, dummyLog(),
		Options{ContractID: contractIDFromSeed("x"), FromLedger: 0, BatchSize: 1})
	_, err := b.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--from-ledger")
}

// TestRun_AbortsOnContextCancel: a canceled context yields the
// partial summary, not an error — same shape as replay.
func TestRun_AbortsOnContextCancel(t *testing.T) {
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)
	h := &fakeHorizon{pageResponses: [][]horizon.Transaction{
		{{ID: "t", Hash: "h", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta}},
	}}
	fs := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: contractIDFromSeed("alpha"), FromLedger: 100, BatchSize: 1,
	})
	sum, err := b.Run(ctx)
	require.NoError(t, err)
	assert.False(t, sum.Completed, "interrupted run reports not completed")
}

// cancelingHorizon wraps fakeHorizon and cancels the run's context right
// after serving its cancelAfter-th page, simulating an operator hitting
// Ctrl-C mid-run. Run's loop only checks ctx.Err() at the top of the
// loop (before the next fetch), so the page that was already fetched
// finishes committing normally and the interruption lands cleanly at
// the next page boundary.
type cancelingHorizon struct {
	fakeHorizon
	cancelAfter int
	cancel      context.CancelFunc
	fired       bool
}

func (c *cancelingHorizon) ListContractTransactions(ctx context.Context, contractID, cursor string, limit int, includeFailed bool) (horizon.TransactionsResponse, error) {
	resp, err := c.fakeHorizon.ListContractTransactions(ctx, contractID, cursor, limit, includeFailed)
	if !c.fired && c.calls == c.cancelAfter {
		c.fired = true
		c.cancel()
	}
	return resp, err
}

// TestRun_RunningTwiceDoesNotDuplicateRows proves the acceptance
// criterion "running twice does not duplicate rows" end to end through
// the Backfiller, not just at the store layer. It runs a full Run()
// against a fixed set of Horizon pages, then builds a fresh Backfiller
// (same store, same options, same fixture data — modeling an operator
// re-running the identical `sorotrail backfill` invocation) and runs it
// again. Because the completed run's backfill_state row has
// completed_at set, resume() treats the second Run as a fresh start
// (see Backfiller.resume), so it re-walks and re-extracts every page —
// exactly what re-running the CLI command does. The fake store dedupes
// on (ledger, id) exactly like the real Postgres ON CONFLICT clause, so
// this only holds if the extraction step also reproduces identical
// event IDs on both passes (it does: formatEventID is a pure function
// of tx hash/ledger/op index/event index, all of which come from the
// same fixture data on both runs).
func TestRun_RunningTwiceDoesNotDuplicateRows(t *testing.T) {
	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("transfer"), scU64(7))
	meta := buildSimpleMeta(t, "alpha", body)

	fixture := func() [][]horizon.Transaction {
		return [][]horizon.Transaction{
			{
				{ID: "t1", Hash: "h1", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta},
				{ID: "t2", Hash: "h2", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{
				{ID: "t3", Hash: "h3", Ledger: 101, ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{}, // empty page → stop
		}
	}
	opts := Options{ContractID: targetContract, FromLedger: 100, BatchSize: 2}
	fs := &fakeStore{}

	// First run.
	b1 := New(&fakeHorizon{pageResponses: fixture()}, fs, decode.XDRDecoder{}, dummyLog(), opts)
	sum1, err := b1.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum1.Completed)
	assert.True(t, fs.completeCalled)
	require.EqualValues(t, 3, sum1.Inserted, "first run inserts all three distinct events")
	require.Len(t, fs.rows, 3, "store holds exactly the three distinct events after run 1")

	// Second run: identical options, identical Horizon fixture, same
	// store instance — this is "run the same command again."
	b2 := New(&fakeHorizon{pageResponses: fixture()}, fs, decode.XDRDecoder{}, dummyLog(), opts)
	sum2, err := b2.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum2.Completed)
	// Every event the second run extracts already exists at the same
	// (ledger, id) key, so nothing new gets counted as inserted.
	assert.EqualValues(t, 3, sum2.Extracted, "second run re-extracts the same three events")
	assert.EqualValues(t, 0, sum2.Inserted, "second run must not report any new rows")
	assert.Len(t, fs.rows, 3, "store row count is unchanged after the second run")
}

// TestRun_ResumeDiscardsOverlapWithoutDuplicating proves the other half
// of "fills gaps idempotently": when a run is interrupted mid-page and
// resumed, Horizon re-serves its page sequence from the start (backfill
// persists last_ledger, not the opaque paging_token — see resume()'s
// doc comment and extractPage's overlap-discard comment), and the rows
// already committed before the interruption must be discarded, not
// re-counted or re-inserted, while ledgers past the resume point are
// captured normally.
func TestRun_ResumeDiscardsOverlapWithoutDuplicating(t *testing.T) {
	targetContract := contractIDFromSeed("alpha")
	body := scVec(scSymbol("x"), scU64(1))
	meta := buildSimpleMeta(t, "alpha", body)

	fixture := func() [][]horizon.Transaction {
		return [][]horizon.Transaction{
			{
				{ID: "t1", Hash: "h1", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta},
				{ID: "t2", Hash: "h2", Ledger: 100, ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{
				{ID: "t3", Hash: "h3", Ledger: 101, ResultCode: "txSuccess", ResultMetaXDR: meta},
			},
			{}, // empty page → stop
		}
	}
	opts := Options{ContractID: targetContract, FromLedger: 100, BatchSize: 2}
	fs := &fakeStore{}

	// First run: interrupted right after page 1 (the two ledger-100
	// rows) commits, before page 2 (ledger 101) is ever fetched.
	ctx1, cancel1 := context.WithCancel(context.Background())
	h1 := &cancelingHorizon{
		fakeHorizon: fakeHorizon{pageResponses: fixture()},
		cancelAfter: 1,
		cancel:      cancel1,
	}
	b1 := New(h1, fs, decode.XDRDecoder{}, dummyLog(), opts)
	sum1, err := b1.Run(ctx1)
	require.NoError(t, err)
	assert.False(t, sum1.Completed, "run is interrupted before the empty page")
	assert.False(t, fs.completeCalled)
	assert.EqualValues(t, 2, sum1.Transactions)
	assert.EqualValues(t, 2, sum1.Extracted)
	assert.EqualValues(t, 2, sum1.Inserted)
	assert.EqualValues(t, 100, fs.state.LastLedger, "progress committed through the last processed ledger")
	require.Len(t, fs.rows, 2)

	// Second run: same options, Horizon re-serves its full page sequence
	// from the start (page 1 again re-includes the ledger-100 rows).
	// resume() picks start=101 from the saved state, so extractPage must
	// discard the re-served ledger-100 rows rather than re-extracting or
	// re-counting them, while still capturing the new ledger-101 row.
	h2 := &fakeHorizon{pageResponses: fixture()}
	b2 := New(h2, fs, decode.XDRDecoder{}, dummyLog(), opts)
	sum2, err := b2.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum2.Resumed)
	assert.True(t, sum2.Completed)
	assert.EqualValues(t, 1, sum2.Transactions, "the two re-served ledger-100 rows are discarded, not counted")
	assert.EqualValues(t, 1, sum2.Extracted)
	assert.EqualValues(t, 1, sum2.Inserted, "only the new ledger-101 row is a genuinely new insert")
	assert.Len(t, fs.rows, 3, "store ends with exactly the three distinct events across both runs, no duplicates")
}

// cursorHorizon is a stateless, cursor-indexed Horizon fake safe for concurrent
// use across N workers. Each call with cursor="" returns page 0, and a call
// with the last transaction's ID/PagingToken returns the next page.
type cursorHorizon struct {
	mu          sync.Mutex
	pages       [][]horizon.Transaction
	rangeErrors map[int64]error // keyed by range fromLedger
	rangeCalls  map[string]int  // keyed by "from..to"
}

func (c *cursorHorizon) pageForCursor(cursor string, rows []horizon.Transaction, limit int) []horizon.Transaction {
	startIdx := 0
	if cursor != "" {
		found := false
		for i, tx := range rows {
			tok := tx.PagingToken
			if tok == "" {
				tok = tx.ID
			}
			if tok == cursor {
				startIdx = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	if startIdx >= len(rows) {
		return nil
	}
	endIdx := startIdx + limit
	if endIdx > len(rows) {
		endIdx = len(rows)
	}
	out := make([]horizon.Transaction, endIdx-startIdx)
	copy(out, rows[startIdx:endIdx])
	return out
}

func (c *cursorHorizon) flattenRows() []horizon.Transaction {
	var all []horizon.Transaction
	for _, p := range c.pages {
		all = append(all, p...)
	}
	return all
}

func (c *cursorHorizon) ListContractTransactions(_ context.Context, _ string, cursor string, limit int, _ bool) (horizon.TransactionsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := c.pageForCursor(cursor, c.flattenRows(), limit)
	var resp horizon.TransactionsResponse
	resp.Embedded.Records = records
	return resp, nil
}

func (c *cursorHorizon) ListContractTransactionsInRange(_ context.Context, _ string, fromLedger, toLedger int64, cursor string, limit int, _ bool) (horizon.TransactionsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rangeCalls == nil {
		c.rangeCalls = make(map[string]int)
	}
	key := fmt.Sprintf("%d..%d", fromLedger, toLedger)
	c.rangeCalls[key]++
	if err, ok := c.rangeErrors[fromLedger]; ok && err != nil {
		return horizon.TransactionsResponse{}, err
	}
	var inRange []horizon.Transaction
	for _, tx := range c.flattenRows() {
		if tx.Ledger >= fromLedger && (toLedger <= 0 || tx.Ledger <= toLedger) {
			inRange = append(inRange, tx)
		}
	}
	var resp horizon.TransactionsResponse
	resp.Embedded.Records = c.pageForCursor(cursor, inRange, limit)
	return resp, nil
}

type countingRateLimiter struct {
	waits atomic.Int64
}

func (l *countingRateLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.waits.Add(1)
	return nil
}

// TestParallelRun_MatchesSerialEventSet verifies that running backfill across
// N parallel workers produces the exact same event set, IDs, and summary counts
// as a serial single-worker run over the same input (#1237).
func TestParallelRun_MatchesSerialEventSet(t *testing.T) {
	targetContract := contractIDFromSeed("parallel-parity")
	var allTxs []horizon.Transaction
	for ledger := int64(100); ledger <= 119; ledger++ {
		for txIdx := 0; txIdx < 2; txIdx++ {
			id := fmt.Sprintf("tx-%d-%d", ledger, txIdx)
			meta := buildSimpleMeta(t, "parallel-parity", scVec(scSymbol("mint"), scU64(uint64(ledger*10+int64(txIdx)))))
			allTxs = append(allTxs, horizon.Transaction{
				ID:            id,
				PagingToken:   id,
				Hash:          fmt.Sprintf("hash-%d-%d", ledger, txIdx),
				Ledger:        ledger,
				CreatedAt:     "2026-07-24T00:00:00Z",
				ResultCode:    "txSuccess",
				ResultMetaXDR: meta,
			})
		}
	}

	serialStore := &fakeStore{}
	serialHorizon := &cursorHorizon{pages: [][]horizon.Transaction{allTxs}}
	serialBackfiller := New(serialHorizon, serialStore, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   119,
		BatchSize:  5,
		Workers:    1,
	})
	serialSum, err := serialBackfiller.Run(context.Background())
	require.NoError(t, err)
	require.True(t, serialSum.Completed)

	parallelStore := &fakeStore{}
	parallelHorizon := &cursorHorizon{pages: [][]horizon.Transaction{allTxs}}
	parallelBackfiller := New(parallelHorizon, parallelStore, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   119,
		BatchSize:  5,
		Workers:    4,
	})
	parallelSum, err := parallelBackfiller.Run(context.Background())
	require.NoError(t, err)
	require.True(t, parallelSum.Completed)

	assert.Equal(t, 4, parallelSum.TotalRanges)
	assert.Equal(t, 4, parallelSum.CompletedRanges)
	assert.Empty(t, parallelSum.FailedRanges)
	assert.Equal(t, serialSum.Transactions, parallelSum.Transactions)
	assert.Equal(t, serialSum.Extracted, parallelSum.Extracted)
	assert.Equal(t, serialSum.Inserted, parallelSum.Inserted)
	assert.Equal(t, serialSum.Skipped, parallelSum.Skipped)
	assert.Equal(t, serialSum.Failed, parallelSum.Failed)
	assert.Equal(t, serialStore.rows, parallelStore.rows, "parallel worker pool must produce the exact same event set as serial execution")
}

// TestParallelRun_WorkerFailureAndResumeOnlyIncompleteRanges verifies that:
//  1. A worker failure in one sub-range does not hang or abort sibling workers.
//  2. The failed sub-range is surfaced clearly in the error and Summary.FailedRanges.
//  3. Re-running resumes only the failed/incomplete sub-range without re-fetching
//     already completed sub-ranges (#1237).
func TestParallelRun_WorkerFailureAndResumeOnlyIncompleteRanges(t *testing.T) {
	targetContract := contractIDFromSeed("parallel-resume")
	var allTxs []horizon.Transaction
	for ledger := int64(100); ledger <= 129; ledger++ {
		id := fmt.Sprintf("tx-%d", ledger)
		meta := buildSimpleMeta(t, "parallel-resume", scVec(scSymbol("swap"), scU64(uint64(ledger))))
		allTxs = append(allTxs, horizon.Transaction{
			ID:            id,
			PagingToken:   id,
			Hash:          fmt.Sprintf("hash-%d", ledger),
			Ledger:        ledger,
			CreatedAt:     "2026-07-24T00:00:00Z",
			ResultCode:    "txSuccess",
			ResultMetaXDR: meta,
		})
	}

	fs := &fakeStore{}
	// 3 ranges of 10 ledgers each: [100..109], [110..119], [120..129].
	// Force range [110..119] to fail with a permanent error on run 1.
	h1 := &cursorHorizon{
		pages: [][]horizon.Transaction{allTxs},
		rangeErrors: map[int64]error{
			110: errors.New("simulated range 110..119 failure"),
		},
	}

	b1 := New(h1, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   129,
		BatchSize:  5,
		Workers:    3,
		RangeSize:  10,
	})
	sum1, err1 := b1.Run(context.Background())
	require.Error(t, err1)
	assert.Contains(t, err1.Error(), "[110..119]")
	assert.False(t, sum1.Completed)
	assert.Equal(t, 3, sum1.TotalRanges)
	assert.Equal(t, 2, sum1.CompletedRanges, "sibling ranges [100..109] and [120..129] must complete despite range [110..119] failing")
	require.Len(t, sum1.FailedRanges, 1)
	assert.Equal(t, int64(110), sum1.FailedRanges[0].FromLedger)
	assert.Equal(t, int64(119), sum1.FailedRanges[0].ToLedger)
	assert.Len(t, fs.rows, 20, "20 events from the two healthy ranges must be committed")

	// Run 2: clear the failure and resume. Completed ranges [100..109] and [120..129]
	// must NOT be re-fetched; only [110..119] should be queried.
	h2 := &cursorHorizon{
		pages: [][]horizon.Transaction{allTxs},
	}
	b2 := New(h2, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   129,
		BatchSize:  5,
		Workers:    3,
		RangeSize:  10,
	})
	sum2, err2 := b2.Run(context.Background())
	require.NoError(t, err2)
	assert.True(t, sum2.Resumed)
	assert.True(t, sum2.Completed)
	assert.Equal(t, 3, sum2.CompletedRanges)
	assert.Empty(t, sum2.FailedRanges)
	assert.Equal(t, 0, h2.rangeCalls["100..109"], "completed range [100..109] must not be re-fetched on resume")
	assert.Equal(t, 0, h2.rangeCalls["120..129"], "completed range [120..129] must not be re-fetched on resume")
	assert.Greater(t, h2.rangeCalls["110..119"], 0, "incomplete range [110..119] must be fetched on resume")
	assert.Len(t, fs.rows, 30, "all 30 events across all three ranges are now stored without duplicates")
}

// TestParallelRun_AggregateRateLimitingAcrossWorkers verifies that all workers
// share the configured aggregate RateLimiter so concurrent workers never bypass
// rate limiting (#1237).
func TestParallelRun_AggregateRateLimitingAcrossWorkers(t *testing.T) {
	targetContract := contractIDFromSeed("rate-limit")
	var allTxs []horizon.Transaction
	for ledger := int64(100); ledger <= 115; ledger++ {
		id := fmt.Sprintf("tx-%d", ledger)
		meta := buildSimpleMeta(t, "rate-limit", scVec(scSymbol("tick"), scU64(uint64(ledger))))
		allTxs = append(allTxs, horizon.Transaction{
			ID:            id,
			PagingToken:   id,
			Hash:          fmt.Sprintf("hash-%d", ledger),
			Ledger:        ledger,
			CreatedAt:     "2026-07-24T00:00:00Z",
			ResultCode:    "txSuccess",
			ResultMetaXDR: meta,
		})
	}

	limiter := &countingRateLimiter{}
	var snapshots []ProgressSnapshot
	var snapMu sync.Mutex

	h := &cursorHorizon{pages: [][]horizon.Transaction{allTxs}}
	fs := &fakeStore{}
	b := New(h, fs, decode.XDRDecoder{}, dummyLog(), Options{
		ContractID: targetContract,
		FromLedger: 100,
		ToLedger:   115,
		BatchSize:  2,
		Workers:    4,
		Limiter:    limiter,
		RangeProgress: func(snap ProgressSnapshot) {
			snapMu.Lock()
			snapshots = append(snapshots, snap)
			snapMu.Unlock()
		},
	})
	sum, err := b.Run(context.Background())
	require.NoError(t, err)
	assert.True(t, sum.Completed)
	assert.GreaterOrEqual(t, limiter.waits.Load(), sum.PagesFetched,
		"every page fetch across all concurrent workers must pass through the shared RateLimiter")

	snapMu.Lock()
	require.NotEmpty(t, snapshots)
	finalSnap := snapshots[len(snapshots)-1]
	snapMu.Unlock()
	assert.Equal(t, 4, finalSnap.TotalRanges)
	assert.Equal(t, 4, finalSnap.CompletedRanges)
	assert.Equal(t, int64(16), finalSnap.CompletedLedgers)
	assert.Equal(t, int64(0), finalSnap.EstimatedRemainingLedgers)
}

func TestIntervalRateLimiter_EnforcesAggregateSpacing(t *testing.T) {
	rl := NewIntervalRateLimiter(5 * time.Millisecond)
	ctx := context.Background()

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, rl.Wait(ctx))
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	assert.Equal(t, int64(4), rl.WaitCount())
	// 4 calls spaced by 5ms require at least 3 intervals (~15ms).
	assert.GreaterOrEqual(t, elapsed, 12*time.Millisecond)
}

// avoid `errors` import flagging by referencing it once.
var _ = errors.New
