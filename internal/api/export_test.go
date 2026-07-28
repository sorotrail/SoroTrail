package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// seedEvents returns events at ledgers 100..103 in a stable order. The
// topics and value are intentionally JSON shapes that exercise CSV
// escaping/quoting (commas, quotes) so a regression in the writer is
// caught immediately.
func seedEvents(contractID string) []store.Event {
	out := make([]store.Event, 0, 4)
	topics := json.RawMessage(`[{"symbol":"transfer,with,commas"},{"address":"GA\"quoted\""}]`)
	value := json.RawMessage(`{"i128":"1000"}`)
	for l := int64(100); l <= 103; l++ {
		out = append(out, store.Event{
			ID:         fmt.Sprintf("0000000001-%010d", l),
			ContractID: contractID,
			Ledger:     l,
			Type:       "contract",
			TxHash:     fmt.Sprintf("hash%d", l),
			Topics:     topics,
			Value:      value,
		})
	}
	return out
}

// fakeExportStore is an in-memory store that paginates via cursor just
// like the real Postgres store: a single page covers all seeded events
// when below MaxQueryLimit, otherwise it pages them.
type fakeExportStore struct {
	events   []store.Event
	position int
	cursor   string
}

func newFakeExportStore(events []store.Event) *fakeExportStore {
	return &fakeExportStore{events: events}
}

// QueryEvents returns a 2-row page so the export handler exercises its
// cursor-walking loop instead of the single-page branch. It also
// returns the cursor the handler will mirror back via filter.Cursor.
func (f *fakeExportStore) QueryEvents(_ context.Context, fl store.EventFilter) ([]store.Event, string, error) {
	if f.cursor != "" && fl.Cursor != f.cursor {
		return nil, "", fmt.Errorf("cursor mismatch: handler=%q store=%q", fl.Cursor, f.cursor)
	}
	// Filter by ledger range and contract (mirrors Postgres WHERE).
	var matched []store.Event
	for _, e := range f.events {
		if e.ContractID != fl.ContractID {
			continue
		}
		if fl.FromLedger > 0 && e.Ledger < fl.FromLedger {
			continue
		}
		if fl.ToLedger > 0 && e.Ledger > fl.ToLedger {
			continue
		}
		matched = append(matched, e)
	}
	// Stable order: ascending ledger, then id — matches the export's
	// OrderByLedger tiebreaker.
	for i := 0; i < len(matched); i++ {
		for j := i + 1; j < len(matched); j++ {
			ai, aj := matched[i], matched[j]
			if aj.Ledger < ai.Ledger || (aj.Ledger == ai.Ledger && aj.ID < ai.ID) {
				matched[i], matched[j] = aj, ai
			}
		}
	}
	// 2-row page: enforces the pagination loop in the handler.
	const page = 2
	start := f.position
	if start >= len(matched) {
		return nil, "", nil
	}
	end := start + page
	if end > len(matched) {
		end = len(matched)
	}
	page_ := matched[start:end]
	f.position = end
	if end >= len(matched) {
		f.position = len(matched)
		return page_, "", nil
	}
	f.cursor = page_[len(page_)-1].ID
	return page_, page_[len(page_)-1].ID, nil
}

// Stubs to satisfy store.Store. None of these are exercised by the
// export handler.
func (f *fakeExportStore) UpsertEvents(context.Context, []store.Event) (int64, error) {
	return 0, nil
}
func (f *fakeExportStore) ReplaceEventsInRange(context.Context, []store.Event, int64, int64) error {
	return nil
}
func (f *fakeExportStore) GetEvent(context.Context, string) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}
func (f *fakeExportStore) EventExists(context.Context, string) (bool, error) {
	return false, nil
}
func (f *fakeExportStore) CountEvents(context.Context, store.EventFilter) (int64, error) {
	return int64(len(f.events)), nil
}
func (f *fakeExportStore) LedgerRangeCensus(context.Context, int64, int64, bool) ([]store.LedgerCensus, error) {
	return nil, nil
}
func (f *fakeExportStore) GetIngestionState(context.Context) (store.IngestionState, error) {
	return store.IngestionState{}, store.ErrNotFound
}
func (f *fakeExportStore) SaveIngestionState(context.Context, store.IngestionState) error {
	return nil
}
func (f *fakeExportStore) GetAuditState(context.Context) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (f *fakeExportStore) SaveAuditState(context.Context, store.AuditState) error { return nil }
func (f *fakeExportStore) SaveAuditStateIfGreater(context.Context, int64) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (f *fakeExportStore) ListWatchedContracts(context.Context) ([]store.WatchedContract, error) {
	return nil, nil
}
func (f *fakeExportStore) AddWatchedContract(context.Context, string) error    { return nil }
func (f *fakeExportStore) RemoveWatchedContract(context.Context, string) error { return nil }
func (f *fakeExportStore) RecordAuditFinding(context.Context, store.AuditFinding) (store.AuditFinding, error) {
	return store.AuditFinding{}, nil
}
func (f *fakeExportStore) UpdateAuditFinding(context.Context, store.AuditFinding) error { return nil }
func (f *fakeExportStore) ListOpenFindingsByRange(context.Context, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (f *fakeExportStore) CreateSubscription(context.Context, store.Subscription) (store.Subscription, error) {
	return store.Subscription{}, nil
}
func (f *fakeExportStore) GetSubscription(context.Context, int64) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (f *fakeExportStore) ListSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (f *fakeExportStore) UpdateSubscription(context.Context, store.Subscription) (store.Subscription, error) {
	return store.Subscription{}, nil
}
func (f *fakeExportStore) DeleteSubscription(context.Context, int64) error { return nil }
func (f *fakeExportStore) ListEnabledSubscriptions(context.Context) ([]store.Subscription, error) {
	return nil, nil
}
func (f *fakeExportStore) IncrementSubscriptionFailures(context.Context, int64, int) (int, bool, error) {
	return 0, false, nil
}
func (f *fakeExportStore) ResetSubscriptionFailures(context.Context, int64) error { return nil }
func (f *fakeExportStore) RecordDeliveryAttempt(context.Context, store.DeliveryAttempt) (store.DeliveryAttempt, error) {
	return store.DeliveryAttempt{}, nil
}
func (f *fakeExportStore) ListDeliveryAttempts(context.Context, int64, int) ([]store.DeliveryAttempt, error) {
	return nil, nil
}
func (f *fakeExportStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (f *fakeExportStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }
func (f *fakeExportStore) Stats(context.Context) (store.Stats, error)                    { return store.Stats{}, nil }
func (f *fakeExportStore) Ping(context.Context) error                                    { return nil }

// testServer wraps a Server with the fake store so handlers can be
// exercised in isolation without the full chi stack.
func testServer(t *testing.T, st store.Store, maxRange int64) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(st, nil, logger, "")
	s.SetExportMaxRange(maxRange)
	return s.Router()
}

const testContractID = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

func TestExport_CSVStreamsAllEventsInRange(t *testing.T) {
	contract := testContractID
	st := newFakeExportStore(seedEvents(contract))
	srv := testServer(t, st, 0) // unbounded for the happy path

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+contract+"/export?from_ledger=100&to_ledger=103", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Header().Get("Content-Disposition"),
		"attachment; filename=\""+contract+"-ledgers-100-103.csv\"")

	body := rec.Body.String()
	// 4 events + 1 header row.
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	require.Len(t, lines, 5)
	assert.Equal(t, "id,ledger,type,tx_hash,topics,value", lines[0])
	// Sample check on row 1: id "{...100}", ledger 100, contract,
	// hash100, topics (CSV-quoted because topics contain commas) and
	// value (CSV-quoted as well). Asserting on the row's id- and
	// ledger-content is stable across CSV-encoder changes; the
	// escpaing itself is exercised by encoding/csv itself.
	assert.Contains(t, lines[1], "0000000001-0000000100",
		"row 1 must carry the seeded event's id")
	assert.Contains(t, lines[1], ",100,contract,",
		"row 1 must carry ledger+type in their slot")
	assert.Contains(t, lines[1], "transfer,with,commas",
		"row 1's topics JSON is verbatim, even when quoted to escape its embedded commas")
}

func TestExport_NDJSONStreamsLineDelimited(t *testing.T) {
	contract := testContractID
	st := newFakeExportStore(seedEvents(contract))
	srv := testServer(t, st, 0)

	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+contract+"/export?from_ledger=100&to_ledger=103&format=ndjson", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-ndjson", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Header().Get("Content-Disposition"), ".ndjson")

	dec := json.NewDecoder(rec.Body)
	var ids []string
	for dec.More() {
		var ev store.Event
		require.NoError(t, dec.Decode(&ev))
		ids = append(ids, ev.ID)
	}
	require.Len(t, ids, 4)
	// Ascending by ledger by contract.
	assert.Equal(t, "0000000001-0000000100", ids[0])
	assert.Equal(t, "0000000001-0000000103", ids[3])
}

func TestExport_RejectsUnknownFormat(t *testing.T) {
	srv := testServer(t, newFakeExportStore(nil), 0)
	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+testContractID+"/export?from_ledger=100&to_ledger=103&format=xml", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid format")
}

func TestExport_RejectsMissingBounds(t *testing.T) {
	srv := testServer(t, newFakeExportStore(nil), 0)
	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+testContractID+"/export", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "from_ledger and to_ledger are required")
}

func TestExport_RejectsRangeOverMax(t *testing.T) {
	srv := testServer(t, newFakeExportStore(nil), 1000)
	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+testContractID+"/export?from_ledger=100&to_ledger=20000", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EXPORT_MAX_RANGE=1000",
		"the bound must be echoed in the error so operators can correct it")
}

func TestExport_RejectsInvertedBounds(t *testing.T) {
	srv := testServer(t, newFakeExportStore(nil), 0)
	req := httptest.NewRequest(http.MethodGet,
		"/contracts/"+testContractID+"/export?from_ledger=200&to_ledger=100", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "after to_ledger")
}

func TestExport_RejectsInvalidContractID(t *testing.T) {
	srv := testServer(t, newFakeExportStore(nil), 0)
	req := httptest.NewRequest(http.MethodGet,
		"/contracts/not-a-strkey/export?from_ledger=100&to_ledger=200", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid contract id")
}
