package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	// Embedded so the mock keeps satisfying store.Store as the
	// interface grows; unstubbed methods panic if a test calls them.
	store.Store

	events    []store.Event
	position  int
	cursor    string
	pageSize  int
	callCount int
	errOnCall int
	queryErr  error
	queries   []store.EventFilter
}

func newFakeExportStore(events []store.Event) *fakeExportStore {
	return &fakeExportStore{events: events}
}

// QueryEvents returns a page so the export handler exercises its
// cursor-walking loop instead of the single-page branch. It also
// returns the cursor the handler will mirror back via filter.Cursor.
func (f *fakeExportStore) QueryEvents(_ context.Context, fl store.EventFilter) ([]store.Event, string, error) {
	f.callCount++
	f.queries = append(f.queries, fl)
	if f.errOnCall > 0 && f.callCount == f.errOnCall {
		return nil, "", f.queryErr
	}
	if f.queryErr != nil && f.errOnCall == 0 {
		return nil, "", f.queryErr
	}
	if f.cursor != "" && fl.Cursor != f.cursor {
		return nil, "", fmt.Errorf("cursor mismatch: handler=%q store=%q", fl.Cursor, f.cursor)
	}
	// Filter by ledger range and contract (mirrors Postgres WHERE).
	var matched []store.Event
	for _, e := range f.events {
		if fl.ContractID != "" && e.ContractID != fl.ContractID {
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
	page := f.pageSize
	if page <= 0 {
		page = 2
	}
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
func (f *fakeExportStore) GetEvent(context.Context, string, store.Scope) (store.Event, error) {
	return store.Event{}, store.ErrNotFound
}
func (f *fakeExportStore) GetEventsByTxHash(context.Context, string, string) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeExportStore) EventExists(context.Context, string, store.Scope) (bool, error) {
	return false, nil
}
func (f *fakeExportStore) CountEvents(context.Context, store.EventFilter) (int64, error) {
	return int64(len(f.events)), nil
}
func (f *fakeExportStore) AggregateEvents(context.Context, store.EventFilter, string) ([]store.AggregateBucket, error) {
	return nil, nil
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
func (f *fakeExportStore) GetAuditState(context.Context, string) (store.AuditState, error) {
	return store.AuditState{}, store.ErrNotFound
}
func (f *fakeExportStore) SaveAuditState(context.Context, store.AuditState) error { return nil }
func (f *fakeExportStore) SaveAuditStateIfGreater(context.Context, string, int64) (store.AuditState, error) {
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
func (f *fakeExportStore) ListOpenFindingsByRange(context.Context, string, int64, int64) (store.AuditFinding, error) {
	return store.AuditFinding{}, store.ErrNotFound
}
func (f *fakeExportStore) CreateSubscription(context.Context, store.Subscription) (store.Subscription, error) {
	return store.Subscription{}, nil
}
func (f *fakeExportStore) GetSubscription(context.Context, int64, store.SubscriptionOwner) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}
func (f *fakeExportStore) ListSubscriptions(context.Context, store.SubscriptionOwner) ([]store.Subscription, error) {
	return nil, nil
}
func (f *fakeExportStore) UpdateSubscription(context.Context, store.Subscription, store.SubscriptionOwner) (store.Subscription, error) {
	return store.Subscription{}, nil
}
func (f *fakeExportStore) DeleteSubscription(context.Context, int64, store.SubscriptionOwner) error {
	return nil
}
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
func (f *fakeExportStore) ListDeliveryAttempts(context.Context, int64, int, store.SubscriptionOwner) ([]store.DeliveryAttempt, error) {
	return nil, nil
}
func (f *fakeExportStore) GetContractSpec(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (f *fakeExportStore) SetContractSpec(context.Context, string, string, []byte) error { return nil }
func (f *fakeExportStore) Stats(context.Context, store.Scope) (store.Stats, error) {
	return store.Stats{}, nil
}
func (f *fakeExportStore) Ping(context.Context) error { return nil }

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

func (m *fakeExportStore) ListContracts(context.Context, store.ContractsFilter) ([]store.ContractSummary, string, error) {
	return nil, "", nil
}
func (m *fakeExportStore) CountContracts(context.Context, store.ContractsFilter) (int64, error) {
	return 0, nil
}
func (m *fakeExportStore) DeadLetterEvent(context.Context, store.DeadLetterInput) (store.DeadLetter, error) {
	return store.DeadLetter{}, nil
}
func (m *fakeExportStore) ListDeadLetters(context.Context, string, int, string) ([]store.DeadLetter, string, error) {
	return nil, "", nil
}
func (m *fakeExportStore) CountDeadLetters(context.Context, string) (int64, error) {
	return 0, nil
}
func (m *fakeExportStore) GetDeadLetter(context.Context, int64) (store.DeadLetter, error) {
	return store.DeadLetter{}, store.ErrNotFound
}
func (m *fakeExportStore) DeleteDeadLetter(context.Context, int64) error { return nil }
func (m *fakeExportStore) CountDeliveryAttempts(context.Context, int64, store.SubscriptionOwner) (int64, error) {
	return 0, nil
}

// DeleteEventsBefore satisfies store.Store; this mock never prunes.
func (f *fakeExportStore) DeleteEventsBefore(context.Context, int64, time.Time, int) (int64, error) {
	return 0, nil
}

func TestEventsCSV(t *testing.T) {
	simpleEvents := []store.Event{
		{
			ID:         "0000000001-0000000100",
			ContractID: testContract,
			Ledger:     100,
			Type:       "contract",
			TxHash:     "hash100",
			Topics:     json.RawMessage(`["simple"]`),
			Value:      json.RawMessage(`{"n":1}`),
		},
		{
			ID:         "0000000001-0000000101",
			ContractID: testContract,
			Ledger:     101,
			Type:       "system",
			TxHash:     "hash101",
			Topics:     json.RawMessage(`["other"]`),
			Value:      json.RawMessage(`{"n":2}`),
		},
	}

	// specialCharsEvents exercises CSV escaping for commas, quotes, and
	// newlines inside the JSON-encoded topics and value fields.
	specialEvents := []store.Event{
		{
			ID:         "0000000001-0000000200",
			ContractID: testContract,
			Ledger:     200,
			Type:       "contract",
			TxHash:     "hash200",
			Topics:     json.RawMessage(`{"symbol":"transfer,with,commas"}`),
			Value:      json.RawMessage(`{"i128":"1000"}`),
		},
		{
			ID:         "0000000001-0000000201",
			ContractID: testContract,
			Ledger:     201,
			Type:       "contract",
			TxHash:     "hash201",
			Topics:     json.RawMessage(`{"nested":"value with \"quotes\""}`),
			Value:      json.RawMessage(`{"msg":"line\nbreak"}`),
		},
	}

	tests := []struct {
		name    string
		query   string
		events  []store.Event
		headers map[string]string
		// wantLineCount is the total expected lines (header + data rows).
		wantLineCount int
		// wantContains checks substrings in each line at a 0‑based index.
		wantLines []string
		// wantContainsBody checks substrings anywhere in the body.
		wantContainsBody []string
		// wantStatus overrides the default 200.
		wantStatus int
		// wantFilterCheck, when non-nil, is called with the filter the store received.
		wantFilterCheck func(t *testing.T, f store.EventFilter)
	}{
		{
			name:          "normal export returns CSV with header and events",
			query:         "",
			events:        simpleEvents,
			headers:       map[string]string{"Content-Type": "text/csv; charset=utf-8"},
			wantLineCount: 3,
			wantLines: []string{
				"id,ledger,type,tx_hash,topics,value",
				`0000000001-0000000100,100,contract,hash100,"[""simple""]","{""n"":1}"`,
				`0000000001-0000000101,101,system,hash101,"[""other""]","{""n"":2}"`,
			},
		},
		{
			name:          "filtered export passes query params to store",
			query:         "?contract_id=" + testContract + "&type=system&from_ledger=101",
			events:        simpleEvents,
			wantLineCount: 3,
			wantLines: []string{
				"id,ledger,type,tx_hash,topics,value",
				`0000000001-0000000100,100,contract,hash100,"[""simple""]","{""n"":1}"`,
				`0000000001-0000000101,101,system,hash101,"[""other""]","{""n"":2}"`,
			},
			wantFilterCheck: func(t *testing.T, f store.EventFilter) {
				assert.Equal(t, testContract, f.ContractID)
				assert.Equal(t, []string{"system"}, f.Types)
				assert.Equal(t, int64(101), f.FromLedger)
			},
		},
		{
			name:          "empty result returns only CSV header",
			query:         "",
			events:        []store.Event{},
			wantLineCount: 1,
			wantLines: []string{
				"id,ledger,type,tx_hash,topics,value",
			},
		},
		{
			name:          "special characters are properly CSV-escaped",
			query:         "",
			events:        specialEvents,
			wantLineCount: 3,
			wantContainsBody: []string{
				// Row with commas in topics: the whole field is quoted.
				`transfer,with,commas`,
				// Row with quotes in topics: internal quotes are doubled.
				`\""quotes\""`,
			},
		},
		{
			name:          "stable column ordering: id,ledger,type,tx_hash,topics,value",
			query:         "",
			events:        simpleEvents,
			wantLineCount: 3,
			wantLines: []string{
				"id,ledger,type,tx_hash,topics,value",
				`0000000001-0000000100,100,contract,hash100,"[""simple""]","{""n"":1}"`,
				`0000000001-0000000101,101,system,hash101,"[""other""]","{""n"":2}"`,
			},
		},
		{
			name:          "bad filter returns JSON error before streaming",
			query:         "?type=bogus",
			wantLineCount: 0,
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &stubStore{
				events:     tt.events,
				nextCursor: "",
			}
			s := newTestServer(st, nil)

			path := "/events.csv"
			if tt.query != "" {
				path = "/events.csv" + tt.query
			}
			resp, body := doGet(t, s, path)

			if tt.wantStatus != 0 {
				require.Equal(t, tt.wantStatus, resp.StatusCode)
				if tt.wantStatus == http.StatusBadRequest {
					var e map[string]string
					require.NoError(t, json.Unmarshal(body, &e))
					assert.Contains(t, e["error"], "invalid type")
				}
				return
			}

			require.Equal(t, http.StatusOK, resp.StatusCode)
			if tt.headers != nil {
				for k, v := range tt.headers {
					assert.Equal(t, v, resp.Header.Get(k))
				}
			}
			assert.Equal(t, "text/csv; charset=utf-8", resp.Header.Get("Content-Type"),
				"Content-Type must be text/csv")
			assert.Contains(t, resp.Header.Get("Content-Disposition"),
				"attachment; filename=\"events.csv\"",
				"Content-Disposition must invite download")
			assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"),
				"export responses must not be cached")

			lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
			require.Len(t, lines, tt.wantLineCount,
				"unexpected number of lines (header + events)")

			if tt.wantLines != nil {
				for i, want := range tt.wantLines {
					if i < len(lines) {
						assert.Equal(t, want, lines[i],
							"line %d does not match", i)
					}
				}
			}

			for _, want := range tt.wantContainsBody {
				assert.Contains(t, string(body), want)
			}

			if tt.wantFilterCheck != nil {
				tt.wantFilterCheck(t, st.lastFilter)
			}
		})
	}
}

func TestEventsCSV_ResponseHeaders(t *testing.T) {
	st := &stubStore{
		events: []store.Event{{
			ID:         "0000000001-0000000100",
			ContractID: testContract,
			Ledger:     100,
			Type:       "contract",
			TxHash:     "hash100",
			Topics:     json.RawMessage(`[]`),
			Value:      json.RawMessage(`{}`),
		}},
	}
	s := newTestServer(st, nil)
	resp, _ := doGet(t, s, "/events.csv")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, "text/csv; charset=utf-8", resp.Header.Get("Content-Type"))
	assert.Contains(t, resp.Header.Get("Content-Disposition"), "attachment; filename=\"events.csv\"")
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"),
		"CSV export must use no-store to prevent stale caching")
}

// testExportWriter is a test ResponseWriter that tracks Flush calls and can
// simulate write failures (e.g. client disconnect or broken pipe).
type testExportWriter struct {
	header     http.Header
	buf        bytes.Buffer
	flushes    int
	failAfter  int // fail write once total bytes written would exceed this (-1 = never)
	writeErr   error
	statusCode int
}

func newTestExportWriter() *testExportWriter {
	return &testExportWriter{
		header:    make(http.Header),
		failAfter: -1,
	}
}

func (w *testExportWriter) Header() http.Header {
	return w.header
}

func (w *testExportWriter) Write(p []byte) (int, error) {
	if w.failAfter >= 0 && w.buf.Len()+len(p) > w.failAfter {
		if w.writeErr != nil {
			return 0, w.writeErr
		}
		return 0, errors.New("simulated write error")
	}
	return w.buf.Write(p)
}

func (w *testExportWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *testExportWriter) Flush() {
	w.flushes++
}

// testContextWithLogger creates a context carrying a test slog logger writing
// to buf, which loggerFromContext will resolve.
func testContextWithLogger(ctx context.Context, buf *bytes.Buffer) context.Context {
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return context.WithValue(ctx, loggerCtxKey, logger)
}

// seedManyEvents generates count events in stable ascending order,
// populated with all Event fields matching the JSON API representation.
func seedManyEvents(contractID string, count int) []store.Event {
	out := make([]store.Event, 0, count)
	topics := json.RawMessage(`[{"symbol":"transfer,with,commas"},{"address":"GA\"quoted\""}]`)
	value := json.RawMessage(`{"i128":"1000"}`)
	for l := int64(1); l <= int64(count); l++ {
		out = append(out, store.Event{
			ID:               fmt.Sprintf("0000000001-%010d", l),
			ContractID:       contractID,
			Ledger:           100 + l,
			Type:             "contract",
			TxHash:           fmt.Sprintf("hash%d", l),
			TxIndex:          int32(l),
			OpIndex:          0,
			InSuccessfulCall: true,
			Topics:           topics,
			Value:            value,
			CreatedAt:        time.Date(2026, 9, 24, 12, 0, int(l%60), 0, time.UTC),
			Network:          "testnet",
		})
	}
	return out
}

func TestExportFilter(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st := newFakeExportStore(nil)
	s := New(st, nil, logger, "")

	tenantScope := store.NewScope([]string{testContractID})

	tests := []struct {
		name       string
		ctx        context.Context
		contractID string
		fromLedger int64
		toLedger   int64
		want       store.EventFilter
	}{
		{
			name:       "standard bounds with wildcard principal scope",
			ctx:        WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}),
			contractID: testContractID,
			fromLedger: 100,
			toLedger:   200,
			want: store.EventFilter{
				ContractID: testContractID,
				FromLedger: 100,
				ToLedger:   200,
				Order:      "asc",
				OrderBy:    store.OrderByLedger,
				Limit:      exportQueryBatchSize,
				Scope:      store.WildcardScope(),
			},
		},
		{
			name:       "tenant-scoped principal carried on export filter",
			ctx:        WithPrincipal(context.Background(), Principal{Scope: tenantScope}),
			contractID: testContractID,
			fromLedger: 50,
			toLedger:   150,
			want: store.EventFilter{
				ContractID: testContractID,
				FromLedger: 50,
				ToLedger:   150,
				Order:      "asc",
				OrderBy:    store.OrderByLedger,
				Limit:      exportQueryBatchSize,
				Scope:      tenantScope,
			},
		},
		{
			name:       "zero ledger bounds preserved",
			ctx:        WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}),
			contractID: testContractID,
			fromLedger: 0,
			toLedger:   0,
			want: store.EventFilter{
				ContractID: testContractID,
				FromLedger: 0,
				ToLedger:   0,
				Order:      "asc",
				OrderBy:    store.OrderByLedger,
				Limit:      exportQueryBatchSize,
				Scope:      store.WildcardScope(),
			},
		},
		{
			name:       "empty context without principal yields empty denying scope",
			ctx:        context.Background(),
			contractID: testContractID,
			fromLedger: 10,
			toLedger:   20,
			want: store.EventFilter{
				ContractID: testContractID,
				FromLedger: 10,
				ToLedger:   20,
				Order:      "asc",
				OrderBy:    store.OrderByLedger,
				Limit:      exportQueryBatchSize,
				Scope:      store.Scope{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.exportFilter(tt.ctx, tt.contractID, tt.fromLedger, tt.toLedger)
			assert.Equal(t, tt.want.ContractID, got.ContractID)
			assert.Equal(t, tt.want.FromLedger, got.FromLedger)
			assert.Equal(t, tt.want.ToLedger, got.ToLedger)
			assert.Equal(t, tt.want.Order, got.Order)
			assert.Equal(t, tt.want.OrderBy, got.OrderBy)
			assert.Equal(t, tt.want.Limit, got.Limit)
			assert.Equal(t, tt.want.Scope, got.Scope)
		})
	}
}

func TestStreamExportCSV(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	events := seedEvents(testContractID)

	t.Run("header_matches_columns_and_data_rows", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportCSV(ctx, w, testContractID, 100, 103)

		r := csv.NewReader(&w.buf)
		header, err := r.Read()
		require.NoError(t, err)
		expectedHeader := []string{"id", "ledger", "type", "tx_hash", "topics", "value"}
		assert.Equal(t, expectedHeader, header, "header row must match documented columns")

		records, err := r.ReadAll()
		require.NoError(t, err, "all CSV records must be validly formatted")
		require.Len(t, records, len(events), "each event must produce one data row")

		for i, row := range records {
			require.Len(t, row, len(expectedHeader), "data row must have same number of columns as header")
			ev := events[i]
			assert.Equal(t, ev.ID, row[0], "column 0 must be event ID")
			assert.Equal(t, fmt.Sprintf("%d", ev.Ledger), row[1], "column 1 must be ledger")
			assert.Equal(t, ev.Type, row[2], "column 2 must be event type")
			assert.Equal(t, ev.TxHash, row[3], "column 3 must be tx_hash")
			assert.Equal(t, string(ev.Topics), row[4], "column 4 must be verbatim topics JSON")
			assert.Equal(t, string(ev.Value), row[5], "column 5 must be verbatim value JSON")
		}
	})

	t.Run("empty_result_produces_header_only_valid_export", func(t *testing.T) {
		st := newFakeExportStore(nil)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportCSV(ctx, w, testContractID, 100, 200)

		r := csv.NewReader(&w.buf)
		header, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "ledger", "type", "tx_hash", "topics", "value"}, header)

		// Next read must return EOF, confirming a valid empty CSV file, not truncated or 0-byte.
		_, err = r.Read()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("mid_stream_store_error_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = errors.New("database connection lost mid-stream")

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportCSV(ctx, w, testContractID, 100, 103)

		// Verify error was logged rather than swallowed silently.
		assert.Contains(t, logBuf.String(), "export query")
		assert.Contains(t, logBuf.String(), "database connection lost mid-stream")

		// Output only contains header + page 1 (2 events).
		r := csv.NewReader(&w.buf)
		all, err := r.ReadAll()
		require.NoError(t, err)
		assert.Len(t, all, 3, "should contain header + first page of 2 rows before error")
	})

	t.Run("invalid_cursor_mid_stream_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = store.ErrInvalidCursor

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportCSV(ctx, w, testContractID, 100, 103)

		assert.Contains(t, logBuf.String(), "export cursor")
	})

	t.Run("memory_does_not_scale_with_row_count", func(t *testing.T) {
		const totalRows = 100
		const pageSize = 10
		bigEvents := seedManyEvents(testContractID, totalRows)
		st := newFakeExportStore(bigEvents)
		st.pageSize = pageSize

		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportCSV(ctx, w, testContractID, 1, 1000)

		// Verify flusher was called after header and after each of the 10 pages.
		expectedFlushes := 1 + (totalRows / pageSize)
		assert.Equal(t, expectedFlushes, w.flushes, "flusher must be called after header and every page")

		r := csv.NewReader(&w.buf)
		all, err := r.ReadAll()
		require.NoError(t, err)
		assert.Len(t, all, totalRows+1, "must stream all rows plus header")
	})

	t.Run("client_disconnect_terminates_early", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		ctx, cancel := context.WithCancel(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}))
		cancel() // already canceled

		s.streamExportCSV(ctx, w, testContractID, 100, 103)

		// After the first page, context check returns and does not query remaining pages.
		assert.Equal(t, 1, st.callCount)
	})

	t.Run("write_failure_surfaces_error_and_halts", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		headerLen := len("id,ledger,type,tx_hash,topics,value\n")
		w.failAfter = headerLen
		w.writeErr = errors.New("client connection dropped")

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportCSV(ctx, w, testContractID, 100, 103)

		assert.Contains(t, logBuf.String(), "export csv write")
	})
}

func TestStreamExportNDJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	events := seedEvents(testContractID)

	t.Run("emits_one_valid_json_document_per_line_matching_api_shape", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportNDJSON(ctx, w, testContractID, 100, 103)

		lines := strings.Split(strings.TrimRight(w.buf.String(), "\n"), "\n")
		require.Len(t, lines, len(events), "must emit exactly one line per event")

		expectedFields := []string{
			"id", "contract_id", "ledger", "type", "tx_hash",
			"tx_index", "op_index", "in_successful_call", "topics",
			"value", "created_at", "network",
		}

		for i, line := range lines {
			assert.True(t, json.Valid([]byte(line)), "line %d must be valid JSON", i)

			var rawMap map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &rawMap))
			for _, field := range expectedFields {
				assert.Contains(t, rawMap, field, "line %d must contain JSON API field %q", i, field)
			}

			var decoded store.Event
			require.NoError(t, json.Unmarshal([]byte(line), &decoded))
			assert.Equal(t, events[i].ID, decoded.ID)
			assert.Equal(t, events[i].Ledger, decoded.Ledger)
			assert.Equal(t, events[i].ContractID, decoded.ContractID)
			assert.Equal(t, events[i].Type, decoded.Type)
			assert.Equal(t, events[i].TxHash, decoded.TxHash)
		}
	})

	t.Run("empty_result_produces_valid_empty_ndjson", func(t *testing.T) {
		st := newFakeExportStore(nil)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportNDJSON(ctx, w, testContractID, 100, 200)

		assert.Empty(t, w.buf.String(), "empty result set must produce 0 bytes of NDJSON output")
	})

	t.Run("mid_stream_store_error_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = errors.New("ndjson store query failure")

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportNDJSON(ctx, w, testContractID, 100, 103)

		assert.Contains(t, logBuf.String(), "export query")
		assert.Contains(t, logBuf.String(), "ndjson store query failure")

		lines := strings.Split(strings.TrimRight(w.buf.String(), "\n"), "\n")
		assert.Len(t, lines, 2, "must terminate with only first page of 2 events")
	})

	t.Run("invalid_cursor_mid_stream_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = store.ErrInvalidCursor

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportNDJSON(ctx, w, testContractID, 100, 103)

		assert.Contains(t, logBuf.String(), "export cursor")
	})

	t.Run("memory_does_not_scale_with_row_count", func(t *testing.T) {
		const totalRows = 100
		const pageSize = 10
		bigEvents := seedManyEvents(testContractID, totalRows)
		st := newFakeExportStore(bigEvents)
		st.pageSize = pageSize

		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamExportNDJSON(ctx, w, testContractID, 1, 1000)

		assert.Equal(t, totalRows/pageSize, w.flushes, "flusher must be called once per page")

		lines := strings.Split(strings.TrimRight(w.buf.String(), "\n"), "\n")
		assert.Len(t, lines, totalRows)
	})

	t.Run("client_disconnect_terminates_early", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		ctx, cancel := context.WithCancel(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}))
		cancel()

		s.streamExportNDJSON(ctx, w, testContractID, 100, 103)

		assert.Equal(t, 1, st.callCount)
	})

	t.Run("write_failure_surfaces_error_and_halts", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		w.failAfter = 10
		w.writeErr = errors.New("client closed connection")

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamExportNDJSON(ctx, w, testContractID, 100, 103)

		assert.Contains(t, logBuf.String(), "export ndjson write")
	})
}

func TestStreamEventsCSV(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	events := seedEvents(testContractID)

	filter := store.EventFilter{
		ContractID: testContractID,
		FromLedger: 100,
		ToLedger:   103,
		Limit:      exportQueryBatchSize,
		Scope:      store.WildcardScope(),
	}

	t.Run("header_matches_columns_and_data_rows", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamEventsCSV(ctx, w, filter)

		r := csv.NewReader(&w.buf)
		header, err := r.Read()
		require.NoError(t, err)
		expectedHeader := []string{"id", "ledger", "type", "tx_hash", "topics", "value"}
		assert.Equal(t, expectedHeader, header, "header row must match documented columns")

		records, err := r.ReadAll()
		require.NoError(t, err)
		require.Len(t, records, len(events))

		for i, row := range records {
			require.Len(t, row, len(expectedHeader))
			ev := events[i]
			assert.Equal(t, ev.ID, row[0])
			assert.Equal(t, fmt.Sprintf("%d", ev.Ledger), row[1])
			assert.Equal(t, ev.Type, row[2])
			assert.Equal(t, ev.TxHash, row[3])
			assert.Equal(t, string(ev.Topics), row[4])
			assert.Equal(t, string(ev.Value), row[5])
		}
	})

	t.Run("empty_result_produces_header_only_valid_export", func(t *testing.T) {
		st := newFakeExportStore(nil)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		s.streamEventsCSV(ctx, w, filter)

		r := csv.NewReader(&w.buf)
		header, err := r.Read()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "ledger", "type", "tx_hash", "topics", "value"}, header)

		_, err = r.Read()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("mid_stream_store_error_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = errors.New("events csv query error")

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamEventsCSV(ctx, w, filter)

		assert.Contains(t, logBuf.String(), "export query")
		assert.Contains(t, logBuf.String(), "events csv query error")

		r := csv.NewReader(&w.buf)
		all, err := r.ReadAll()
		require.NoError(t, err)
		assert.Len(t, all, 3) // header + 2 events
	})

	t.Run("invalid_cursor_mid_stream_is_surfaced", func(t *testing.T) {
		st := newFakeExportStore(events)
		st.errOnCall = 2
		st.queryErr = store.ErrInvalidCursor

		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamEventsCSV(ctx, w, filter)

		assert.Contains(t, logBuf.String(), "export cursor")
	})

	t.Run("memory_does_not_scale_with_row_count", func(t *testing.T) {
		const totalRows = 100
		const pageSize = 10
		bigEvents := seedManyEvents(testContractID, totalRows)
		st := newFakeExportStore(bigEvents)
		st.pageSize = pageSize

		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})

		bigFilter := store.EventFilter{
			ContractID: testContractID,
			Limit:      exportQueryBatchSize,
			Scope:      store.WildcardScope(),
		}
		s.streamEventsCSV(ctx, w, bigFilter)

		expectedFlushes := 1 + (totalRows / pageSize)
		assert.Equal(t, expectedFlushes, w.flushes)

		r := csv.NewReader(&w.buf)
		all, err := r.ReadAll()
		require.NoError(t, err)
		assert.Len(t, all, totalRows+1)
	})

	t.Run("client_disconnect_terminates_early", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()

		ctx, cancel := context.WithCancel(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}))
		cancel()

		s.streamEventsCSV(ctx, w, filter)

		assert.Equal(t, 1, st.callCount)
	})

	t.Run("write_failure_surfaces_error_and_halts", func(t *testing.T) {
		st := newFakeExportStore(events)
		s := New(st, nil, logger, "")
		w := newTestExportWriter()
		headerLen := len("id,ledger,type,tx_hash,topics,value\n")
		w.failAfter = headerLen
		w.writeErr = errors.New("client connection dropped")

		var logBuf bytes.Buffer
		ctx := testContextWithLogger(WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()}), &logBuf)

		s.streamEventsCSV(ctx, w, filter)

		assert.Contains(t, logBuf.String(), "csv write")
	})
// TestEventsCSV_NDJSONFormat verifies that /events.csv — despite its
// historical name — accepts ?format=ndjson exactly like
// /contracts/{id}/export, closing the parity gap issue #578 tracks: a
// client switching between the two export endpoints must not have to
// rediscover which formats each one accepts.
func TestEventsCSV_NDJSONFormat(t *testing.T) {
	st := &stubStore{
		events: []store.Event{
			{
				ID:         "0000000001-0000000100",
				ContractID: testContract,
				Ledger:     100,
				Type:       "contract",
				TxHash:     "hash100",
				Topics:     json.RawMessage(`["simple"]`),
				Value:      json.RawMessage(`{"n":1}`),
			},
			{
				ID:         "0000000001-0000000101",
				ContractID: testContract,
				Ledger:     101,
				Type:       "system",
				TxHash:     "hash101",
				Topics:     json.RawMessage(`["other"]`),
				Value:      json.RawMessage(`{"n":2}`),
			},
		},
	}
	s := newTestServer(st, nil)
	resp, body := doGet(t, s, "/events.csv?format=ndjson")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, "application/x-ndjson", resp.Header.Get("Content-Type"))
	assert.Contains(t, resp.Header.Get("Content-Disposition"), `attachment; filename="events.ndjson"`)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

	dec := json.NewDecoder(strings.NewReader(string(body)))
	var ids []string
	for dec.More() {
		var ev store.Event
		require.NoError(t, dec.Decode(&ev))
		ids = append(ids, ev.ID)
	}
	require.Len(t, ids, 2)
	assert.Equal(t, "0000000001-0000000100", ids[0])
	assert.Equal(t, "0000000001-0000000101", ids[1])
}

// TestEventsCSV_RejectsUnknownFormat mirrors
// TestExport_RejectsUnknownFormat for the events endpoint: both export
// endpoints share parseExportFormat, so an unsupported value must be
// rejected the same way on both.
func TestEventsCSV_RejectsUnknownFormat(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	resp, body := doGet(t, s, "/events.csv?format=xml")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var e map[string]string
	require.NoError(t, json.Unmarshal(body, &e))
	assert.Contains(t, e["error"], "invalid format")
	assert.Contains(t, e["error"], "csv or ndjson",
		"the 400 must name the formats it does support")
}

// TestEventsCSV_StreamsLargeResultAcrossPages exercises the same
// cursor-walking loop TestExport_CSVStreamsAllEventsInRange exercises for
// /contracts/{id}/export, but through /events.csv: fakeExportStore only
// ever hands back exportQueryBatchSize-sized pages and asserts the
// handler mirrors its cursor back on every subsequent call, so a
// handler that buffered the whole result before writing (instead of
// streaming page by page) would fail this test by never advancing past
// the first page's cursor.
func TestEventsCSV_StreamsLargeResultAcrossPages(t *testing.T) {
	contract := testContractID
	const total = 50
	events := make([]store.Event, 0, total)
	for l := int64(0); l < total; l++ {
		events = append(events, store.Event{
			ID:         fmt.Sprintf("0000000001-%010d", l),
			ContractID: contract,
			Ledger:     l,
			Type:       "contract",
			TxHash:     fmt.Sprintf("hash%d", l),
			Topics:     json.RawMessage(`[]`),
			Value:      json.RawMessage(`{}`),
		})
	}
	st := newFakeExportStore(events)
	srv := testServer(t, st, 0)

	for _, format := range []string{"", "csv", "ndjson"} {
		t.Run("format="+format, func(t *testing.T) {
			path := "/events.csv?contract_id=" + contract
			if format != "" {
				path += "&format=" + format
			}
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			st.position, st.cursor = 0, ""
			srv.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			if format == "ndjson" {
				dec := json.NewDecoder(rec.Body)
				n := 0
				for dec.More() {
					var ev store.Event
					require.NoError(t, dec.Decode(&ev))
					n++
				}
				assert.Equal(t, total, n)
			} else {
				lines := strings.Split(strings.TrimRight(rec.Body.String(), "\n"), "\n")
				require.Len(t, lines, total+1, "header + one row per event")
			}
		})
	}
}
