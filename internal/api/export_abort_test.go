package api

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

// An export that stops after rows were sent has already answered 200, so the
// consumer needs another way to tell a finished file from a truncated one
// (#1243). These tests run the real server over a real connection, because
// the signal is a broken stream and a recorder cannot show one.

// exportServer serves the fake store through the real router.
func exportServer(t *testing.T, st store.Store) *httptest.Server {
	t.Helper()
	h := testServer(t, st, 0)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// exportResponse is what is left of a response once its body has been read and
// closed: the head and the trailers, which only exist after the body.
type exportResponse struct {
	StatusCode int
	Header     http.Header
	Trailer    http.Header
}

func getExport(t *testing.T, srv *httptest.Server, path string) (exportResponse, []byte, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	return exportResponse{StatusCode: resp.StatusCode, Header: resp.Header, Trailer: resp.Trailer}, body, readErr
}

func TestACompleteExportSaysSoInItsTrailers(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	st.pageSize = 2
	srv := exportServer(t, st)

	for _, format := range []string{"ndjson", "csv"} {
		t.Run(format, func(t *testing.T) {
			st.position, st.cursor, st.callCount = 0, "", 0
			resp, _, err := getExport(t, srv, "/events.csv?format="+format)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "complete", resp.Trailer.Get("X-Export-Status"))
			assert.Equal(t, "4", resp.Trailer.Get("X-Export-Rows"))
		})
	}
}

func TestAnExportThatFailsMidStreamIsABrokenStream(t *testing.T) {
	for _, format := range []string{"ndjson", "csv"} {
		t.Run(format, func(t *testing.T) {
			st := newFakeExportStore(seedEvents(testContractID))
			st.pageSize = 2
			st.errOnCall = 2
			st.queryErr = errors.New("connection to the database was lost")
			srv := exportServer(t, st)

			resp, body, err := getExport(t, srv, "/events.csv?format="+format)

			assert.Equal(t, http.StatusOK, resp.StatusCode, "the status was committed with the first row")
			require.Error(t, err, "the client must see a transport error, not a short but valid body")
			assert.NotEmpty(t, body, "the rows already sent did arrive")
			assert.Empty(t, resp.Trailer.Get("X-Export-Status"),
				"a stopped export never reports itself complete")
		})
	}
}

func TestAContractExportThatFailsMidStreamIsABrokenStream(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	st.pageSize = 2
	st.errOnCall = 2
	st.queryErr = errors.New("boom")
	srv := exportServer(t, st)

	_, _, err := getExport(t, srv, "/contracts/"+testContractID+"/export?from_ledger=100&to_ledger=103&format=ndjson")
	require.Error(t, err)
}

func TestAnExportThatFailsBeforeAnyRowIsAnOrdinaryError(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	st.queryErr = errors.New("database down")
	srv := exportServer(t, st)

	resp, body, err := getExport(t, srv, "/events.csv?format=ndjson")
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "application/json", strings.SplitN(resp.Header.Get("Content-Type"), ";", 2)[0])
	assert.Empty(t, resp.Header.Get("Content-Disposition"), "an error is not a file download")
	assert.Contains(t, string(body), "export failed")
	assert.NotContains(t, string(body), "database down", "store errors are logged, not echoed")
}

func TestNDJSONTerminatorMarksACompleteExport(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	st.pageSize = 2
	srv := exportServer(t, st)

	_, body, err := getExport(t, srv, "/events.csv?format=ndjson&terminator=true")
	require.NoError(t, err)

	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	require.Len(t, lines, 5, "four events and the terminator")
	var last struct {
		Export struct {
			Complete bool  `json:"complete"`
			Rows     int64 `json:"rows"`
		} `json:"export"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[4]), &last))
	assert.True(t, last.Export.Complete)
	assert.Equal(t, int64(4), last.Export.Rows)
}

func TestCSVTerminatorMarksACompleteExport(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	st.pageSize = 2
	srv := exportServer(t, st)

	_, body, err := getExport(t, srv, "/events.csv?terminator=true")
	require.NoError(t, err)

	r := csv.NewReader(strings.NewReader(string(body)))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 6, "header, four events and the terminator")
	assert.Equal(t, []string{"#export-complete", "4"}, records[5])
}

func TestNoTerminatorUnlessAskedFor(t *testing.T) {
	st := newFakeExportStore(seedEvents(testContractID))
	srv := exportServer(t, st)

	_, body, err := getExport(t, srv, "/events.csv?format=ndjson")
	require.NoError(t, err)
	assert.NotContains(t, string(body), `"export"`, "existing consumers see exactly the rows")
}

func TestARejectedTerminatorValueIsA400(t *testing.T) {
	srv := exportServer(t, newFakeExportStore(nil))

	resp, _, err := getExport(t, srv, "/events.csv?terminator=maybe")
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// slowCountingStore pages like the fake store but takes a moment per page, so a
// client that hangs up has time to be noticed, and counts pages atomically.
type slowCountingStore struct {
	*fakeExportStore
	pages atomic.Int64
}

func (s *slowCountingStore) QueryEvents(ctx context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.pages.Add(1)
	time.Sleep(10 * time.Millisecond)
	return s.fakeExportStore.QueryEvents(ctx, f)
}

func TestACancelledClientStopsTheWalk(t *testing.T) {
	st := &slowCountingStore{fakeExportStore: newFakeExportStore(seedManyEvents(testContractID, 600))}
	st.pageSize = 10 // 60 pages, about 600ms if walked to the end
	srv := exportServer(t, st)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events.csv?format=ndjson", nil)
	require.NoError(t, err)
	resp, err := http.DefaultTransport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Read one row, then hang up.
	_, err = bufio.NewReader(resp.Body).ReadString(0x0a)
	require.NoError(t, err)
	cancel()
	_, _ = io.Copy(io.Discard, resp.Body)

	// Wait for the server to settle: the page counter stops moving.
	var last int64 = -1
	require.Eventually(t, func() bool {
		cur := st.pages.Load()
		settled := cur == last
		last = cur
		return settled
	}, 5*time.Second, 150*time.Millisecond)
	assert.Less(t, st.pages.Load(), int64(50),
		"the walk must stop when the client is gone, not run through all 60 pages")
}

func TestCSVCellsCannotBeExecutedAsFormulas(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain text", "contract", "contract"},
		{"equals", "=HYPERLINK(\"http://evil\")", "'=HYPERLINK(\"http://evil\")"},
		{"plus", "+1+1", "'+1+1"},
		{"at", "@SUM(A1)", "'@SUM(A1)"},
		{"leading tab", "\t=1", "'\t=1"},
		{"leading carriage return", "\r=1", "'\r=1"},
		{"minus that is not JSON", "-2+3", "'-2+3"},
		{"a negative JSON number is data, not a formula", "-5", "-5"},
		{"JSON text is untouched", `[{"symbol":"=x"}]`, `[{"symbol":"=x"}]`},
		{"formula in the middle is harmless", "a=b", "a=b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, csvSafe(tt.in))
		})
	}

	row := exportCSVRow(store.Event{ID: "=1", Ledger: 7, Type: "@x", TxHash: "+h", Topics: []byte(`["a"]`), Value: []byte(`-1`)})
	assert.Equal(t, []string{"'=1", "7", "'@x", "'+h", `["a"]`, "-1"}, row,
		"every text column is checked and the ledger number never needs it")
}

func TestExportedRowsAreChargedToTheTenant(t *testing.T) {
	f := newTenantFixture(t)

	rec := f.get(t, f.keyA, "/events.csv?format=ndjson")
	require.Equal(t, http.StatusOK, rec.Code)
	rows := strings.Count(strings.TrimRight(rec.Body.String(), "\n"), "\n") + 1
	require.Positive(t, rows)

	usage := f.get(t, f.keyA, "/tenant/usage") // flushes pending counters first
	require.Equal(t, http.StatusOK, usage.Code)
	var served int64
	for _, u := range f.tenants.usage[1] {
		served += u.EventsServed
	}
	assert.GreaterOrEqual(t, served, int64(rows), "exported rows count against events served")
	_ = strconv.Itoa(rows)
}

func TestRecovererLetsAnAbortedStreamThrough(t *testing.T) {
	r := NewRecoverer(slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "partial")
		panic(http.ErrAbortHandler)
	}))

	assert.PanicsWithValue(t, http.ErrAbortHandler, func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}, "swallowing the abort would end a truncated stream cleanly")
	assert.Zero(t, r.PanicsRecovered(), "a deliberate abort is not a fault")

	other := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("a real bug") }))
	rec := httptest.NewRecorder()
	other.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code, "ordinary panics are still recovered")
	assert.Equal(t, uint64(1), r.PanicsRecovered())
}

// genStore serves total synthetic events one page at a time without ever
// holding more than a page, so any growth in heap during an export is the
// export's own buffering.
type genStore struct {
	store.Store
	total int
	next  int
}

func (g *genStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	n := f.Limit
	if n <= 0 || n > 200 {
		n = 200
	}
	if g.next+n > g.total {
		n = g.total - g.next
	}
	page := make([]store.Event, n)
	for i := range page {
		id := g.next + i
		page[i] = store.Event{
			ID: "ev-" + strconv.Itoa(id), ContractID: testContractID, Ledger: int64(id), Type: "contract",
			TxHash: strings.Repeat("ab", 32),
			Topics: json.RawMessage(`[{"symbol":"transfer"},{"address":"GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}]`),
			Value:  json.RawMessage(`{"i128":"1000000000000000000"}`),
		}
	}
	g.next += n
	if g.next >= g.total {
		return page, "", nil
	}
	return page, "more", nil
}

// discardFlusher is a response writer that drops what it is given.
type discardFlusher struct {
	h     http.Header
	bytes int64
}

func (d *discardFlusher) Header() http.Header { return d.h }
func (d *discardFlusher) WriteHeader(int)     {}
func (d *discardFlusher) Flush()              {}
func (d *discardFlusher) Write(b []byte) (int, error) {
	d.bytes += int64(len(b))
	return len(b), nil
}

// TestExportMemoryIsFlatInTheSizeOfTheResult streams 10x more rows and checks
// the heap does not follow. Buffering the result would put the second run
// tens of megabytes above the first.
func TestExportMemoryIsFlatInTheSizeOfTheResult(t *testing.T) {
	if testing.Short() {
		t.Skip("streams a few hundred thousand rows")
	}
	peak := func(rows int, format exportFormat) (heap uint64, written int64) {
		runtime.GC()
		g := &genStore{total: rows}
		srv := New(g, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
		w := &discardFlusher{h: http.Header{}}

		done := make(chan struct{})
		var max atomic.Uint64
		go func() {
			var ms runtime.MemStats
			for {
				select {
				case <-done:
					return
				default:
				}
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > max.Load() {
					max.Store(ms.HeapAlloc)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
		ctx := WithPrincipal(context.Background(), Principal{Scope: store.WildcardScope()})
		require.NoError(t, srv.streamExport(ctx, w, store.EventFilter{Limit: 200, Scope: store.WildcardScope()}, format))
		close(done)
		return max.Load(), w.bytes
	}

	for _, format := range []exportFormat{formatNDJSON, formatCSV} {
		small, smallBytes := peak(20_000, format)
		large, largeBytes := peak(200_000, format)
		t.Logf("%s: 20k rows (%d MiB written) peaked at %d MiB heap; 200k rows (%d MiB written) peaked at %d MiB",
			format, smallBytes>>20, small>>20, largeBytes>>20, large>>20)

		require.Greater(t, largeBytes, smallBytes*8, "the large run really did write ~10x more")
		const slack = 24 << 20
		assert.Less(t, large, small+slack,
			"%s: heap grew with the size of the result, so rows are being accumulated", format)
		assert.Less(t, large, uint64(64<<20), "%s: absolute heap stayed small", format)
	}
}
