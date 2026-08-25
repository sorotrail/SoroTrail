//go:build integration

package api_test

// GET /events filter combinations exercised end-to-end against a real
// Postgres and the actual HTTP handler (via httptest). Mocks would pass
// these tests but never catch a SQL drift: the column list in
// QueryEvents missing an index the API relies on, the topic containment
// operator receiving a different JSON shape, the cursor narrowing or
// expanding by one event when someone changes the ORDER BY clause.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

// captureLogger buffers the server's log and prints it only when the test
// fails. Discarding it hides the one thing that explains a 500: handlers
// answer with a generic message and log the underlying SQL error, so a
// failing assertion on the status code otherwise tells you nothing.
func captureLogger(t *testing.T) *slog.Logger {
	t.Helper()
	buf := &lockedBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			if out := buf.String(); out != "" {
				t.Logf("server log:\n%s", out)
			}
		}
	})
	return slog.New(slog.NewTextHandler(buf, nil))
}

// lockedBuffer is written from the httptest server's handler goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	apiContractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	apiContractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

type healthOnlyRPC struct{}

func (healthOnlyRPC) GetEvents(context.Context, rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}
func (healthOnlyRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{}, nil
}
func (healthOnlyRPC) GetHealth(context.Context) (rpc.Health, error) {
	return rpc.Health{Status: "healthy"}, nil
}
func (healthOnlyRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}
func (healthOnlyRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

func apiEventID(n int) string { return fmt.Sprintf("%020d-%010d", n, 0) }

// apiSeed builds a deterministic dataset: 10 events split across two
// contracts, event 3 marked diagnostic with a different topic, with
// staggered timestamps to make time-range filters meaningful.
func apiSeed() []store.Event {
	anchor := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	out := make([]store.Event, 0, 10)
	for i := 1; i <= 10; i++ {
		contract := apiContractA
		if i%2 == 0 {
			contract = apiContractB
		}
		e := store.Event{
			ID:               apiEventID(i),
			ContractID:       contract,
			Ledger:           int64(100 + i),
			Type:             "contract",
			TxHash:           "deadbeef",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"},{"u64":7}]`),
			Value:            json.RawMessage(`{"i128":"1000"}`),
			CreatedAt:        anchor.Add(time.Duration(i) * time.Hour),
		}
		if i == 3 {
			e.Type = "diagnostic"
			e.Topics = json.RawMessage(`[{"symbol":"mint"}]`)
		}
		out = append(out, e)
	}
	return out
}

// fromTimeBound is fixed half-way through the seed so the time-range
// assertion intersects events whose timestamps straddle it.
func fromTimeBound() string {
	return time.Date(2026, 7, 21, 17, 0, 0, 0, time.UTC).Format(time.RFC3339)
}

// healthCheckOnly is the minimum rpc.Client the API needs at
// construction time; only /health uses it.
var _ rpc.Client = healthOnlyRPC{}

// TestListEvents_FilterCombinationsAgainstSeededData is the headline
// coverage that pins every documented filter combination against a
// real SQL filter plan.
func TestListEvents_FilterCombinationsAgainstSeededData(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)

	ctx := context.Background()
	if _, err := st.UpsertEvents(ctx, apiSeed()); err != nil {
		t.Fatalf("seeding api events: %v", err)
	}

	log := captureLogger(t)
	srv := httptest.NewServer(api.New(st, healthOnlyRPC{}, log, "test-key").Router())
	t.Cleanup(srv.Close)

	allTen := []string{
		apiEventID(1), apiEventID(2), apiEventID(3), apiEventID(4), apiEventID(5),
		apiEventID(6), apiEventID(7), apiEventID(8), apiEventID(9), apiEventID(10),
	}

	type tcase struct {
		name    string
		path    string
		wantIDs []string
		wantBad bool
	}
	// Event 3 is the deliberate odd one out in apiSeed: type=diagnostic with
	// topics [{"symbol":"mint"}]. Both topic filters below therefore match
	// the other nine, not all ten.
	allButThree := []string{
		apiEventID(1), apiEventID(2), apiEventID(4), apiEventID(5),
		apiEventID(6), apiEventID(7), apiEventID(8), apiEventID(9),
		apiEventID(10),
	}

	cases := []tcase{
		{"no filter", "/events", allTen, false},
		{"by contract A", "/events?contract_id=" + apiContractA,
			[]string{apiEventID(1), apiEventID(3), apiEventID(5), apiEventID(7), apiEventID(9)}, false},
		{"by contract B", "/events?contract_id=" + apiContractB,
			[]string{apiEventID(2), apiEventID(4), apiEventID(6), apiEventID(8), apiEventID(10)}, false},
		{"by ledger range", "/events?from_ledger=104&to_ledger=106",
			[]string{apiEventID(4), apiEventID(5), apiEventID(6)}, false},
		{"by type=diagnostic", "/events?type=diagnostic",
			[]string{apiEventID(3)}, false},
		{"topic match in second position", "/events?topic={\"u64\":7}", allButThree, false},
		{"topic match in first position", "/events?topic={\"symbol\":\"transfer\"}",
			allButThree, false},
		{"intersection: contract + ledger", "/events?contract_id=" + apiContractA + "&from_ledger=104&to_ledger=108",
			[]string{apiEventID(5), apiEventID(7)}, false},
		{"intersection: ledger range + time", "/events?from_ledger=104&to_ledger=106&from_time=" + fromTimeBound(),
			[]string{apiEventID(5), apiEventID(6)}, false},
		{"invalid type rejected", "/events?type=bogus", nil, true},
		{"invalid limit rejected", "/events?limit=99999", nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tc.path)
			require.NoError(t, err)
			defer resp.Body.Close()
			if tc.wantBad {
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
					"path %q must return 400", tc.path)
				return
			}
			require.Equal(t, http.StatusOK, resp.StatusCode, tc.path)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			var got struct {
				Events []store.Event `json:"events"`
				Cursor string        `json:"cursor"`
			}
			require.NoError(t, json.Unmarshal(body, &got), string(body))
			ids := make([]string, 0, len(got.Events))
			for _, e := range got.Events {
				ids = append(ids, e.ID)
			}
			assert.Equal(t, tc.wantIDs, ids,
				"filter %q returned wrong IDs; raw: %s", tc.path, string(body))
		})
	}
}

// TestPagination_BoundaryCases exercises three critical pagination boundaries
// against a real Postgres store:
//  - empty results (no rows match the filter)
//  - single page (all rows fit in one page)
//  - exact-multiple-of-limit (last page returns no cursor)
//
// It also walks a full multi-page cursor chain to verify no rows are
// duplicated or skipped across page boundaries.
func TestPagination_BoundaryCases(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)

	ctx := context.Background()
	if _, err := st.UpsertEvents(ctx, apiSeed()); err != nil {
	t.Fatalf("seeding api events: %v", err)
	}

	log := captureLogger(t)
	srv := httptest.NewServer(api.New(st, healthOnlyRPC{}, log, "test-key").Router())
	t.Cleanup(srv.Close)

	// paginatedResponse is the JSON shape returned by GET /events.
	type paginatedResponse struct {
		Events []store.Event `json:"events"`
		Cursor string        `json:"cursor"`
	}

	fetchPage := func(t *testing.T, path string) paginatedResponse {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", path)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var got paginatedResponse
		require.NoError(t, json.Unmarshal(body, &got), "body: %s", string(body))
		return got
	}

	// collectAllPages follows cursor links until exhausted and returns all
	// event IDs across every page.
	collectAllPages := func(t *testing.T, basePath string, limit int) []string {
		t.Helper()
		var allIDs []string
		cursor := ""
		for i := 0; i < 50; i++ { // safety cap
			path := fmt.Sprintf("%s?limit=%d", basePath, limit)
			if cursor != "" {
				path += "&cursor=" + cursor
			}
			pg := fetchPage(t, path)
			for _, e := range pg.Events {
				allIDs = append(allIDs, e.ID)
			}
			if pg.Cursor == "" {
				return allIDs
			}
			cursor = pg.Cursor
		}
		t.Fatal("pagination did not terminate (safety cap hit)")
		return nil
	}

	t.Run("empty results", func(t *testing.T) {
		// Filter for a contract that has zero events in the seed.
		pg := fetchPage(t, "/events?contract_id=" + apiContractB[0:5] + "ZZZZZZZZZZZZZZZZZZZZZZZZ")
		assert.Empty(t, pg.Events, "empty result must return an empty array")
		assert.Empty(t, pg.Cursor, "empty result must not return a cursor")
	})

	t.Run("single page within default limit", func(t *testing.T) {
		// Default limit is 50; our seed has 10 events, so everything fits
		// in one page and the cursor must be empty.
		pg := fetchPage(t, "/events")
		assert.Equal(t, 10, len(pg.Events), "seed has 10 events")
		assert.Empty(t, pg.Cursor, "all events fit in one page; cursor must be empty")
	})

	t.Run("single page with explicit limit larger than dataset", func(t *testing.T) {
		pg := fetchPage(t, "/events?limit=100")
		assert.Equal(t, 10, len(pg.Events))
		assert.Empty(t, pg.Cursor)
	})

	t.Run("exact multiple of limit", func(t *testing.T) {
		// 10 events / limit 5 = exactly 2 pages.
		pg1 := fetchPage(t, "/events?limit=5")
		assert.Equal(t, 5, len(pg1.Events), "first page must have exactly 5 events")
		assert.NotEmpty(t, pg1.Cursor, "first page must carry a cursor")

		pg2 := fetchPage(t, "/events?limit=5&cursor="+pg1.Cursor)
		assert.Equal(t, 5, len(pg2.Events), "second page must have exactly 5 events")
		assert.Empty(t, pg2.Cursor, "last page must not carry a cursor")
	})

	t.Run("exact multiple of limit — limit=2", func(t *testing.T) {
		// 10 events / limit 2 = exactly 5 pages, each with 2 events.
		cursor := ""
		var pages int
		for i := 0; i < 10; i++ {
			path := "/events?limit=2"
			if cursor != "" {
				path += "&cursor=" + cursor
			}
			pg := fetchPage(t, path)
			assert.Equal(t, 2, len(pg.Events),
				"page %d must have 2 events (got %d)", pages+1, len(pg.Events))
			pages++
			if pg.Cursor == "" {
				break
			}
			cursor = pg.Cursor
		}
		assert.Equal(t, 5, pages, "10 events with limit=2 must produce exactly 5 pages")
	})

	t.Run("non-exact limit leaves partial final page", func(t *testing.T) {
		// 10 events / limit 3 = 3 full pages + 1 partial page (1 event).
		cursor := ""
		var pages int
		for i := 0; i < 10; i++ {
			path := "/events?limit=3"
			if cursor != "" {
				path += "&cursor=" + cursor
			}
			pg := fetchPage(t, path)
			pages++
			if pg.Cursor == "" {
				assert.Equal(t, 1, len(pg.Events),
					"final page must have 1 event (10%%3)")
				break
			}
			assert.Equal(t, 3, len(pg.Events),
				"non-final page %d must have 3 events", pages)
			cursor = pg.Cursor
		}
		assert.Equal(t, 4, pages, "10 events with limit=3 must produce 4 pages")
	})

	t.Run("limit=1 walks every event individually", func(t *testing.T) {
		allIDs := collectAllPages(t, "/events", 1)
		assert.Equal(t, 10, len(allIDs),
			"limit=1 must eventually return all 10 events")

		// Verify no duplicates.
		seen := make(map[string]bool)
		for _, id := range allIDs {
			assert.False(t, seen[id], "duplicate event ID %s across pages", id)
			seen[id] = true
		}
	})

	t.Run("pagination walk returns complete dataset without duplicates", func(t *testing.T) {
		allIDs := collectAllPages(t, "/events", 3)
		assert.Equal(t, 10, len(allIDs),
			"limit=3 must return all 10 events across pages")

		seen := make(map[string]bool)
		for _, id := range allIDs {
			assert.False(t, seen[id], "duplicate event ID %s across pages", id)
			seen[id] = true
		}
	})

	t.Run("filtered pagination — contract A has 5 events", func(t *testing.T) {
		// Contract A has events 1,3,5,7,9 (5 events). limit=2 → 3 pages.
		allIDs := collectAllPages(t, "/events?contract_id="+apiContractA, 2)
		want := []string{
			apiEventID(1), apiEventID(3), apiEventID(5),
			apiEventID(7), apiEventID(9),
		}
		assert.Equal(t, want, allIDs,
			"paginated contract A walk must return the 5 expected events in order")
	})

	t.Run("descending order pagination walk", func(t *testing.T) {
		var allIDs []string
		cursor := ""
		for i := 0; i < 20; i++ {
			path := "/events?limit=4&order=desc"
			if cursor != "" {
				path += "&cursor=" + cursor
			}
			pg := fetchPage(t, path)
			for _, e := range pg.Events {
				allIDs = append(allIDs, e.ID)
			}
			if pg.Cursor == "" {
				break
			}
			cursor = pg.Cursor
		}
		assert.Equal(t, 10, len(allIDs),
			"desc walk must return all 10 events")
		// Newest first: event 10 should come before event 9.
		for i := 0; i < len(allIDs)-1; i++ {
			assert.Greater(t, allIDs[i], allIDs[i+1],
				"desc order must have decreasing IDs at positions %d and %d", i, i+1)
		}
	})

	t.Run("X-Total-Count matches paginated total", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/events?limit=3")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		totalCount := resp.Header.Get("X-Total-Count")
		assert.Equal(t, "10", totalCount,
			"X-Total-Count must be 10 (the total events in the seed)")

		// Also verify the count matches on a filtered query.
		resp2, err := http.Get(srv.URL + "/events?contract_id=" + apiContractA + "&limit=2")
		require.NoError(t, err)
		defer resp2.Body.Close()
		require.Equal(t, http.StatusOK, resp2.StatusCode)
		assert.Equal(t, "5", resp2.Header.Get("X-Total-Count"),
			"X-Total-Count for contract A must be 5")
	})

	t.Run("invalid cursor returns 400", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/events?limit=5&cursor=NOT_A_REAL_CURSOR")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
			"bogus cursor must return 400")
	})

	t.Run("envelope mode preserves empty result shape", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/events?contract_id=NOPE&envelope=true")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var got struct {
			Data       []store.Event `json:"data"`
			NextCursor string        `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(body, &got), "body: %s", string(body))
		assert.NotNil(t, got.Data, "data must be non-nil array (not null)")
		assert.Empty(t, got.Data, "no matches → empty data array")
		assert.Empty(t, got.NextCursor, "no matches → no next_cursor")
	})

	t.Run("envelope mode pagination boundary", func(t *testing.T) {
		path := srv.URL + "/events?limit=5&envelope=true"
		resp, err := http.Get(path)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var got struct {
			Data       []store.Event `json:"data"`
			NextCursor string        `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(body, &got), "body: %s", string(body))
		assert.Equal(t, 5, len(got.Data))
		assert.NotEmpty(t, got.NextCursor, "first page must have next_cursor")

		// Follow cursor to page 2.
		path2 := srv.URL + fmt.Sprintf("/events?limit=5&cursor=%s&envelope=true", got.NextCursor)
		resp2, err := http.Get(path2)
		require.NoError(t, err)
		defer resp2.Body.Close()
		body2, err := io.ReadAll(resp2.Body)
		require.NoError(t, err)
		var got2 struct {
			Data       []store.Event `json:"data"`
			NextCursor string        `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(body2, &got2), "body: %s", string(body2))
		assert.Equal(t, 5, len(got2.Data))
		assert.Empty(t, got2.NextCursor, "second page must have no next_cursor")
	})
}
