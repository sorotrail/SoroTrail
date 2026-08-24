//go:build integration

package api_test

// GET /events filter combinations exercised end-to-end against a real
// Postgres and the actual HTTP handler (via httptest). Mocks would pass
// these tests but never catch a SQL drift: the column list in
// QueryEvents missing an index the API relies on, the topic containment
// operator receiving a different JSON shape, the cursor narrowing or
// expanding by one event when someone changes the ORDER BY clause.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

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

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
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
		{"topic match in second position", "/events?topic={\"u64\":7}", allTen, false},
		{"topic match in first position", "/events?topic={\"symbol\":\"transfer\"}",
			[]string{
				apiEventID(1), apiEventID(2), apiEventID(4), apiEventID(5),
				apiEventID(6), apiEventID(7), apiEventID(8), apiEventID(9),
				apiEventID(10),
			}, false},
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
