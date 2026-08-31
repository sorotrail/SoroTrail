//go:build integration

package client_test

// The generated client is exercised against the real HTTP server — the
// chi router over a real Postgres — so a drift between the spec and the
// server's behaviour surfaces here (and in drift_test.go) rather than in
// some consumer's integration. The request/response types the client
// decodes into are generated from the same spec the server's routes are
// checked against, which is what makes the spec load-bearing.

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
	"github.com/sorotrail/sorotrail/pkg/client"
)

const (
	clientContractA = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
	clientContractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
)

func clientEventID(n int) string { return fmt.Sprintf("%020d-%010d", 1000+n, 0) }

// healthRPC is the minimal rpc.Client the API needs; /health is the only
// endpoint that consults it.
type healthRPC struct{}

func (healthRPC) GetEvents(context.Context, rpc.GetEventsRequest) (rpc.GetEventsResponse, error) {
	return rpc.GetEventsResponse{}, nil
}
func (healthRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	return rpc.LatestLedger{}, nil
}
func (healthRPC) GetHealth(context.Context) (rpc.Health, error) {
	return rpc.Health{Status: "healthy"}, nil
}
func (healthRPC) GetLedgerEntries(context.Context, rpc.GetLedgerEntriesRequest) (rpc.GetLedgerEntriesResponse, error) {
	return rpc.GetLedgerEntriesResponse{}, nil
}
func (healthRPC) SimulateTransaction(context.Context, rpc.SimulateTransactionRequest) (rpc.SimulateTransactionResponse, error) {
	return rpc.SimulateTransactionResponse{}, nil
}

// TestClientAgainstRealServer drives the generated client end to end:
// seed events into Postgres, boot the real router, and exercise the
// documented surface through the typed client.
func TestClientAgainstRealServer(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)
	ctx := context.Background()

	seed := make([]store.Event, 0, 6)
	for i := 1; i <= 6; i++ {
		contract := clientContractA
		if i%2 == 0 {
			contract = clientContractB
		}
		seed = append(seed, store.Event{
			ID:               clientEventID(i),
			ContractID:       contract,
			Ledger:           int64(100 + i),
			Type:             "contract",
			TxHash:           "deadbeef",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(`{"i128":"1000"}`),
			CreatedAt:        time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		})
	}
	_, err := st.UpsertEvents(ctx, seed)
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(api.New(st, healthRPC{}, log, "test-key").Router())
	t.Cleanup(srv.Close)

	c := client.New(srv.URL)

	t.Run("Health", func(t *testing.T) {
		got, err := c.Health(ctx)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "ok", got.Status)
		assert.Equal(t, "ok", got.Checks["database"])
	})

	t.Run("Version", func(t *testing.T) {
		got, err := c.Version(ctx)
		require.NoError(t, err)
		require.NotNil(t, got)
		// The test binary is not built with the ldflags, so the fields
		// are the zero values; the shape itself is what matters here.
		assert.IsType(t, "", got.Version)
	})

	t.Run("ListEvents filters by contract", func(t *testing.T) {
		page, err := c.ListEvents(ctx, client.ListEventsParams{
			ContractID: clientContractA,
			Limit:      10,
		})
		require.NoError(t, err)
		require.NotNil(t, page)
		require.Len(t, page.Events, 3, "contract A has the odd-indexed events")
		for _, e := range page.Events {
			assert.Equal(t, clientContractA, e.ContractID)
		}
	})

	t.Run("ListEvents pages with cursor", func(t *testing.T) {
		page1, err := c.ListEvents(ctx, client.ListEventsParams{Limit: 2})
		require.NoError(t, err)
		require.Len(t, page1.Events, 2)
		require.NotEmpty(t, page1.Cursor, "a full page must carry a cursor")

		page2, err := c.ListEvents(ctx, client.ListEventsParams{Limit: 2, Cursor: page1.Cursor})
		require.NoError(t, err)
		require.Len(t, page2.Events, 2)
		// Keyset pagination by ID: the second page continues after the
		// first, never repeating an event.
		assert.Greater(t, page2.Events[0].ID, page1.Events[1].ID)
	})

	t.Run("GetEvent returns the seeded row", func(t *testing.T) {
		got, err := c.GetEvent(ctx, seed[0].ID, client.GetEventParams{})
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, seed[0].ContractID, got.ContractID)
		assert.Equal(t, seed[0].Ledger, got.Ledger)
		assert.Equal(t, int64(0), got.TxIndex, "zero-value fields decode as zero, not garbage")
	})

	t.Run("Stats reflect the seeded rows", func(t *testing.T) {
		got, err := c.Stats(ctx)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, int64(6), got.TotalEvents)
		assert.Equal(t, int64(2), got.ContractCount)
	})

	t.Run("non-2xx maps to APIError", func(t *testing.T) {
		_, err := c.ListEvents(ctx, client.ListEventsParams{Limit: 99999})
		require.Error(t, err)
		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	})
}
