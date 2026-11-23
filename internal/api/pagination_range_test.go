package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

type captureFilterStore struct {
	stubStore
	lastFilter store.EventFilter
}

func (c *captureFilterStore) QueryEvents(_ context.Context, f store.EventFilter) ([]store.Event, string, error) {
	c.lastFilter = f
	if err := store.ValidateSparseFilterRange(f, store.MaxSparseFilterLedgerSpan()); err != nil {
		return nil, "", err
	}
	ev := store.Event{
		ID:         store.FormatTOIDCursor(120_000, 1, 0, 0),
		ContractID: validAddr('C'),
		Ledger:     120_000,
		Type:       "contract",
		Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:      json.RawMessage(`{"u64":42}`),
	}
	return []store.Event{ev}, ev.ID, nil
}

func TestSparseTopicFilterRangeGuard_HTTP(t *testing.T) {
	SetMaxSparseFilterLedgerSpan(store.DefaultMaxSparseFilterLedgerSpan)
	t.Cleanup(func() { SetMaxSparseFilterLedgerSpan(0) })

	contractID := validAddr('C')
	st := &captureFilterStore{}
	srv := New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-key")

	t.Run("unbounded sparse topic filter returns 400 RANGE_TOO_WIDE with export guidance", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, `/events?topic0={"symbol":"transfer"}`, nil)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		var body errorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, CodeRangeTooWide, body.Code)
		assert.Equal(t, "to_ledger", body.Field)
		assert.Contains(t, body.Error, "/events.csv")
		assert.Contains(t, body.Error, "/contracts/{id}/export")
	})

	t.Run("sparse topic filter exceeding max span returns 400 RANGE_TOO_WIDE", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, `/events?topic_contains=[{"symbol":"transfer"}]&from_ledger=100000&to_ledger=700000`, nil)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		var body errorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, CodeRangeTooWide, body.Code)
		assert.Equal(t, "to_ledger", body.Field)
	})

	t.Run("bounded sparse topic filter within max span succeeds and forwards ledger bounds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, `/events?topic0={"symbol":"transfer"}&from_ledger=100000&to_ledger=300000`, nil)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, store.DefaultMaxSparseFilterLedgerSpan, MaxSparseFilterLedgerSpan())
		assert.Equal(t, int64(100_000), st.lastFilter.FromLedger)
		assert.Equal(t, int64(300_000), st.lastFilter.ToLedger)
	})

	t.Run("non-sparse contract filter over unbounded ledger range succeeds", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/events?contract_id="+contractID, nil)
		rec := httptest.NewRecorder()
		srv.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
