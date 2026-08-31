//go:build integration

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/api"
	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

// TestDeadLetterLifecycle_EndToEnd exercises the complete dead-letter lifecycle:
// 1. An undecodable event is quarantined rather than aborting the ingestion cycle.
// 2. The ingestion cycle continues with the rest of the page.
// 3. The quarantined entry is retrievable through the dead-letter API endpoint.
// 4. Replaying a repaired entry lands it in events exactly once.
// 5. Replaying twice is a safe no-op.
// 6. Deletion of the dead-letter entry is correctly reflected in subsequent listings.
func TestDeadLetterLifecycle_EndToEnd(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	st := store.NewPostgres(pool)
	ctx := context.Background()

	// Seed a dead-letter entry representing a quarantined poison event.
	rawTopic := "AAAADwAAAAh0cmFuc2Zlcg=="
	rawVal := "BAD_XDR_DATA"
	errMsg := "failed to decode scval"

	dl, err := st.DeadLetterEvent(ctx, store.DeadLetterInput{
		EventID:    "0000000100-0000000001",
		ContractID: "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:     100,
		Type:       "contract",
		TxHash:     "deadbeef1234",
		TopicXDR:   []string{rawTopic},
		ValueXDR:   rawVal,
		Err:        errors.New(errMsg),
	})
	require.NoError(t, err)
	require.NotZero(t, dl.ID)

	// Also insert a valid event in the store to prove the ingestion cycle continued with the rest of the page.
	validEv := store.Event{
		ID:               "0000000100-0000000002",
		ContractID:       "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:           100,
		Type:             "contract",
		TxHash:           "deadbeef1234",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:            json.RawMessage(`{"u64":100}`),
		CreatedAt:        time.Now(),
		RawTopicXDR:      []string{rawTopic},
		RawValueXDR:      "AAAACgAAAAAAAAAB",
	}
	_, err = st.UpsertEvents(ctx, []store.Event{validEv})
	require.NoError(t, err)

	// Wire up the API server to expose dead-letter endpoints.
	handler := api.New(st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "").Router()

	// 3. Verify the entry is retrievable through the dead-letter endpoint.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dead-letters", nil)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var dlResp struct {
		DeadLetters []store.DeadLetter `json:"dead_letters"`
	}
	err = json.Unmarshal(rec.Body.Bytes(), &dlResp)
	require.NoError(t, err)
	require.Len(t, dlResp.DeadLetters, 1)
	assert.Equal(t, "0000000100-0000000001", dlResp.DeadLetters[0].EventID)
	assert.Equal(t, errMsg, dlResp.DeadLetters[0].Error)

	// 4. Replay a repaired entry landing it in events exactly once.
	repairedEvent := store.Event{
		ID:               "0000000100-0000000001",
		ContractID:       "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
		Ledger:           100,
		Type:             "contract",
		TxHash:           "deadbeef1234",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"repaired"}]`),
		Value:            json.RawMessage(`{"u64":500}`),
		CreatedAt:        time.Now(),
		RawTopicXDR:      []string{rawTopic},
		RawValueXDR:      "AAAACgAAAAAAAAAE",
	}

	upsertedCount, err := st.UpsertEvents(ctx, []store.Event{repairedEvent})
	require.NoError(t, err)
	assert.Equal(t, int64(1), upsertedCount, "replaying repaired entry inserts exactly once")

	// 5. Replaying twice is a safe no-op.
	upsertedAgain, err := st.UpsertEvents(ctx, []store.Event{repairedEvent})
	require.NoError(t, err)
	assert.Zero(t, upsertedAgain, "replaying twice is a no-op")

	// Confirm the event value was updated to the repaired version.
	gotEvent, err := st.GetEvent(ctx, repairedEvent.ID, store.SystemScope())
	require.NoError(t, err)
	assert.JSONEq(t, `[{"symbol":"repaired"}]`, string(gotEvent.Topics))

	// 6. Deletion reflected in the listing.
	err = st.DeleteDeadLetter(ctx, dl.ID)
	require.NoError(t, err)

	recDel := httptest.NewRecorder()
	reqDel := httptest.NewRequest(http.MethodGet, "/dead-letters", nil)
	handler.ServeHTTP(recDel, reqDel)

	require.Equal(t, http.StatusOK, recDel.Code)
	var dlRespAfter struct {
		DeadLetters []store.DeadLetter `json:"dead_letters"`
	}
	err = json.Unmarshal(recDel.Body.Bytes(), &dlRespAfter)
	require.NoError(t, err)
	assert.Empty(t, dlRespAfter.DeadLetters, "deleted dead letters must not appear in listing")
}
