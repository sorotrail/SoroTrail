//go:build integration

package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

func TestAddressIndex_EndToEnd(t *testing.T) {
	pool := testdb.Setup(t, Migrate)
	st := NewPostgres(pool)
	ctx := context.Background() 

	accountAddr := "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"
	contractAddr := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

	// 1. Ingest an event with a topic address and a value address.
	// Both account (G...) and contract (C...) types are represented.
	eventID := eventID(1)
	e := Event{
		ID:               eventID,
		ContractID:       contractA,
		Ledger:           100,
		Type:             "contract",
		TxHash:           "deadbeef1234",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`["symbol", "` + accountAddr + `"]`),
		Value:            json.RawMessage(`{"recipient": "` + contractAddr + `"}`),
		CreatedAt:        time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
	}

	inserted, err := st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	assert.Equal(t, int64(1), inserted)

	// Extract and upsert address references into event_addresses (simulating index-addresses or ingestion extraction)
	extracted := decode.ExtractAddresses(e.Topics, e.Value)
	var refs []AddressRef
	for _, r := range extracted {
		refs = append(refs, AddressRef{
			Address: r.Address,
			EventID: e.ID,
			Role:    r.Role,
		})
	}

	// Should extract both topic[1] and value addresses
	require.Len(t, refs, 2)
	err = st.UpsertAddressRefs(ctx, refs)
	require.NoError(t, err)

	// 2. Re-ingesting the same event and address refs must not duplicate index rows (idempotency)
	_, err = st.UpsertEvents(ctx, []Event{e})
	require.NoError(t, err)
	err = st.UpsertAddressRefs(ctx, refs)
	require.NoError(t, err)

	// 3. Test findability and summary counts matching event listing
	t.Run("findable by account address and role", func(t *testing.T) {
		foundEvents, _, err := st.QueryAddressEvents(ctx, accountAddr, EventFilter{Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, foundEvents, 1)
		assert.Equal(t, eventID, foundEvents[0].ID)

		count, err := st.CountAddressEvents(ctx, accountAddr)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("findable by contract address in value", func(t *testing.T) {
		foundEvents, _, err := st.QueryAddressEvents(ctx, contractAddr, EventFilter{Scope: WildcardScope()})
		require.NoError(t, err)
		require.Len(t, foundEvents, 1)
		assert.Equal(t, eventID, foundEvents[0].ID)

		count, err := st.CountAddressEvents(ctx, contractAddr)
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	// 4. Index reads honouring the caller's Scope
	t.Run("honours caller scope", func(t *testing.T) {
		// Scope that does NOT include contractA
		restrictedScope := NewScope([]string{"CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC_OTHER"})
		foundEvents, _, err := st.QueryAddressEvents(ctx, accountAddr, EventFilter{Scope: restrictedScope})
		require.NoError(t, err)
		assert.Empty(t, foundEvents)
	})
}
