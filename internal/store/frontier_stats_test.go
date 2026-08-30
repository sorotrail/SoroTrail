package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/testdb"
)

func TestFrontierStatsAndAggregateScanHelpers_Integration(t *testing.T) {
	pool := testdb.Setup(t, Migrate)
	st := NewPostgres(pool)
	ctx := context.Background()

	// 1. Empty store reports zeroes rather than erroring or returning nil pointers
	t.Run("empty store reports zeroes", func(t *testing.T) {
		stats, err := st.FrontierStats(ctx, WildcardScope())
		require.NoError(t, err, "frontier stats on an empty table must not error on SQL NULL")
		assert.Equal(t, int64(0), stats.TotalEvents)
		assert.Equal(t, int64(0), stats.MinLedger)
		assert.Equal(t, int64(0), stats.MaxLedger)
		assert.True(t, stats.MinCreatedAt.IsZero())
		assert.True(t, stats.MaxCreatedAt.IsZero())
	})

	// 2. Seed events across different ledgers and scopes, and verify populated figures
	now := time.Now().UTC().Truncate(time.Microsecond)
	e1 := Event{
		ID:               eventID(1),
		Network:          "default",
		ContractID:       contractA,
		Ledger:           100,
		Type:             "contract",
		TxHash:           "hash1",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[]`),
		Value:            json.RawMessage(`null`),
		CreatedAt:        now,
	}
	e2 := Event{
		ID:               eventID(2),
		Network:          "default",
		ContractID:       contractB,
		Ledger:           200,
		Type:             "contract",
		TxHash:           "hash2",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[]`),
		Value:            json.RawMessage(`null`),
		CreatedAt:        now.Add(10 * time.Second),
	}

	_, err := st.UpsertEvents(ctx, []Event{e1, e2})
	require.NoError(t, err)

	t.Run("populated store reports correct figures", func(t *testing.T) {
		stats, err := st.FrontierStats(ctx, WildcardScope())
		require.NoError(t, err)
		assert.Equal(t, int64(2), stats.TotalEvents)
		assert.Equal(t, int64(100), stats.MinLedger)
		assert.Equal(t, int64(200), stats.MaxLedger)
		assert.Equal(t, now.Unix(), stats.MinCreatedAt.Unix())
		assert.Equal(t, now.Add(10*time.Second).Unix(), stats.MaxCreatedAt.Unix())
	})

	t.Run("figures respect the caller scope", func(t *testing.T) {
		scope := Scope{ContractID: contractA}
		stats, err := st.FrontierStats(ctx, scope)
		require.NoError(t, err)
		assert.Equal(t, int64(1), stats.TotalEvents)
		assert.Equal(t, int64(100), stats.MinLedger)
		assert.Equal(t, int64(100), stats.MaxLedger)
		assert.Equal(t, now.Unix(), stats.MinCreatedAt.Unix())
	})
}
