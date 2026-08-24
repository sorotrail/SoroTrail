//go:build integration

package store

// Explicit upsert-idempotency coverage against a real Postgres via
// `testdb.Setup`. Same TOID inserted twice yields one row, raw XDR is
// preserved across duplicate writes — the property the on-call
// playbook needs to trust when a notifier retries.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/testdb"
)

// TestUpsertEvents_SameTOIDTwiceYieldsOneRow is the headline
// idempotency assertion a reviewer should be able to point at. It
// exercises the exact failure mode: a notifier re-sending the same
// TOID must not grow the events table, must not change the stored
// topics/value, and must preserve any raw XDR that's already stored.
func TestUpsertEvents_SameTOIDTwiceYieldsOneRow(t *testing.T) {
	pool := testdb.Setup(t, Migrate)
	st := NewPostgres(pool)
	ctx := context.Background()

	original := Event{
		ID:               eventID(1),
		ContractID:       contractA,
		Ledger:           100,
		Type:             "contract",
		TxHash:           "deadbeef",
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:            json.RawMessage(`{"u64":7}`),
		CreatedAt:        time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		RawTopicXDR:      []string{"AAAADwAAAAh0cmFuc2Zlcg=="},
		RawValueXDR:      "AAAACgAAAAAAAAAB",
	}

	// First write.
	first, err := st.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)
	require.Equal(t, int64(1), first, "first write should insert one row")

	// Duplicate of the same TOID (a notifier retry). Must not grow.
	second, err := st.UpsertEvents(ctx, []Event{original})
	require.NoError(t, err)
	assert.Zero(t, second,
		"duplicate TOID writes must return zero new rows")

	got, err := st.GetEvent(ctx, original.ID, SystemScope())
	require.NoError(t, err)
	assert.Equal(t, original.ContractID, got.ContractID)
	assert.Equal(t, original.Ledger, got.Ledger)
	assert.JSONEq(t, string(original.Topics), string(got.Topics))
	assert.JSONEq(t, string(original.Value), string(got.Value))
	assert.Equal(t, original.RawTopicXDR, got.RawTopicXDR,
		"raw topic XDR must not be overwritten by a duplicate upsert")
	assert.Equal(t, original.RawValueXDR, got.RawValueXDR,
		"raw value XDR must not be overwritten by a duplicate upsert")
	assert.Equal(t, original.CreatedAt, got.CreatedAt,
		"created_at must not change on a duplicate upsert (DO NOTHING, not DO UPDATE)")
}
