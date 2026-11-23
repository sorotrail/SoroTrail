//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAPIKey builds a key row with a fixed fake hash (the store never
// hashes; the apikey package does).
func testAPIKey(name, prefix string) APIKey {
	return APIKey{Name: name, Prefix: prefix, KeyHash: "bcrypt-hash-placeholder"}
}

func TestAPIKeyCRUD(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	created, err := st.CreateAPIKey(ctx, testAPIKey("ops", "aabbccdd0011"))
	require.NoError(t, err)
	assert.Positive(t, created.ID)
	assert.NotZero(t, created.CreatedAt)
	assert.False(t, created.Revoked())
	assert.Equal(t, "bcrypt-hash-placeholder", created.KeyHash, "hash round-trips for verification")

	// The stored key is exactly what was created (no plaintext fields).
	got, err := st.GetAPIKey(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "ops", got.Name)
	assert.Equal(t, "aabbccdd0011", got.Prefix)
	assert.Equal(t, "bcrypt-hash-placeholder", got.KeyHash)
	assert.Nil(t, got.RevokedAt)

	// Lookup by prefix finds the active key.
	byPrefix, err := st.LookupAPIKeyByPrefix(ctx, "aabbccdd0011")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byPrefix.ID)

	// Unknown prefix is not found.
	_, err = st.LookupAPIKeyByPrefix(ctx, "unknown-prefix")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestAPIKeyRevocationIsImmediate(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	created, err := st.CreateAPIKey(ctx, testAPIKey("tmp", "deadbeef0022"))
	require.NoError(t, err)

	// Revoke and verify the flag.
	require.NoError(t, st.RevokeAPIKey(ctx, created.ID))
	got, err := st.GetAPIKey(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.RevokedAt)
	assert.True(t, got.Revoked())

	// The key is no longer resolvable for validation: a revoked key
	// stops working on the very next lookup.
	_, err = st.LookupAPIKeyByPrefix(ctx, "deadbeef0022")
	assert.ErrorIs(t, err, ErrNotFound)

	// Revoking again is a no-op, not an error.
	require.NoError(t, st.RevokeAPIKey(ctx, created.ID))

	// Revoking an unknown key is a not-found.
	assert.ErrorIs(t, st.RevokeAPIKey(ctx, 9999), ErrNotFound)
}

func TestAPIKeyListIncludesRevoked(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	keep, err := st.CreateAPIKey(ctx, testAPIKey("keep", "aaaa00000001"))
	require.NoError(t, err)
	_, err = st.CreateAPIKey(ctx, testAPIKey("drop", "bbbb00000002"))
	require.NoError(t, err)
	require.NoError(t, st.RevokeAPIKey(ctx, keep.ID))

	keys, err := st.ListAPIKeys(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 2, "list shows both active and revoked keys")

	assert.True(t, keys[0].Revoked(), "oldest key is the revoked one")
	assert.False(t, keys[1].Revoked())
}

func TestAPIKeyPrefixIsUnique(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	_, err := st.CreateAPIKey(ctx, testAPIKey("one", "same-prefix"))
	require.NoError(t, err)
	_, err = st.CreateAPIKey(ctx, testAPIKey("two", "same-prefix"))
	require.Error(t, err, "prefix collision must be rejected by the unique index")
}
func TestAPIKeyRotationAndOverlap(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	pred, err := st.CreateAPIKey(ctx, testAPIKey("predecessor", "predprefix001"))
	require.NoError(t, err)

	// Rotate: create successor linked to predecessor with an expiry
	expiresAt := time.Now().Add(1 * time.Hour)
	succ, err := st.CreateAPIKey(ctx, APIKey{
		Name:          "successor",
		Prefix:        "succprefix002",
		KeyHash:       "bcrypt-hash-succ",
		PredecessorID: &pred.ID,
		ExpiresAt:     &expiresAt,
	})
	require.NoError(t, err)

	// Both keys are valid simultaneously during the overlap period
	gotPred, err := st.LookupAPIKeyByPrefix(ctx, "predprefix001")
	require.NoError(t, err)
	assert.Equal(t, pred.ID, gotPred.ID)

	gotSucc, err := st.LookupAPIKeyByPrefix(ctx, "succprefix002")
	require.NoError(t, err)
	assert.Equal(t, succ.ID, gotSucc.ID)
	assert.Equal(t, pred.ID, *gotSucc.PredecessorID)

	// Last-used tracking test
	now := time.Now().UTC()
	err = st.UpdateAPIKeyLastUsed(ctx, gotPred.ID, now)
	require.NoError(t, err)

	updatedPred, err := st.GetAPIKey(ctx, pred.ID)
	require.NoError(t, err)
	require.NotNil(t, updatedPred.LastUsedAt)

	// Abandon rotation: revoke successor, predecessor remains valid
	err = st.RevokeAPIKey(ctx, succ.ID)
	require.NoError(t, err)

	_, err = st.LookupAPIKeyByPrefix(ctx, "succprefix002")
	assert.ErrorIs(t, err, ErrNotFound)

	validPred, err := st.LookupAPIKeyByPrefix(ctx, "predprefix001")
	require.NoError(t, err)
	assert.Equal(t, pred.ID, validPred.ID)
}
