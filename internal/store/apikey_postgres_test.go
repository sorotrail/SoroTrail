package store

// Integration tests for API key persistence. They need a real database
// and are skipped unless TEST_DATABASE_URL is set, like the rest of the
// Postgres suite (see postgres_test.go).

import (
	"context"
	"testing"

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
	assert.Equal(t, int64(1), created.ID)
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
