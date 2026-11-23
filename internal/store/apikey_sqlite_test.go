package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func testAPIKey(name, prefix string) APIKey {
	return APIKey{Name: name, Prefix: prefix, KeyHash: "bcrypt-hash-placeholder"}
}
func testSQLiteStore(t *testing.T) *SQLite {
	baseDB, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = baseDB.Close() })
	_, err = baseDB.Exec(`CREATE TABLE IF NOT EXISTS api_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		prefix TEXT NOT NULL UNIQUE,
		key_hash TEXT NOT NULL,
		scopes TEXT NOT NULL DEFAULT '[]',
		created_at TIMESTAMP NOT NULL,
		revoked_at TIMESTAMP
	);`)
	require.NoError(t, err)
	return NewSQLite(baseDB)
}

func TestSQLiteAPIKeyCRUD(t *testing.T) {
	st := testSQLiteStore(t)
	ctx := context.Background()

	created, err := st.CreateAPIKey(ctx, testAPIKey("ops", "aabbccdd0011"))
	require.NoError(t, err)
	assert.Positive(t, created.ID)
	assert.NotZero(t, created.CreatedAt)
	assert.False(t, created.Revoked())

	got, err := st.GetAPIKey(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, "ops", got.Name)
	assert.Equal(t, "aabbccdd0011", got.Prefix)

	byPrefix, err := st.LookupAPIKeyByPrefix(ctx, "aabbccdd0011")
	require.NoError(t, err)
	assert.Equal(t, created.ID, byPrefix.ID)

	_, err = st.LookupAPIKeyByPrefix(ctx, "unknown-prefix")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestSQLiteAPIKeyRevocation(t *testing.T) {
	st := testSQLiteStore(t)
	ctx := context.Background()

	created, err := st.CreateAPIKey(ctx, testAPIKey("tmp", "deadbeef0022"))
	require.NoError(t, err)

	require.NoError(t, st.RevokeAPIKey(ctx, created.ID))
	got, err := st.GetAPIKey(ctx, created.ID)
	require.NoError(t, err)
	assert.NotNil(t, got.RevokedAt)

	_, err = st.LookupAPIKeyByPrefix(ctx, "deadbeef0022")
	assert.ErrorIs(t, err, ErrNotFound)
}
