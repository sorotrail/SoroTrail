//go:build integration

package store

// Integration tests for the Postgres contract-metadata methods in
// contract_meta_postgres.go. They need a real database and are skipped unless
// TEST_DATABASE_URL is set (see testStore in postgres_test.go).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	metaContractA = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	metaContractB = "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	metaContractC = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
)

var metaEpoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// metaEventSeq keeps event IDs unique across seedMetaEvents calls in one test,
// so a later call never collides with (and is silently dropped by) the
// idempotent upsert.
var metaEventSeq atomic.Int64

// seedMetaEvents inserts one event per (contract, network) entry with unique IDs.
func seedMetaEvents(t *testing.T, st *Postgres, entries ...struct{ contract, network string }) {
	t.Helper()
	events := make([]Event, 0, len(entries))
	for _, e := range entries {
		seq := metaEventSeq.Add(1)
		ev := testEvent(eventID(int(seq)), 100+seq, e.contract)
		ev.Network = e.network
		ev.CreatedAt = metaEpoch.Add(time.Duration(seq) * time.Second)
		events = append(events, ev)
	}
	n, err := st.UpsertEvents(context.Background(), events)
	require.NoError(t, err)
	require.Equal(t, int64(len(events)), n)
}

type contractNetwork = struct{ contract, network string }

func TestPostgresGetContractMeta(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	t.Run("unknown contract is ErrNotFound", func(t *testing.T) {
		_, err := st.GetContractMeta(ctx, metaContractA)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("a resolved token round-trips every field", func(t *testing.T) {
		want := ContractMeta{
			ContractID: metaContractA,
			Name:       ptr("Wrapped Lumen"),
			Symbol:     ptr("WXLM"),
			Decimals:   ptr(7),
			IsToken:    true,
			FetchedAt:  metaEpoch,
		}
		require.NoError(t, st.UpsertContractMeta(ctx, want))

		got, err := st.GetContractMeta(ctx, metaContractA)
		require.NoError(t, err)
		assert.Equal(t, want.ContractID, got.ContractID)
		assert.Equal(t, "Wrapped Lumen", *got.Name)
		assert.Equal(t, "WXLM", *got.Symbol)
		assert.Equal(t, 7, *got.Decimals)
		assert.True(t, got.IsToken)
		assert.True(t, got.FetchedAt.Equal(metaEpoch), "fetched_at survives: got %s", got.FetchedAt)
		assert.True(t, got.HasMetadata())
	})

	t.Run("null name, symbol and decimals read back as nil", func(t *testing.T) {
		require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
			ContractID: metaContractB, IsToken: true, FetchedAt: metaEpoch,
		}))

		got, err := st.GetContractMeta(ctx, metaContractB)
		require.NoError(t, err)
		assert.Nil(t, got.Name)
		assert.Nil(t, got.Symbol)
		assert.Nil(t, got.Decimals)
		assert.True(t, got.IsToken)
		assert.False(t, got.HasMetadata(), "a token whose fields are all null has no metadata yet")
	})

	t.Run("a negatively cached contract has no metadata", func(t *testing.T) {
		require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
			ContractID: metaContractC, IsToken: false, FetchedAt: metaEpoch,
		}))

		got, err := st.GetContractMeta(ctx, metaContractC)
		require.NoError(t, err)
		assert.False(t, got.IsToken)
		assert.False(t, got.HasMetadata())
	})

	t.Run("partial metadata is not complete", func(t *testing.T) {
		require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
			ContractID: "CPARTIAL", Name: ptr("Only a name"), IsToken: true, FetchedAt: metaEpoch,
		}))
		got, err := st.GetContractMeta(ctx, "CPARTIAL")
		require.NoError(t, err)
		require.NotNil(t, got.Name)
		assert.False(t, got.HasMetadata(), "name, symbol and decimals must all be present")
	})
}

func TestPostgresUpsertContractMetaOverwrites(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
		ContractID: metaContractA, Name: ptr("Old"), Symbol: ptr("OLD"), Decimals: ptr(2), IsToken: true, FetchedAt: metaEpoch,
	}))

	later := metaEpoch.Add(48 * time.Hour)
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
		ContractID: metaContractA, Name: ptr("New"), Symbol: ptr("NEW"), Decimals: ptr(9), IsToken: true, FetchedAt: later,
	}))

	got, err := st.GetContractMeta(ctx, metaContractA)
	require.NoError(t, err)
	assert.Equal(t, "New", *got.Name)
	assert.Equal(t, "NEW", *got.Symbol)
	assert.Equal(t, 9, *got.Decimals)
	assert.True(t, got.FetchedAt.Equal(later))

	// The second write replaces every column, including back to NULL.
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
		ContractID: metaContractA, IsToken: false, FetchedAt: later,
	}))
	got, err = st.GetContractMeta(ctx, metaContractA)
	require.NoError(t, err)
	assert.Nil(t, got.Name)
	assert.Nil(t, got.Symbol)
	assert.Nil(t, got.Decimals)
	assert.False(t, got.IsToken)

	var rows int
	require.NoError(t, st.pool.QueryRow(ctx, `SELECT count(*) FROM contract_meta WHERE contract_id = $1`, metaContractA).Scan(&rows))
	assert.Equal(t, 1, rows, "upserting the same contract never adds a row")
}

func TestPostgresCountContractEvents(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	t.Run("an empty table counts zero", func(t *testing.T) {
		n, err := st.CountContractEvents(ctx, metaContractA)
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})

	seedMetaEvents(t, st,
		contractNetwork{metaContractA, "default"},
		contractNetwork{metaContractB, "default"},
		contractNetwork{metaContractB, "default"},
		contractNetwork{metaContractB, "testnet"},
	)

	tests := []struct {
		name     string
		contract string
		want     int64
	}{
		{"one event for the single-event contract", metaContractA, 1},
		{"counts only that contract's events, across networks", metaContractB, 3},
		{"an unknown contract counts zero", metaContractC, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := st.CountContractEvents(ctx, tt.contract)
			require.NoError(t, err)
			assert.Equal(t, tt.want, n)
		})
	}
}

func TestPostgresListContractIDs(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	t.Run("an empty table lists nothing", func(t *testing.T) {
		ids, err := st.ListContractIDs(ctx)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})

	t.Run("one contract", func(t *testing.T) {
		seedMetaEvents(t, st, contractNetwork{metaContractB, "default"})
		ids, err := st.ListContractIDs(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{metaContractB}, ids)
	})

	t.Run("several contracts are distinct and sorted by ID", func(t *testing.T) {
		seedMetaEvents(t, st,
			contractNetwork{metaContractC, "default"},
			contractNetwork{metaContractA, "default"},
			contractNetwork{metaContractC, "testnet"},
			contractNetwork{metaContractB, "default"},
			contractNetwork{metaContractA, "testnet"},
		)
		ids, err := st.ListContractIDs(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{metaContractA, metaContractB, metaContractC}, ids,
			"each contract appears once, in ascending contract_id order, regardless of network")
	})
}

func TestPostgresListContractsNeedingRefresh(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	cutoff := metaEpoch

	fresh := cutoff.Add(time.Hour)
	stale := cutoff.Add(-time.Hour)

	const (
		neverProbed    = "CNEVERPROBED"
		negativeCached = "CNEGATIVE"
		freshToken     = "CFRESHTOKEN"
		staleToken     = "CSTALETOKEN"
		failedFetch    = "CFAILEDFETCH"
	)
	seedMetaEvents(t, st,
		contractNetwork{neverProbed, "default"},
		contractNetwork{negativeCached, "default"},
		contractNetwork{freshToken, "default"},
		contractNetwork{staleToken, "default"},
		contractNetwork{failedFetch, "default"},
	)
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{ContractID: negativeCached, IsToken: false, FetchedAt: stale}))
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
		ContractID: freshToken, Name: ptr("Fresh"), Symbol: ptr("FRS"), Decimals: ptr(7), IsToken: true, FetchedAt: fresh,
	}))
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{
		ContractID: staleToken, Name: ptr("Stale"), Symbol: ptr("STL"), Decimals: ptr(7), IsToken: true, FetchedAt: stale,
	}))
	require.NoError(t, st.UpsertContractMeta(ctx, ContractMeta{ContractID: failedFetch, IsToken: true, FetchedAt: fresh}))

	got, err := st.ListContractsNeedingRefresh(ctx, cutoff)
	require.NoError(t, err)
	assert.Equal(t, []string{failedFetch, neverProbed, staleToken}, got,
		"never-probed, expired and failed-fetch tokens are returned in contract_id order; "+
			"negatively cached and fresh tokens are not")
}
