package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runIdempotencyStoreContractSuite(t *testing.T, newBackend func(t *testing.T) IdempotencyStore) {
	t.Helper()

	t.Run("claim complete and replay", func(t *testing.T) {
		st := newBackend(t)
		ctx := context.Background()
		now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

		reqHash := HashIdempotencyRequest("POST", "/subscriptions", []byte(`{"url":"https://a.example/hook"}`))
		rec := IdempotencyRecord{
			TenantID:    "tenant:1",
			Key:         "key-001",
			RequestHash: reqHash,
			Method:      "POST",
			Path:        "/subscriptions",
			CreatedAt:   now,
			ExpiresAt:   now.Add(DefaultIdempotencyTTL),
		}

		outcome, claimed, err := st.ClaimIdempotencyKey(ctx, rec, now)
		require.NoError(t, err)
		assert.Equal(t, IdempotencyStarted, outcome)
		assert.Equal(t, 0, claimed.StatusCode)

		// While in-flight (status_code == 0), a second claim with the same hash
		// reports ErrIdempotencyInFlight.
		_, _, err = st.ClaimIdempotencyKey(ctx, rec, now.Add(time.Second))
		require.ErrorIs(t, err, ErrIdempotencyInFlight)

		// Completing the key stores status, content-type, and body.
		rec.StatusCode = 201
		rec.ContentType = "application/json"
		rec.ResponseBody = []byte(`{"id":42,"url":"https://a.example/hook"}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, rec))

		// Subsequent claim with the same key and request hash replays the stored response.
		outcome, replayed, err := st.ClaimIdempotencyKey(ctx, rec, now.Add(2*time.Second))
		require.NoError(t, err)
		assert.Equal(t, IdempotencyReplayed, outcome)
		assert.Equal(t, 201, replayed.StatusCode)
		assert.Equal(t, "application/json", replayed.ContentType)
		assert.JSONEq(t, `{"id":42,"url":"https://a.example/hook"}`, string(replayed.ResponseBody))
	})

	t.Run("body or route mismatch rejected", func(t *testing.T) {
		st := newBackend(t)
		ctx := context.Background()
		now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

		rec := IdempotencyRecord{
			TenantID:    "tenant:1",
			Key:         "key-mismatch",
			RequestHash: HashIdempotencyRequest("POST", "/watched-contracts", []byte(`{"contract_id":"C1"}`)),
			Method:      "POST",
			Path:        "/watched-contracts",
			CreatedAt:   now,
			ExpiresAt:   now.Add(DefaultIdempotencyTTL),
		}
		outcome, _, err := st.ClaimIdempotencyKey(ctx, rec, now)
		require.NoError(t, err)
		require.Equal(t, IdempotencyStarted, outcome)
		rec.StatusCode = 200
		rec.ContentType = "application/json"
		rec.ResponseBody = []byte(`{"contract_id":"C1"}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, rec))

		// Different body with the same key -> ErrIdempotencyKeyMismatch.
		diffBody := rec
		diffBody.RequestHash = HashIdempotencyRequest("POST", "/watched-contracts", []byte(`{"contract_id":"C2"}`))
		_, _, err = st.ClaimIdempotencyKey(ctx, diffBody, now.Add(time.Minute))
		require.ErrorIs(t, err, ErrIdempotencyKeyMismatch)

		// Different method with the same key -> ErrIdempotencyKeyMismatch.
		diffMethod := rec
		diffMethod.Method = "DELETE"
		diffMethod.RequestHash = HashIdempotencyRequest("DELETE", "/watched-contracts", []byte(`{"contract_id":"C1"}`))
		_, _, err = st.ClaimIdempotencyKey(ctx, diffMethod, now.Add(time.Minute))
		require.ErrorIs(t, err, ErrIdempotencyKeyMismatch)
	})

	t.Run("cross-tenant isolation", func(t *testing.T) {
		st := newBackend(t)
		ctx := context.Background()
		now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

		recA := IdempotencyRecord{
			TenantID:    "tenant:alpha",
			Key:         "shared-key",
			RequestHash: HashIdempotencyRequest("POST", "/tenant/watch", []byte(`{"contract_id":"CALPHA"}`)),
			Method:      "POST",
			Path:        "/tenant/watch",
			CreatedAt:   now,
			ExpiresAt:   now.Add(DefaultIdempotencyTTL),
		}
		recB := IdempotencyRecord{
			TenantID:    "tenant:beta",
			Key:         "shared-key",
			RequestHash: HashIdempotencyRequest("POST", "/tenant/watch", []byte(`{"contract_id":"CBETA"}`)),
			Method:      "POST",
			Path:        "/tenant/watch",
			CreatedAt:   now,
			ExpiresAt:   now.Add(DefaultIdempotencyTTL),
		}

		outA, _, err := st.ClaimIdempotencyKey(ctx, recA, now)
		require.NoError(t, err)
		require.Equal(t, IdempotencyStarted, outA)
		recA.StatusCode = 200
		recA.ResponseBody = []byte(`{"tenant":"alpha"}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, recA))

		// Tenant B using the same key with its own body starts a fresh operation
		// and never sees Tenant A's response.
		outB, _, err := st.ClaimIdempotencyKey(ctx, recB, now)
		require.NoError(t, err)
		require.Equal(t, IdempotencyStarted, outB)
		recB.StatusCode = 200
		recB.ResponseBody = []byte(`{"tenant":"beta"}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, recB))

		_, repA, err := st.ClaimIdempotencyKey(ctx, recA, now.Add(time.Second))
		require.NoError(t, err)
		assert.JSONEq(t, `{"tenant":"alpha"}`, string(repA.ResponseBody))

		_, repB, err := st.ClaimIdempotencyKey(ctx, recB, now.Add(time.Second))
		require.NoError(t, err)
		assert.JSONEq(t, `{"tenant":"beta"}`, string(repB.ResponseBody))
	})

	t.Run("expiry and retention sweep", func(t *testing.T) {
		st := newBackend(t)
		ctx := context.Background()
		now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

		rec := IdempotencyRecord{
			TenantID:    "tenant:1",
			Key:         "expiring-key",
			RequestHash: HashIdempotencyRequest("POST", "/apikeys", []byte(`{"name":"k1"}`)),
			Method:      "POST",
			Path:        "/apikeys",
			CreatedAt:   now,
			ExpiresAt:   now.Add(time.Hour),
		}
		outcome, _, err := st.ClaimIdempotencyKey(ctx, rec, now)
		require.NoError(t, err)
		require.Equal(t, IdempotencyStarted, outcome)
		rec.StatusCode = 201
		rec.ResponseBody = []byte(`{"id":1}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, rec))

		// After the retention window (1h), presenting the same key with a new
		// payload is treated as a brand-new operation rather than a mismatch.
		afterExpiry := now.Add(2 * time.Hour)
		newRec := rec
		newRec.RequestHash = HashIdempotencyRequest("POST", "/apikeys", []byte(`{"name":"k2"}`))
		newRec.CreatedAt = afterExpiry
		newRec.ExpiresAt = afterExpiry.Add(time.Hour)

		outcome, _, err = st.ClaimIdempotencyKey(ctx, newRec, afterExpiry)
		require.NoError(t, err)
		assert.Equal(t, IdempotencyStarted, outcome)
		newRec.StatusCode = 201
		newRec.ResponseBody = []byte(`{"id":2}`)
		require.NoError(t, st.CompleteIdempotencyKey(ctx, newRec))

		// Sweep after the second window removes the expired row.
		deleted, err := st.DeleteExpiredIdempotencyKeys(ctx, afterExpiry.Add(2*time.Hour), 100)
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted)
	})

	t.Run("concurrent claims resolve to exactly one starter", func(t *testing.T) {
		st := newBackend(t)
		ctx := context.Background()
		now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

		rec := IdempotencyRecord{
			TenantID:    "tenant:race",
			Key:         "race-key",
			RequestHash: HashIdempotencyRequest("POST", "/subscriptions", []byte(`{"url":"https://race.example"}`)),
			Method:      "POST",
			Path:        "/subscriptions",
			CreatedAt:   now,
			ExpiresAt:   now.Add(DefaultIdempotencyTTL),
		}

		const workers = 12
		var started atomic.Int32
		var inFlightOrReplayed atomic.Int32
		var wg sync.WaitGroup
		startGate := make(chan struct{})

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startGate
				outcome, _, err := st.ClaimIdempotencyKey(ctx, rec, now)
				if err == nil && outcome == IdempotencyStarted {
					started.Add(1)
					return
				}
				if errors.Is(err, ErrIdempotencyInFlight) || (err == nil && outcome == IdempotencyReplayed) {
					inFlightOrReplayed.Add(1)
				}
			}()
		}
		close(startGate)
		wg.Wait()

		assert.Equal(t, int32(1), started.Load(), "exactly one concurrent caller may win IdempotencyStarted")
		assert.Equal(t, int32(workers-1), inFlightOrReplayed.Load())
	})
}

func TestIdempotencyStore_SQLiteAndMemoryAndGuarded(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		runIdempotencyStoreContractSuite(t, func(t *testing.T) IdempotencyStore {
			st := newSQLiteStore(t).(*SQLite)
			st.db.SetMaxOpenConns(1)
			return st
		})
	})

	t.Run("memory", func(t *testing.T) {
		runIdempotencyStoreContractSuite(t, func(*testing.T) IdempotencyStore {
			return NewMemoryIdempotencyStore()
		})
	})

	t.Run("guarded", func(t *testing.T) {
		runIdempotencyStoreContractSuite(t, func(t *testing.T) IdempotencyStore {
			st := newSQLiteStore(t).(*SQLite)
			st.db.SetMaxOpenConns(1)
			g := NewGuardedStore(st, GuardedStoreOptions{})
			return g.(IdempotencyStore)
		})
	})
}
