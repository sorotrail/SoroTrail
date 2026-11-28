//go:build integration

package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
)

type pgQuotaStore struct {
	pg *store.Postgres
}

func (p *pgQuotaStore) IncrementQuota(ctx context.Context, clientKey string, now time.Time, batchSize int) (int64, int64, error) {
	return p.pg.IncrementQuota(ctx, clientKey, now, batchSize)
}

func TestMultiInstanceRateLimiterQuotaSharedPostgres(t *testing.T) {
	pool := testdb.Setup(t, store.Migrate)
	qStore := &pgQuotaStore{pg: store.NewPostgres(pool)}
	limiter1 := NewRateLimiter(10, 100, false, WithQuotaStore(qStore), WithDailyQuota(5))
	limiter2 := NewRateLimiter(10, 100, false, WithQuotaStore(qStore), WithDailyQuota(5))

	for i := 0; i < 3; i++ {
		rec := sendFrom(limiter1, "203.0.113.88:1234")
		require.Equal(t, http.StatusOK, rec.Code)
	}
	for i := 0; i < 2; i++ {
		rec := sendFrom(limiter2, "203.0.113.88:1234")
		require.Equal(t, http.StatusOK, rec.Code)
	}
	rec := sendFrom(limiter1, "203.0.113.88:1234")
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
}
