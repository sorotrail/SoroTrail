//go:build integration

package api

import (
	"net/http"
	"testing"

	"github.com/sorotrail/sorotrail/internal/store"
	"github.com/sorotrail/sorotrail/internal/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiInstanceRateLimiterQuotaSharedPostgres(t *testing.T) {
	st := testdb.Setup(t, store.Migrate)
	limiter1 := NewRateLimiter(10, 100, false, WithQuotaStore(st), WithDailyQuota(5))
	limiter2 := NewRateLimiter(10, 100, false, WithQuotaStore(st), WithDailyQuota(5))

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
