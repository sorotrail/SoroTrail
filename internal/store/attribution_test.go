package store

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
	"github.com/sorotrail/sorotrail/internal/requestid"
)

func TestQueryCostAttributionRequestAndBackgroundJob(t *testing.T) {
	var logBuf strings.Builder
	opts := GuardedStoreOptions{
		Timeout:            time.Second,
		SlowQueryThreshold: time.Nanosecond,
		Logger:             slog.New(slog.NewTextHandler(&logBuf, nil)),
	}
	guarded := NewGuardedStore(metricsOKStore{}, opts)

	t.Run("request-driven query attributes route and opaque tenant", func(t *testing.T) {
		ctx := WithQueryAttribution(context.Background(), "GET /events/{id}", "tenant-alpha")
		opaque := OpaqueTenantLabel("tenant-alpha")
		require.True(t, strings.HasPrefix(opaque, "t_"))
		require.NotContains(t, opaque, "tenant-alpha")

		before := testutil.ToFloat64(metrics.DBAttributedQueriesTotal.WithLabelValues("GET /events/{id}", "store.Ping", opaque, "success"))
		require.NoError(t, guarded.Ping(ctx))
		after := testutil.ToFloat64(metrics.DBAttributedQueriesTotal.WithLabelValues("GET /events/{id}", "store.Ping", opaque, "success"))
		assert.Equal(t, before+1, after)

		assert.Contains(t, logBuf.String(), `originator="GET /events/{id}"`)
		assert.Contains(t, logBuf.String(), "tenant="+opaque)
	})

	t.Run("background job context attributes job identity", func(t *testing.T) {
		logBuf.Reset()
		ctx := requestid.WithJob(context.Background(), requestid.JobPruner)

		before := testutil.ToFloat64(metrics.DBAttributedQueriesTotal.WithLabelValues("pruner", "store.Ping", "default", "success"))
		require.NoError(t, guarded.Ping(ctx))
		after := testutil.ToFloat64(metrics.DBAttributedQueriesTotal.WithLabelValues("pruner", "store.Ping", "default", "success"))
		assert.Equal(t, before+1, after)

		assert.Contains(t, logBuf.String(), "originator=pruner")
		assert.Equal(t, "/*originator=pruner,tenant=default,op=store.Ping*/ ", FormatSQLComment(ctx, "store.Ping"))
	})

	t.Run("no sensitive contract ID or raw tenant secret appears in tags", func(t *testing.T) {
		secretContract := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		secretTenant := "acme-corp-secret-tenant-token"
		ctx := WithQueryAttribution(context.Background(), "GET /contracts/"+secretContract+"/events", secretTenant)

		attr := AttributionFromContext(ctx)
		comment := FormatSQLComment(ctx, "store.QueryEvents")
		assert.NotContains(t, attr.Originator, secretContract, "contract address must be scrubbed from originator tag")
		assert.NotContains(t, attr.Tenant, secretTenant, "raw tenant identifier must be hashed into an opaque bucket")
		assert.NotContains(t, comment, secretContract)
		assert.NotContains(t, comment, secretTenant)
	})
}

func BenchmarkQueryAttributionOverhead(b *testing.B) {
	opts := GuardedStoreOptions{
		Timeout:            time.Second,
		SlowQueryThreshold: time.Hour,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	guarded := NewGuardedStore(metricsOKStore{}, opts)
	ctx := WithQueryAttribution(context.Background(), "GET /events", "tenant-bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = guarded.Ping(ctx)
	}
}
