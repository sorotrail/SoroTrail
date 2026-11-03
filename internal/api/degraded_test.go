package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

type degradedTestStore struct {
	*stubStore
	degraded   atomic.Bool
	pingErr    error
	queryCalls atomic.Int64
	statsCalls atomic.Int64
}

func (s *degradedTestStore) Degraded() bool {
	return s.degraded.Load()
}

func (s *degradedTestStore) Ping(_ context.Context) error {
	if s.pingErr != nil {
		return s.pingErr
	}
	if s.degraded.Load() {
		return store.ErrUnavailable
	}
	return nil
}

func (s *degradedTestStore) QueryEvents(_ context.Context, _ store.EventFilter) ([]store.Event, string, error) {
	s.queryCalls.Add(1)
	if s.degraded.Load() {
		return nil, "", store.ErrUnavailable
	}
	return []store.Event{}, "", nil
}

func (s *degradedTestStore) GetEvent(_ context.Context, _ string, _ store.Scope) (store.Event, error) {
	s.queryCalls.Add(1)
	if s.degraded.Load() {
		return store.Event{}, store.ErrUnavailable
	}
	return store.Event{}, store.ErrNotFound
}

func (s *degradedTestStore) Stats(_ context.Context, _ store.Scope) (store.Stats, error) {
	s.statsCalls.Add(1)
	if s.degraded.Load() {
		return store.Stats{}, store.ErrUnavailable
	}
	return store.Stats{TotalEvents: 42, LastIngestedLedger: 1000}, nil
}

func (s *degradedTestStore) GetIngestionState(_ context.Context) (store.IngestionState, error) {
	if s.degraded.Load() {
		return store.IngestionState{}, store.ErrUnavailable
	}
	return store.IngestionState{Network: "default", LastIngestedLedger: 1000}, nil
}

func (s *degradedTestStore) GetIngestionStateForNetwork(_ context.Context, network string) (store.IngestionState, error) {
	if s.degraded.Load() {
		return store.IngestionState{}, store.ErrUnavailable
	}
	return store.IngestionState{Network: network, LastIngestedLedger: 1000}, nil
}

func (s *degradedTestStore) ListContracts(_ context.Context, _ store.ContractsFilter) ([]store.ContractSummary, string, error) {
	s.queryCalls.Add(1)
	if s.degraded.Load() {
		return nil, "", store.ErrUnavailable
	}
	return []store.ContractSummary{}, "", nil
}

func (s *degradedTestStore) CountContracts(_ context.Context, _ store.ContractsFilter) (int64, error) {
	s.queryCalls.Add(1)
	if s.degraded.Load() {
		return 0, store.ErrUnavailable
	}
	return 0, nil
}

type healthyMockRPC struct {
	rpc.Client
}

func (healthyMockRPC) GetHealth(_ context.Context) (rpc.Health, error) {
	return rpc.Health{Status: "healthy", LatestLedger: 1000}, nil
}

func TestDegradedMode_ProbesAndDatabaseRoutes(t *testing.T) {
	st := &degradedTestStore{stubStore: &stubStore{}}
	st.degraded.Store(true)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(st, healthyMockRPC{}, logger, "test-key", nil)
	srv.SetMetricsEnabled(true)
	handler := srv.Router()

	// 1. /livez stays 200 OK while degraded so orchestrators do not restart
	// the process during a database outage.
	t.Run("livez stays 200 OK while degraded", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/livez", nil)
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		got := decodeHealth(t, rec.Body.Bytes())
		assert.Equal(t, healthOK, got.Status)
	})

	// 2. /readyz reports 503 Service Unavailable so load balancers stop
	// routing traffic to the degraded instance.
	t.Run("readyz returns 503 when database is unavailable", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		got := decodeHealth(t, rec.Body.Bytes())
		assert.Equal(t, healthUnready, got.Status)
		assert.Equal(t, healthUnready, got.Checks["database"].Status)
		assert.NotEmpty(t, got.Checks["database"].Detail)
	})

	// 3. /health reports 503 with database check unready and rpc check ok.
	t.Run("health returns 503 with per-dependency status", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		got := decodeHealth(t, rec.Body.Bytes())
		assert.Equal(t, healthUnready, got.Status)
		assert.Equal(t, healthUnready, got.Checks["database"].Status)
		assert.Equal(t, healthOK, got.Checks["rpc"].Status)
	})

	// 4. Non-database routes remain 200 OK during an outage.
	for _, path := range []string{"/version", "/metrics", "/openapi.json", "/docs"} {
		t.Run("exempt route "+path+" stays 200 OK", func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}

	// 5. Database-backed routes fast-fail with 503 Service Unavailable,
	// Retry-After header, and zero queries to the underlying store.
	for _, path := range []string{"/events", "/events/evt-1", "/stats", "/contracts"} {
		t.Run("database route "+path+" fast-fails with 503 and Retry-After", func(t *testing.T) {
			beforeQueries := st.queryCalls.Load()
			beforeStats := st.statsCalls.Load()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
			assert.Equal(t, "5", rec.Header().Get("Retry-After"))
			assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

			var errResp errorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, CodeServiceUnavailable, errResp.Code)
			assert.Contains(t, errResp.Error, "degraded mode")

			assert.Equal(t, beforeQueries, st.queryCalls.Load(), "degraded middleware must short-circuit before querying the store")
			assert.Equal(t, beforeStats, st.statsCalls.Load(), "degraded middleware must short-circuit before calling store.Stats")
		})
	}

	// 6. When the database becomes reachable again, /readyz and database
	// routes recover automatically without a process restart.
	t.Run("automatic recovery when database returns", func(t *testing.T) {
		st.degraded.Store(false)

		recReady := httptest.NewRecorder()
		handler.ServeHTTP(recReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusOK, recReady.Code)

		recEvents := httptest.NewRecorder()
		handler.ServeHTTP(recEvents, httptest.NewRequest(http.MethodGet, "/events", nil))
		assert.Equal(t, http.StatusOK, recEvents.Code)
		assert.Equal(t, int64(1), st.queryCalls.Load())

		recStats := httptest.NewRecorder()
		handler.ServeHTTP(recStats, httptest.NewRequest(http.MethodGet, "/stats", nil))
		assert.Equal(t, http.StatusOK, recStats.Code)
		assert.Equal(t, int64(1), st.statsCalls.Load())
	})
}

func TestDegradedMode_CircuitBreakerFastFailAndProbeRecovery(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	breaker := store.NewStoreBreaker(
		store.WithFailureThreshold(2),
		store.WithCoolDown(12*time.Second),
		store.WithBreakerClock(func() time.Time { return now }),
	)

	st := &degradedTestStore{
		stubStore: &stubStore{},
		pingErr:   errors.New("dial tcp 127.0.0.1:5432: connection refused"),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(st, healthyMockRPC{}, logger, "test-key", nil).WithStoreBreaker(breaker)
	handler := srv.Router()

	// Trip the circuit breaker via two failed readiness probes.
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	}
	require.Equal(t, store.BreakerOpen, breaker.State())

	// Database-backed request now fast-fails with Retry-After reflecting the
	// remaining breaker cool-down (12s) and does not touch the store.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "12", rec.Header().Get("Retry-After"))
	assert.Zero(t, st.queryCalls.Load())

	// Database recovers and cool-down expires: /readyz runs the half-open
	// probe, closes the breaker, and subsequent API requests succeed.
	st.pingErr = nil
	now = now.Add(12 * time.Second)

	recReady := httptest.NewRecorder()
	handler.ServeHTTP(recReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusOK, recReady.Code)
	assert.Equal(t, store.BreakerClosed, breaker.State())

	recEvents := httptest.NewRecorder()
	handler.ServeHTTP(recEvents, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusOK, recEvents.Code)
	assert.Equal(t, int64(1), st.queryCalls.Load())
}
