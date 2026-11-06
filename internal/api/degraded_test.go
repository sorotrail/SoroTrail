package api

import (
	"context"
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

	"github.com/sorotrail/sorotrail/internal/store"
)

type tripwireStore struct {
	stubStore
	queries   atomic.Int64
	pingCalls atomic.Int64
	failWith  error
}

func (s *tripwireStore) Ping(ctx context.Context) error {
	s.pingCalls.Add(1)
	if s.failWith != nil {
		return s.failWith
	}
	return s.stubStore.Ping(ctx)
}

func (s *tripwireStore) QueryEvents(ctx context.Context, f store.EventFilter) ([]store.Event, string, error) {
	s.queries.Add(1)
	if s.failWith != nil {
		return nil, "", s.failWith
	}
	return s.stubStore.QueryEvents(ctx, f)
}

func TestDegradedModeWhenDatabaseUnavailable(t *testing.T) {
	now := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	var transitions []string

	breaker := store.NewStoreBreaker(store.BreakerOptions{
		FailureThreshold: 2,
		BaseCooldown:     4 * time.Second,
		MaxCooldown:      30 * time.Second,
		Now:              func() time.Time { return now },
		Jitter:           func(d time.Duration) time.Duration { return d / 2 },
		OnStateChange: func(from, to store.BreakerState, _ error) {
			transitions = append(transitions, string(from)+"->"+string(to))
		},
	})

	rawStore := &tripwireStore{failWith: errors.New("connection refused")}
	guarded := store.NewGuardedStore(rawStore, store.GuardedStoreOptions{
		Timeout:            time.Second,
		SlowQueryThreshold: time.Hour,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		Breaker:            breaker,
	})

	srv := New(guarded, &stubRPC{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-api-key")
	handler := srv.Router()

	// 1. Database-free endpoints answer 200 without issuing any store query.
	for _, path := range []string{"/livez", "/version", "/openapi.json", "/docs", "/metrics"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, w.Code, "database-free endpoint %s must answer 200 during outage", path)
	}
	assert.Zero(t, rawStore.queries.Load(), "database-free endpoints must not issue store queries")
	assert.Zero(t, rawStore.pingCalls.Load(), "database-free endpoints must not ping the store")

	// 2. Two failing store requests trip the circuit breaker to open.
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events", nil))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	}
	assert.Equal(t, store.BreakerOpen, breaker.State())
	assert.Equal(t, int64(2), rawStore.queries.Load())

	// 3. Subsequent requests fail fast with 503 + Retry-After without touching rawStore.
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/events", nil))
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.NotEmpty(t, w.Header().Get("Retry-After"))
	}
	assert.Equal(t, int64(2), rawStore.queries.Load(), "open breaker must fail fast without calling the store")

	// 4. Readiness reports 503 while liveness stays 200.
	wReady := httptest.NewRecorder()
	handler.ServeHTTP(wReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, wReady.Code)

	wLive := httptest.NewRecorder()
	handler.ServeHTTP(wLive, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, http.StatusOK, wLive.Code)

	// 5. Recovery: advance clock past cooldown, restore store health, and verify half-open probe closes the circuit.
	now = now.Add(10 * time.Second)
	rawStore.failWith = nil

	wRecovered := httptest.NewRecorder()
	handler.ServeHTTP(wRecovered, httptest.NewRequest(http.MethodGet, "/events", nil))
	require.Equal(t, http.StatusOK, wRecovered.Code)
	assert.Equal(t, store.BreakerClosed, breaker.State())
	assert.Equal(t, []string{"closed->open", "open->half-open", "half-open->closed"}, transitions,
		"transitions are logged once per state change, never per request")
}

func TestDegradedModeHalfOpenFailedProbeReopensWithExponentialBackoff(t *testing.T) {
	now := time.Date(2026, 11, 6, 1, 0, 0, 0, time.UTC)
	breaker := store.NewStoreBreaker(store.BreakerOptions{
		FailureThreshold: 1,
		BaseCooldown:     2 * time.Second,
		MaxCooldown:      16 * time.Second,
		Now:              func() time.Time { return now },
		Jitter:           func(d time.Duration) time.Duration { return d },
	})

	rawStore := &tripwireStore{failWith: errors.New("connection reset by peer")}
	guarded := store.NewGuardedStore(rawStore, store.GuardedStoreOptions{
		Timeout:            time.Second,
		SlowQueryThreshold: time.Hour,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		Breaker:            breaker,
	})

	srv := New(guarded, &stubRPC{},
		slog.New(slog.NewTextHandler(io.Discard, nil)), "test-api-key")
	handler := srv.Router()

	// First failure opens the breaker with 2s cooldown.
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusInternalServerError, w1.Code)
	assert.Equal(t, store.BreakerOpen, breaker.State())

	// Advance past first 2s cooldown; probe fails again and re-opens with 4s cooldown.
	now = now.Add(3 * time.Second)
	wProbe := httptest.NewRecorder()
	handler.ServeHTTP(wProbe, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusInternalServerError, wProbe.Code)
	assert.Equal(t, store.BreakerOpen, breaker.State())

	// At +2s into the new 4s cooldown, requests still fail fast with 503.
	now = now.Add(2 * time.Second)
	wFastFail := httptest.NewRecorder()
	handler.ServeHTTP(wFastFail, httptest.NewRequest(http.MethodGet, "/events", nil))
	assert.Equal(t, http.StatusServiceUnavailable, wFastFail.Code)
	assert.NotEmpty(t, wFastFail.Header().Get("Retry-After"))
}
