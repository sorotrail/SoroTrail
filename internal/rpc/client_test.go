package rpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonRPCServer decodes incoming JSON-RPC requests and lets a test handler
// produce the result or error per call.
func jsonRPCServer(t *testing.T, handle func(method string, params json.RawMessage) (any, *Error)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.Number     `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		require.NoError(t, json.Unmarshal(body, &req))
		assert.Equal(t, "2.0", req.JSONRPC)

		result, rpcErr := handle(req.Method, req.Params)
		out := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if rpcErr != nil {
			out["error"] = rpcErr
		} else {
			out["result"] = result
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(out))
	}))
}

func TestGetEvents_SendsXDRFormatJSON(t *testing.T) {
	var gotParams GetEventsRequest
	srv := jsonRPCServer(t, func(method string, params json.RawMessage) (any, *Error) {
		require.Equal(t, "getEvents", method)
		require.NoError(t, json.Unmarshal(params, &gotParams))
		return GetEventsResponse{LatestLedger: 123}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 100})
	require.NoError(t, err)
	assert.Equal(t, uint32(123), resp.LatestLedger)
	assert.Equal(t, XDRFormatJSON, gotParams.XDRFormat)
	assert.Equal(t, uint32(100), gotParams.StartLedger)
}

func TestGetEvents_CursorClearsLedgerRange(t *testing.T) {
	var gotParams GetEventsRequest
	srv := jsonRPCServer(t, func(_ string, params json.RawMessage) (any, *Error) {
		require.NoError(t, json.Unmarshal(params, &gotParams))
		return GetEventsResponse{}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	_, err := c.GetEvents(context.Background(), GetEventsRequest{
		StartLedger: 100,
		EndLedger:   200,
		Pagination:  &Pagination{Cursor: "abc", Limit: 10},
	})
	require.NoError(t, err)
	assert.Zero(t, gotParams.StartLedger)
	assert.Zero(t, gotParams.EndLedger)
	require.NotNil(t, gotParams.Pagination)
	assert.Equal(t, "abc", gotParams.Pagination.Cursor)
}

func TestGetEvents_FallsBackWhenXDRFormatUnsupported(t *testing.T) {
	calls := 0
	srv := jsonRPCServer(t, func(_ string, params json.RawMessage) (any, *Error) {
		calls++
		var req GetEventsRequest
		require.NoError(t, json.Unmarshal(params, &req))
		if req.XDRFormat != "" {
			return nil, &Error{Code: -32602, Message: `unknown field "xdrFormat"`}
		}
		return GetEventsResponse{LatestLedger: 55}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(55), resp.LatestLedger)
	assert.Equal(t, 2, calls, "first call rejected, retried without xdrFormat")

	// The rejection is remembered: no second round-trip next time.
	_, err = c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRPCErrorSurfaced(t *testing.T) {
	srv := jsonRPCServer(t, func(string, json.RawMessage) (any, *Error) {
		return nil, &Error{Code: -32600, Message: "startLedger must be within the ledger range: 100 - 200"}
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	_, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 5})
	require.Error(t, err)
	assert.True(t, IsLedgerOutOfRange(err))
}

func TestSimulateTransaction(t *testing.T) {
	srv := jsonRPCServer(t, func(method string, params json.RawMessage) (any, *Error) {
		require.Equal(t, "simulateTransaction", method)
		return SimulateTransactionResponse{
			TransactionData: "AAAAAg==",
			Cost: SimulationCost{
				CPUInstructions: 1000,
				MemoryBytes:     4096,
			},
		}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.SimulateTransaction(context.Background(), SimulateTransactionRequest{
		Transaction: "AAAAAg...",
	})
	require.NoError(t, err)
	assert.Equal(t, "AAAAAg==", resp.TransactionData)
	assert.Equal(t, uint64(1000), resp.Cost.CPUInstructions)
}

func TestGetHealthAndLatestLedger(t *testing.T) {
	srv := jsonRPCServer(t, func(method string, _ json.RawMessage) (any, *Error) {
		switch method {
		case "getHealth":
			return Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10}, nil
		case "getLatestLedger":
			return LatestLedger{Sequence: 500}, nil
		}
		return nil, &Error{Code: -32601, Message: "method not found"}
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	h, err := c.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", h.Status)
	assert.Equal(t, uint32(10), h.OldestLedger)

	l, err := c.GetLatestLedger(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(500), l.Sequence)
}

// TestIntervalLimiter_SerializesParallelCalls is issue #4's acceptance
// criterion: "The existing rate limiter still governs total request rate
// (~10/s to match public endpoint limits) — parallelism must not bypass it."
//
// In production the ingester fans batches out via errgroup.SetLimit
// (SWEEP_CONCURRENCY). Each goroutine still issues its GetEvents
// through the same HTTPClient, whose call() invokes the limiter's
// Wait() before the HTTP round trip. This test exercises that same
// limiter directly: N goroutines each call Wait() concurrently, the
// limiter must space them at the configured interval, so total elapsed
// time stays serialization-bound (≥ (N-1) * interval), not
// parallel-bound (~ interval). Without this guarantee, raising
// SWEEP_CONCURRENCY would effectively raise the request rate past the
// 10 req/s public ceiling.
func TestIntervalLimiter_SerializesParallelCalls(t *testing.T) {
	const interval = 50 * time.Millisecond
	const goroutines = 4
	l := newIntervalLimiter(interval)

	var wg sync.WaitGroup
	starts := make([]time.Time, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			if err := l.Wait(context.Background()); err != nil {
				t.Errorf("limiter wait: %v", err)
			}
			starts[i] = time.Now()
		}()
	}
	wg.Wait()

	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for i := 1; i < len(starts); i++ {
		gap := starts[i].Sub(starts[i-1])
		// On platforms with coarse timer resolution (e.g. Windows ~15ms),
		// time.NewTimer can fire significantly early, so we verify the gap
		// is at least half the interval — enough to prove serialization
		// (concurrent calls would produce near-zero gaps) without being
		// so tight that timer imprecision causes flaky failures.
		minGap := interval / 2
		assert.GreaterOrEqual(t, gap, minGap,
			"calls %d→%d elapsed=%v must be ≥ %v (half-interval)", i-1, i, gap, minGap)
	}
}

// TestWithRateLimitRPS is issue #58's rate-knob acceptance criterion:
// RPC_RATE_LIMIT=50 must raise the client's request ceiling accordingly,
// while a non-positive value keeps the default public-endpoint spacing.
func TestWithRateLimitRPS(t *testing.T) {
	c := NewHTTPClient("http://localhost", WithRateLimitRPS(50))
	require.NotNil(t, c.limiter)
	assert.Equal(t, 20*time.Millisecond, c.limiter.interval,
		"50 req/s ⇒ 20ms minimum spacing")

	c = NewHTTPClient("http://localhost", WithRateLimitRPS(10))
	require.NotNil(t, c.limiter)
	assert.Equal(t, 100*time.Millisecond, c.limiter.interval,
		"default 10 req/s matches the historical hardcoded spacing")

	c = NewHTTPClient("http://localhost", WithRateLimitRPS(0))
	require.NotNil(t, c.limiter)
	assert.Equal(t, 100*time.Millisecond, c.limiter.interval,
		"non-positive rps keeps the client default (config.Load rejects it anyway)")
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
	}{
		{name: "empty", header: "", want: 0},
		{name: "delta seconds", header: "30", want: 30 * time.Second},
		{name: "delta seconds padded", header: " 7 ", want: 7 * time.Second},
		{name: "delta zero", header: "0", want: 0},
		{name: "delta negative", header: "-5", want: 0},
		{name: "garbage", header: "soon", want: 0},
		{
			name:   "http date in future",
			header: time.Now().UTC().Add(2 * time.Minute).Format(http.TimeFormat),
			want:   119 * time.Second, // ±1s tolerance handled by caller below
		},
		{
			name:   "http date already elapsed",
			header: time.Now().UTC().Add(-2 * time.Minute).Format(http.TimeFormat),
			want:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRetryAfter(tt.header)
			if tt.name == "http date in future" {
				assert.GreaterOrEqual(t, got, 118*time.Second)
				assert.LessOrEqual(t, got, 120*time.Second)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestHTTP429SurfacesRetryAfter verifies that a provider-side HTTP 429
// becomes a typed RateLimitedError carrying both Retry-After formats —
// delta-seconds and HTTP-date — so the retry layer can honor the hint.
func TestHTTP429SurfacesRetryAfter(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter string
		wantHint   time.Duration
	}{
		{
			name:       "delta seconds",
			retryAfter: "2",
			wantHint:   2 * time.Second,
		},
		{
			name:       "http date",
			retryAfter: time.Now().UTC().Add(90 * time.Second).Format(http.TimeFormat),
			wantHint:   89 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", tt.retryAfter)
				http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			}))
			defer srv.Close()

			c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
			_, err := c.GetHealth(context.Background())
			require.Error(t, err)

			var rle *RateLimitedError
			require.ErrorAs(t, err, &rle, "429 must surface as *RateLimitedError")
			assert.Equal(t, http.StatusTooManyRequests, rle.StatusCode)
			assert.GreaterOrEqual(t, rle.RetryAfter, tt.wantHint-2*time.Second)
			assert.LessOrEqual(t, rle.RetryAfter, tt.wantHint+3*time.Second)
			assert.Contains(t, err.Error(), "rate limited")
		})
	}

	// Without the header the hint is zero and callers fall back to backoff.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	_, err := c.GetHealth(context.Background())
	var rle *RateLimitedError
	require.ErrorAs(t, err, &rle)
	assert.Zero(t, rle.RetryAfter, "absent Retry-After ⇒ zero hint")
}
