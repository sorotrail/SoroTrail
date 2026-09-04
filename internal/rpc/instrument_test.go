package rpc

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// histogramSampleCount returns the observation count (_count) of a
// histogram, so tests can assert exactly how many calls were recorded for
// a (method, outcome) pair rather than relying on sample sums.
func histogramSampleCount(h prometheus.Histogram) uint64 {
	pb := &dto.Metric{}
	h.Write(pb)
	return pb.GetHistogram().GetSampleCount()
}

// latencyHist returns the histogram child for a (method, outcome) pair.
func latencyHist(method, outcome string) prometheus.Histogram {
	return metrics.RPCCallLatency.WithLabelValues(method, outcome).(prometheus.Histogram)
}

// ---------------------------------------------------------------------------
// Endpoint label sanitization (security requirement: no credentials in labels)
// ---------------------------------------------------------------------------

func TestProviderLabel_SanitizesURLs(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"https://rpc1.example.com", "rpc1.example.com"},
		{"https://user:pass@rpc1.example.com/", "rpc1.example.com"}, // basic-auth credentials stripped
		{"https://user:pass@rpc1.example.com:443/path?x=1", "rpc1.example.com"},
		{"http://127.0.0.1:8000", "127.0.0.1"},
		{"", "unknown"},
		{"not a url", "unknown"},
		{"rpc0", "unknown"}, // bare tokens used by other failover tests
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got := providerLabel(tt.raw)
			assert.Equal(t, tt.want, got)
			// The label must never leak credentials, the scheme, or a path.
			assert.NotContains(t, got, "pass")
			assert.NotContains(t, got, "user")
			assert.NotContains(t, got, "://")
		})
	}
}

// ---------------------------------------------------------------------------
// Retry / backoff counters
// ---------------------------------------------------------------------------

func TestRetryClient_MetricsCountRetriesAndBackoff(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(context.Context) (Health, error) {
			calls++
			if calls < 3 {
				return Health{}, &Error{Code: 0, Message: "server error"}
			}
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{
		MaxAttempts: 5,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  time.Millisecond,
		Jitter:      false,
	})

	retriesBefore := testutil.ToFloat64(metrics.RPCRetriesTotal.WithLabelValues("getHealth", "backoff"))
	backoffBefore := testutil.ToFloat64(metrics.RPCBackoffSeconds.WithLabelValues("getHealth"))

	_, err := rc.GetHealth(context.Background())
	require.NoError(t, err)

	// Two failures before success ⇒ exactly two backoff-sourced retries.
	assert.Equal(t, retriesBefore+2,
		testutil.ToFloat64(metrics.RPCRetriesTotal.WithLabelValues("getHealth", "backoff")))
	// Two waits of 1ms each on the exponential schedule.
	assert.GreaterOrEqual(t,
		testutil.ToFloat64(metrics.RPCBackoffSeconds.WithLabelValues("getHealth"))-backoffBefore, 0.002)
}

func TestRetryClient_MetricsReasonRetryAfter(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(context.Context) (Health, error) {
			calls++
			if calls == 1 {
				return Health{}, &RateLimitedError{StatusCode: 429, RetryAfter: time.Millisecond}
			}
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: time.Hour, // the hint must win over this
		MaxBackoff:  time.Hour,
		Jitter:      false,
	})

	before := testutil.ToFloat64(metrics.RPCRetriesTotal.WithLabelValues("getHealth", "retry_after"))
	_, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before+1,
		testutil.ToFloat64(metrics.RPCRetriesTotal.WithLabelValues("getHealth", "retry_after")))
}

// ---------------------------------------------------------------------------
// Circuit-breaker state gauge
// ---------------------------------------------------------------------------

func TestCircuitBreakerMetrics_GaugeTracksState(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{FailureThreshold: 2, ProbeTimeout: time.Hour}, testLogger())

	// A freshly-constructed breaker is seeded closed.
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.RPCCircuitBreakerState.WithLabelValues("closed")))

	cb.RecordFailure()
	cb.RecordFailure()
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.RPCCircuitBreakerState.WithLabelValues("open")))
	assert.Equal(t, 0.0, testutil.ToFloat64(metrics.RPCCircuitBreakerState.WithLabelValues("closed")))

	cb.RecordSuccess()
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.RPCCircuitBreakerState.WithLabelValues("closed")))
	assert.Equal(t, 0.0, testutil.ToFloat64(metrics.RPCCircuitBreakerState.WithLabelValues("open")))
}

// ---------------------------------------------------------------------------
// Failover counters and per-provider state gauge
// ---------------------------------------------------------------------------

func TestFailoverMetrics_SwitchCounted(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"https://metrics-a.example.com", "https://metrics-b.example.com"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	switchBefore := testutil.ToFloat64(metrics.RPCFailoversTotal.WithLabelValues("switch"))

	// Provider 0 handles the first call.
	mocks[0].getEventsResp = []GetEventsResponse{{LatestLedger: 100}}
	_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)

	// Demote provider 0 with 3 network errors.
	mocks[0].getEventsResp = nil
	mocks[0].resetCallCount()
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}
	assert.Equal(t, StateDegraded, ProviderState(fc.providers[0].state.Load()))

	// The next call routes to provider 1: a switch is counted.
	_, err = fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, switchBefore+1,
		testutil.ToFloat64(metrics.RPCFailoversTotal.WithLabelValues("switch")))
}

func TestFailoverMetrics_ReanchorCounted(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"https://metrics-c.example.com", "https://metrics-d.example.com"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	reanchorBefore := testutil.ToFloat64(metrics.RPCFailoversTotal.WithLabelValues("reanchor"))

	// First cursor call succeeds on provider 0.
	mocks[0].getEventsResp = []GetEventsResponse{{LatestLedger: 100}}
	_, err := fc.GetEvents(context.Background(), GetEventsRequest{
		Pagination: &Pagination{Cursor: "cursor-p0"},
	})
	require.NoError(t, err)

	// Demote provider 0.
	mocks[0].getEventsResp = nil
	mocks[0].resetCallCount()
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}

	// A cursor call now routes to provider 1: re-anchor is signalled and counted.
	_, err = fc.GetEvents(context.Background(), GetEventsRequest{
		Pagination: &Pagination{Cursor: "cursor-p0"},
	})
	require.True(t, IsFailoverReanchor(err))
	assert.Equal(t, reanchorBefore+1,
		testutil.ToFloat64(metrics.RPCFailoversTotal.WithLabelValues("reanchor")))
}

func TestFailoverMetrics_ProviderStateGauge(t *testing.T) {
	fc, mocks := newFailoverTestClient(
		[]string{"https://provider-state-a.example.com", "https://provider-state-b.example.com"},
		WithFailoverLogger(testLogger()),
		WithFailoverMaxErrors(3),
	)

	// Both providers start active (gauge is seeded at construction).
	assert.Equal(t, 1.0, testutil.ToFloat64(
		metrics.RPCProviderState.WithLabelValues("provider-state-a.example.com", "active")))
	assert.Equal(t, 1.0, testutil.ToFloat64(
		metrics.RPCProviderState.WithLabelValues("provider-state-b.example.com", "active")))

	// Demote provider 0.
	mocks[0].getEventsErr = []error{
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
		fmt.Errorf("connection refused"),
	}
	for i := 0; i < 3; i++ {
		_, err := fc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
	}

	assert.Equal(t, 1.0, testutil.ToFloat64(
		metrics.RPCProviderState.WithLabelValues("provider-state-a.example.com", "degraded")))
	assert.Equal(t, 0.0, testutil.ToFloat64(
		metrics.RPCProviderState.WithLabelValues("provider-state-a.example.com", "active")))
}

// ---------------------------------------------------------------------------
// Per-method latency with outcome
// ---------------------------------------------------------------------------

func TestHTTPClientLatency_LabelledByMethodAndOutcome(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"healthy"}}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))

	// Success outcome is recorded exactly once per call.
	successBefore := histogramSampleCount(latencyHist("getHealth", "success"))
	_, err := c.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, successBefore+1, histogramSampleCount(latencyHist("getHealth", "success")))

	// Error outcome is recorded exactly once per call, in the same method.
	errorBefore := histogramSampleCount(latencyHist("getHealth", "error"))
	fail.Store(true)
	_, err = c.GetHealth(context.Background())
	require.Error(t, err)
	assert.Equal(t, errorBefore+1, histogramSampleCount(latencyHist("getHealth", "error")))
}
