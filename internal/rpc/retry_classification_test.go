package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsTransientHTTP_Table pins the fragment list behind transport-level
// retry classification, including the case that decides whether a flaky
// provider degrades gracefully or stops ingestion: an HTTP 5xx response is
// NOT matched, because call() renders it as "method returned HTTP 500: …"
// and no fragment in the list appears in that sentence.
func TestIsTransientHTTP_Table(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil is never transient", err: nil, want: false},
		{name: "connection refused", err: errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), want: true},
		{name: "connection reset mid-response", err: errors.New("read tcp: connection reset by peer"), want: true},
		{name: "EOF on a keep-alive connection", err: errors.New("unexpected EOF"), want: true},
		{name: "i/o timeout", err: errors.New("dial tcp: i/o timeout"), want: true},
		{name: "DNS failure", err: errors.New("lookup rpc.example: no such host"), want: true},
		{name: "temporary resolver failure", err: errors.New("temporary failure in name resolution"), want: true},
		{
			// The wrapped transport error from call() keeps the fragment in
			// the message, so classification survives the wrapping.
			name: "wrapped transport error",
			err:  fmt.Errorf("calling getEvents: %w", errors.New("dial tcp: i/o timeout")),
			want: true,
		},
		{
			// Pinned gap: a 502 from the provider reads as a gateway failure
			// to an operator but is not matched here, so it is neither
			// retried nor treated as transient. Failover still demotes the
			// provider because isDemotableError matches "HTTP 5"; the retry
			// layer just never fires. Reported separately rather than fixed
			// here, since changing it alters production behaviour.
			name: "http 5xx status is not matched by the fragment list",
			err:  errors.New("getEvents returned HTTP 502: bad gateway"),
			want: false,
		},
		{
			name: "http 4xx status is not transient",
			err:  errors.New("getHealth returned HTTP 400: bad request"),
			want: false,
		},
		{
			// "timed out" is the prose a reader expects to match; the list
			// looks for "timeout", so it does not.
			name: "similar wording is not matched",
			err:  errors.New("context timed out while waiting for response"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransientHTTP(tt.err))
		})
	}
}

// TestRetryClient_EveryMethodRetries walks the Client surface: each method
// must reach doWithRetry with its own JSON-RPC name so a single transient
// failure is absorbed before the caller sees it.
func TestRetryClient_EveryMethodRetries(t *testing.T) {
	transient := errors.New("dial tcp: connection refused")
	cfg := RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}

	t.Run("getEvents", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getEvents: func(context.Context, GetEventsRequest) (GetEventsResponse, error) {
			attempts++
			if attempts == 1 {
				return GetEventsResponse{}, transient
			}
			return GetEventsResponse{LatestLedger: 9}, nil
		}}
		rc := NewRetryClient(inner, cfg)

		resp, err := rc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.NoError(t, err)
		assert.Equal(t, uint32(9), resp.LatestLedger)
		assert.Equal(t, 2, attempts, "a transient failure must be retried once")
	})

	t.Run("getLatestLedger", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getLatestLedger: func(context.Context) (LatestLedger, error) {
			attempts++
			if attempts == 1 {
				return LatestLedger{}, transient
			}
			return LatestLedger{Sequence: 42}, nil
		}}
		rc := NewRetryClient(inner, cfg)

		resp, err := rc.GetLatestLedger(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(42), resp.Sequence)
		assert.Equal(t, 2, attempts)
	})

	t.Run("getHealth", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getHealth: func(context.Context) (Health, error) {
			attempts++
			if attempts == 1 {
				return Health{}, transient
			}
			return Health{Status: "healthy"}, nil
		}}
		rc := NewRetryClient(inner, cfg)

		resp, err := rc.GetHealth(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "healthy", resp.Status)
		assert.Equal(t, 2, attempts)
	})

	t.Run("getLedgerEntries", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getLedgerEntries: func(context.Context, GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
			attempts++
			if attempts == 1 {
				return GetLedgerEntriesResponse{}, transient
			}
			return GetLedgerEntriesResponse{Entries: []LedgerEntryResult{{Key: "AAAA"}}}, nil
		}}
		rc := NewRetryClient(inner, cfg)

		resp, err := rc.GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{Keys: []string{"AAAA"}})
		require.NoError(t, err)
		require.Len(t, resp.Entries, 1)
		assert.Equal(t, 2, attempts)
	})

	t.Run("simulateTransaction", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{simulate: func(context.Context, SimulateTransactionRequest) (SimulateTransactionResponse, error) {
			attempts++
			if attempts == 1 {
				return SimulateTransactionResponse{}, transient
			}
			return SimulateTransactionResponse{TransactionData: "AAAA"}, nil
		}}
		rc := NewRetryClient(inner, cfg)

		resp, err := rc.SimulateTransaction(context.Background(), SimulateTransactionRequest{Transaction: "AAAA"})
		require.NoError(t, err)
		assert.Equal(t, "AAAA", resp.TransactionData)
		assert.Equal(t, 2, attempts)
	})
}

// TestRetryClient_TransportErrorsEndToEnd drives the retry layer against a
// real HTTP endpoint rather than a mock, so the classification is proven for
// the error values the transport actually produces.
func TestRetryClient_TransportErrorsEndToEnd(t *testing.T) {
	t.Run("refused connection is retried to exhaustion", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := dead.URL
		dead.Close()

		rc := NewRetryClient(NewHTTPClient(url, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exhausted 3 retries",
			"a provider nobody is listening for must burn every attempt, got %v", err)
	})

	t.Run("429 with a hint is retried and succeeds", func(t *testing.T) {
		var attempts int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			if attempts == 1 {
				w.Header().Set("Retry-After", "0")
				http.Error(w, "slow down", http.StatusTooManyRequests)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"healthy"}}`))
		}))
		defer srv.Close()

		rc := NewRetryClient(NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		resp, err := rc.GetHealth(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "healthy", resp.Status)
		assert.Equal(t, 2, attempts)
	})

	t.Run("4xx is returned immediately", func(t *testing.T) {
		var attempts int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			http.Error(w, `invalid request`, http.StatusBadRequest)
		}))
		defer srv.Close()

		rc := NewRetryClient(NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 4, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)
		assert.Equal(t, 1, attempts, "a 4xx is our fault, not the provider's; retrying earns the same answer")
		assert.Contains(t, err.Error(), "HTTP 400")
	})

	t.Run("5xx is returned immediately - known gap", func(t *testing.T) {
		var attempts int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts++
			http.Error(w, "internal error", http.StatusInternalServerError)
		}))
		defer srv.Close()

		rc := NewRetryClient(NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 4, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)

		// Pinned deliberately: 5xx is the textbook transient failure, yet
		// the retry layer gives up after one attempt because the rendered
		// message matches no fragment in isTransientHTTP and is not an
		// *Error. The failover layer does demote the provider, so a request
		// is moved rather than retried — but a single-provider deployment
		// sees a hard failure. Filed as a follow-up; the fix belongs in
		// production code, not here.
		assert.Equal(t, 1, attempts, "current behaviour: a 5xx is never retried")
		assert.Contains(t, err.Error(), "HTTP 500")
		assert.False(t, isRetryable(err), "the gap this test pins: 5xx is classified as non-retryable")
	})

	t.Run("json-rpc server error is retried", func(t *testing.T) {
		var attempts int
		srv := jsonRPCServer(t, func(string, json.RawMessage) (any, *Error) {
			attempts++
			if attempts == 1 {
				return nil, &Error{Code: -32603, Message: "internal error"}
			}
			return Health{Status: "healthy"}, nil
		})
		defer srv.Close()

		rc := NewRetryClient(NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		_, err := rc.GetHealth(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 2, attempts, "an internal JSON-RPC error is transient")
	})

	t.Run("json-rpc method-not-found is not retried", func(t *testing.T) {
		var attempts int
		srv := jsonRPCServer(t, func(string, json.RawMessage) (any, *Error) {
			attempts++
			return nil, &Error{Code: -32601, Message: "method not found"}
		})
		defer srv.Close()

		rc := NewRetryClient(NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
			RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)
		assert.Equal(t, 1, attempts, "a missing method will not appear between attempts")
	})
}

// TestRetryClient_ContextErrorsAreFinal proves context problems short-circuit
// the retry loop instead of being masked as transient failures: a canceled
// or expired context must stop further attempts and surface ctx.Err(), so a
// shutdown is not delayed by backoff sleeps.
func TestRetryClient_ContextErrorsAreFinal(t *testing.T) {
	t.Run("already canceled context never calls the provider", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getEvents: func(context.Context, GetEventsRequest) (GetEventsResponse, error) {
			attempts++
			return GetEventsResponse{}, nil
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
		_, err := rc.GetEvents(ctx, GetEventsRequest{StartLedger: 1})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, attempts, "doWithRetry checks ctx before the first attempt")
	})

	t.Run("deadline during backoff stops the loop", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getHealth: func(context.Context) (Health, error) {
			attempts++
			return Health{}, errors.New("dial tcp: connection refused")
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 10, BaseBackoff: 500 * time.Millisecond, MaxBackoff: time.Second})
		_, err := rc.GetHealth(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded,
			"the caller's deadline is the reason the call ended, not the provider error")
		assert.Equal(t, 1, attempts, "sleeping past the deadline must not spend the remaining attempts")
	})

	t.Run("provider returning ctx.Err is not retried", func(t *testing.T) {
		var attempts int
		inner := &retryMockClient{getLatestLedger: func(context.Context) (LatestLedger, error) {
			attempts++
			return LatestLedger{}, context.Canceled
		}}

		rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 5, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
		_, err := rc.GetLatestLedger(context.Background())
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, attempts, "a canceled inner call is a signal to stop, not to retry")
	})
}

// waitPattern extracts the scheduled wait from the debug line doWithRetry
// logs before each sleep. Reading the schedule off the log keeps the bounds
// assertions exact: measuring wall-clock gaps between attempts would fail on
// a loaded CI runner that schedules a timer a few microseconds late.
var waitPattern = regexp.MustCompile(`wait=([0-9a-zµ.]+)`)

func scheduledWaits(t *testing.T, buf *bytes.Buffer) []time.Duration {
	t.Helper()
	var waits []time.Duration
	for _, m := range waitPattern.FindAllStringSubmatch(buf.String(), -1) {
		d, err := time.ParseDuration(m[1])
		require.NoError(t, err, "unparsable wait %q in the retry log", m[1])
		waits = append(waits, d)
	}
	return waits
}

// TestRetryClient_BackoffSchedule pins the exponential curve, the cap, and
// the jitter window by reading back the waits the loop actually scheduled.
func TestRetryClient_BackoffSchedule(t *testing.T) {
	t.Run("doubles then holds at the cap", func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		var attempts int
		inner := &retryMockClient{getHealth: func(context.Context) (Health, error) {
			attempts++
			return Health{}, errors.New("dial tcp: connection refused")
		}}
		rc := NewRetryClient(inner, RetryConfig{
			MaxAttempts: 6,
			BaseBackoff: 10 * time.Millisecond,
			MaxBackoff:  40 * time.Millisecond,
			Jitter:      false,
			Logger:      log,
		})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)
		assert.Equal(t, 6, attempts)

		assert.Equal(t, []time.Duration{
			10 * time.Millisecond,
			20 * time.Millisecond,
			40 * time.Millisecond, // capped from 80ms
			40 * time.Millisecond,
			40 * time.Millisecond, // the last attempt schedules no wait
		}, scheduledWaits(t, &buf))
	})

	t.Run("jitter stays inside half-to-one-and-a-half the computed wait", func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		inner := &retryMockClient{getHealth: func(context.Context) (Health, error) {
			return Health{}, errors.New("dial tcp: connection refused")
		}}
		rc := NewRetryClient(inner, RetryConfig{
			MaxAttempts: 25,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  time.Millisecond,
			Jitter:      true,
			Logger:      log,
		})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)

		waits := scheduledWaits(t, &buf)
		require.Len(t, waits, 24)

		distinct := map[time.Duration]bool{}
		for i, w := range waits {
			assert.GreaterOrEqual(t, w, time.Millisecond/2, "wait %d must not be below half the base", i)
			assert.Less(t, w, 3*time.Millisecond/2, "wait %d must stay under 1.5× the base", i)
			distinct[w] = true
		}
		assert.Greater(t, len(distinct), 1,
			"every scheduled wait was identical (%v) — jitter is not being applied", waits[0])
	})

	t.Run("a provider hint replaces the schedule", func(t *testing.T) {
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		var attempts int
		inner := &retryMockClient{getHealth: func(context.Context) (Health, error) {
			attempts++
			if attempts == 1 {
				return Health{}, &RateLimitedError{StatusCode: 429, RetryAfter: 25 * time.Millisecond}
			}
			return Health{}, errors.New("dial tcp: connection refused")
		}}
		rc := NewRetryClient(inner, RetryConfig{
			MaxAttempts: 3,
			BaseBackoff: 5 * time.Millisecond,
			MaxBackoff:  40 * time.Millisecond,
			Jitter:      false,
			Logger:      log,
		})

		_, err := rc.GetHealth(context.Background())
		require.Error(t, err)

		// The hint is honored once; the following transport failure falls
		// back to the doubled exponential value (5ms × 2), which shows the
		// two sources are independent rather than hint-sticky.
		assert.Equal(t, []time.Duration{25 * time.Millisecond, 10 * time.Millisecond}, scheduledWaits(t, &buf))
		out := buf.String()
		assert.Contains(t, out, "source=retry_after")
		assert.Contains(t, out, "source=backoff")
	})
}

// TestSleepCtx covers the only place the retry loop blocks: it must wait the
// requested duration, wake early on cancellation, and treat a non-positive
// duration as "already elapsed" rather than an error.
func TestSleepCtx(t *testing.T) {
	t.Run("waits the requested duration", func(t *testing.T) {
		start := time.Now()
		require.True(t, sleepCtx(context.Background(), 15*time.Millisecond))
		assert.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)
	})

	t.Run("returns false when canceled during the wait", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		start := time.Now()
		require.False(t, sleepCtx(ctx, time.Hour), "a cancel must cut the hour-long wait short")
		assert.Less(t, time.Since(start), time.Second, "the wait must end on the cancel, not the hour")
	})

	t.Run("non-positive durations defer to the context", func(t *testing.T) {
		tests := []struct {
			name string
			ctx  func() (context.Context, context.CancelFunc)
			d    time.Duration
			want bool
		}{
			{name: "zero with a live context", ctx: liveCtx, d: 0, want: true},
			{name: "negative with a live context", ctx: liveCtx, d: -time.Second, want: true},
			{name: "zero with a dead context", ctx: deadCtx, d: 0, want: false},
			{name: "negative with a dead context", ctx: deadCtx, d: -time.Second, want: false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx, cancel := tt.ctx()
				defer cancel()
				assert.Equal(t, tt.want, sleepCtx(ctx, tt.d))
			})
		}
	})
}

func liveCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

func deadCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}

// TestRetryExhaustionMessageKeepsTheCause checks that the wrapped cause
// survives the "exhausted N retries" wrapper, because operators read that
// single line as the diagnosis of a provider outage.
func TestRetryExhaustionMessageKeepsTheCause(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.1:443: connect: connection refused")
	inner := &retryMockClient{getEvents: func(context.Context, GetEventsRequest) (GetEventsResponse, error) {
		return GetEventsResponse{}, cause
	}}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})

	_, err := rc.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.Error(t, err)
	assert.ErrorIs(t, err, cause, "callers classify on the cause, so it must stay unwrappable")
	assert.Contains(t, err.Error(), "exhausted 2 retries")
	assert.True(t, strings.Contains(err.Error(), "connection refused"))
}
