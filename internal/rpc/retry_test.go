package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryMockClient implements Client for testing.
type retryMockClient struct {
	getEvents        func(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error)
	getLatestLedger  func(ctx context.Context) (LatestLedger, error)
	getHealth        func(ctx context.Context) (Health, error)
	getLedgerEntries func(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error)
}

func (m *retryMockClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	if m.getEvents != nil {
		return m.getEvents(ctx, req)
	}
	return GetEventsResponse{}, nil
}

func (m *retryMockClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	if m.getLatestLedger != nil {
		return m.getLatestLedger(ctx)
	}
	return LatestLedger{}, nil
}

func (m *retryMockClient) GetHealth(ctx context.Context) (Health, error) {
	if m.getHealth != nil {
		return m.getHealth(ctx)
	}
	return Health{}, nil
}

func (m *retryMockClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	if m.getLedgerEntries != nil {
		return m.getLedgerEntries(ctx, req)
	}
	return GetLedgerEntriesResponse{}, nil
}

func (m *retryMockClient) SimulateTransaction(ctx context.Context, req SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	return SimulateTransactionResponse{}, nil
}

func TestRetryClient_SuccessOnFirstAttempt(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			return Health{Status: "healthy", LatestLedger: 100}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	health, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", health.Status)
	assert.Equal(t, 1, calls, "should succeed on first attempt")
}

func TestRetryClient_RetriesOnTransientError(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			if calls < 3 {
				return Health{}, &Error{Code: 0, Message: "server error"}
			}
			return Health{Status: "healthy", LatestLedger: 100}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 5, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Jitter: false})
	health, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", health.Status)
	assert.Equal(t, 3, calls, "should succeed on third attempt")
}

func TestRetryClient_ExhaustsRetries(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			return Health{}, &Error{Code: 0, Message: "persistent error"}
		},
	}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Jitter: false})
	_, err := rc.GetHealth(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exhausted 3 retries")
	assert.Equal(t, 3, calls, "should attempt exactly 3 times")
}

func TestRetryClient_NonRetryableError(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			return Health{}, &Error{Code: -32601, Message: "Method not found"}
		},
	}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	_, err := rc.GetHealth(context.Background())
	require.Error(t, err)
	assert.Equal(t, 1, calls, "should not retry non-retryable errors")
}

func TestRetryClient_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	_, err := rc.GetHealth(ctx)
	require.ErrorIs(t, err, context.Canceled)
	// When the context is already cancelled, the first attempt is skipped
	// because the context check happens before the inner call.
	assert.Equal(t, 0, calls, "context cancelled; inner function is never called")
}

func TestRetryClient_BackoffRespectsMax(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(ctx context.Context) (Health, error) {
			calls++
			return Health{}, errors.New("EOF")
		},
	}
	// Very small base, small max — backoff is capped on second retry.
	rc := NewRetryClient(inner, RetryConfig{MaxAttempts: 5, BaseBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, Jitter: false})
	_, err := rc.GetHealth(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exhausted 5 retries")
	assert.Equal(t, 5, calls)
}

func TestIsRetryable_ErrorCodes(t *testing.T) {
	tests := []struct {
		err       error
		name      string
		retryable bool
	}{
		{&Error{Code: 0, Message: "server error"}, "code 0", true},
		{&Error{Code: -32000, Message: "ledger out of range"}, "code -32000", true},
		{&Error{Code: -32600, Message: "Invalid Request"}, "code -32600 Invalid Request", false},
		{&Error{Code: -32601, Message: "Method not found"}, "code -32601 Method not found", false},
		{errors.New("connection refused"), "connection refused", true},
		{errors.New("EOF"), "EOF", true},
		{context.Canceled, "context canceled", false},
		{context.DeadlineExceeded, "deadline", false},
		{errors.New("file not found"), "file not found", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.retryable, isRetryable(tt.err))
		})
	}
}

func TestNewRetryClient_DefaultConfig(t *testing.T) {
	rc := NewRetryClient(&retryMockClient{}, RetryConfig{})
	assert.Equal(t, 3, rc.config.MaxAttempts)
	assert.Equal(t, 500*time.Millisecond, rc.config.BaseBackoff)
	assert.Equal(t, 30*time.Second, rc.config.MaxBackoff)
}

// --- Issue #58: Retry-After-aware backoff ---

func TestIsRetryable_RateLimited(t *testing.T) {
	assert.True(t, isRetryable(&RateLimitedError{StatusCode: http.StatusTooManyRequests}),
		"429 without a Retry-After hint is still worth retrying")
	assert.True(t, isRetryable(&RateLimitedError{StatusCode: http.StatusTooManyRequests, RetryAfter: 5 * time.Second}))
	assert.True(t, isRetryable(fmt.Errorf("wrapped: %w", &RateLimitedError{StatusCode: 429})),
		"retryability must survive error wrapping")
}

// TestRetryWait covers the wait decision itself (no sleeping): the
// provider hint wins and is capped; anything else keeps the computed,
// jittered backoff.
func TestRetryWait(t *testing.T) {
	t.Run("hint honored", func(t *testing.T) {
		rc := NewRetryClient(nil, RetryConfig{BaseBackoff: time.Hour, MaxBackoff: time.Hour, Jitter: false})
		wait, source := rc.retryWait(&RateLimitedError{StatusCode: 429, RetryAfter: 3 * time.Second}, time.Hour)
		assert.Equal(t, 3*time.Second, wait)
		assert.Equal(t, "retry_after", source)
	})
	t.Run("hint capped at sane maximum", func(t *testing.T) {
		rc := NewRetryClient(nil, RetryConfig{Jitter: false})
		wait, source := rc.retryWait(&RateLimitedError{StatusCode: 429, RetryAfter: maxRetryAfterWait*10 + time.Second}, time.Millisecond)
		assert.Equal(t, maxRetryAfterWait, wait)
		assert.Equal(t, "retry_after", source)
	})
	t.Run("absent hint falls back to computed backoff", func(t *testing.T) {
		rc := NewRetryClient(nil, RetryConfig{BaseBackoff: 40 * time.Millisecond, MaxBackoff: time.Hour, Jitter: false})
		wait, source := rc.retryWait(&RateLimitedError{StatusCode: 429}, 40*time.Millisecond)
		assert.Equal(t, 40*time.Millisecond, wait)
		assert.Equal(t, "backoff", source)
	})
	t.Run("non-429 errors use backoff even with hint in play", func(t *testing.T) {
		rc := NewRetryClient(nil, RetryConfig{BaseBackoff: time.Second, MaxBackoff: time.Hour, Jitter: false})
		wait, source := rc.retryWait(errors.New("EOF"), time.Second)
		assert.Equal(t, time.Second, wait)
		assert.Equal(t, "backoff", source)
	})
	t.Run("jitter applies to backoff but never to hints", func(t *testing.T) {
		rc := NewRetryClient(nil, RetryConfig{BaseBackoff: time.Second, MaxBackoff: time.Hour, Jitter: true})
		for i := 0; i < 20; i++ {
			wait, source := rc.retryWait(errors.New("EOF"), time.Second)
			assert.Equal(t, "backoff", source)
			assert.GreaterOrEqual(t, wait, 500*time.Millisecond)
			assert.Less(t, wait, 1500*time.Millisecond)

			wait, source = rc.retryWait(&RateLimitedError{StatusCode: 429, RetryAfter: 700 * time.Millisecond}, time.Second)
			assert.Equal(t, "retry_after", source)
			assert.Equal(t, 700*time.Millisecond, wait, "hints are honored as-is, un-jittered")
		}
	})
}

// TestRetryClient_RetryAfterSecondsHonored proves end-to-end that a
// delta-seconds hint overrides a much larger computed backoff.
func TestRetryClient_RetryAfterSecondsHonored(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(context.Context) (Health, error) {
			calls++
			if calls == 1 {
				return Health{}, &RateLimitedError{StatusCode: 429, RetryAfter: 80 * time.Millisecond}
			}
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Second, // would dominate if the hint were ignored
		MaxBackoff:  10 * time.Second,
		Jitter:      false,
	})

	start := time.Now()
	_, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond, "must wait at least the hinted duration")
	assert.Less(t, elapsed, 3*time.Second, "hint must win over the 10s computed backoff")
}

// TestRetryClient_RetryAfterHTTPDateHonored exercises the HTTP-date form
// of the header through a real HTTP round trip.
func TestRetryClient_RetryAfterHTTPDateHonored(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			// HTTP-date has second granularity; +2s guarantees the parsed
			// hint is still ≥1s away even after truncation.
			w.Header().Set("Retry-After", time.Now().UTC().Add(2*time.Second).Format(http.TimeFormat))
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"healthy"}}`))
	}))
	defer srv.Close()

	rc := NewRetryClient(
		NewHTTPClient(srv.URL, WithMinRequestInterval(0)),
		RetryConfig{
			MaxAttempts: 2,
			BaseBackoff: 30 * time.Second, // ignoring the date would stall ~30s
			MaxBackoff:  30 * time.Second,
			Jitter:      false,
		},
	)

	start := time.Now()
	h, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", h.Status)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "must wait until roughly the hinted date")
	assert.Less(t, elapsed, 15*time.Second, "HTTP-date hint must win over the 30s computed backoff")
}

// TestRetryClient_RateLimitedWithoutHintKeepsBackoff pins the fallback:
// a 429 with no usable Retry-After leaves the existing backoff untouched.
func TestRetryClient_RateLimitedWithoutHintKeepsBackoff(t *testing.T) {
	var calls int
	inner := &retryMockClient{
		getHealth: func(context.Context) (Health, error) {
			calls++
			if calls < 3 {
				return Health{}, &RateLimitedError{StatusCode: 429}
			}
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  time.Hour,
		Jitter:      false,
	})

	start := time.Now()
	_, err := rc.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
	// Two waits of 20ms + 40ms — exactly today's exponential schedule.
	assert.GreaterOrEqual(t, time.Since(start), 60*time.Millisecond)
	assert.Less(t, time.Since(start), 2*time.Second)
}

// TestRetryClient_DebugLogNotesSource checks the acceptance criterion that
// the wait's origin ("retry_after" vs "backoff") is logged at debug level.
func TestRetryClient_DebugLogNotesSource(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var calls int
	inner := &retryMockClient{
		getHealth: func(context.Context) (Health, error) {
			calls++
			if calls == 1 {
				return Health{}, &RateLimitedError{StatusCode: 429, RetryAfter: 10 * time.Millisecond}
			}
			return Health{Status: "healthy"}, nil
		},
	}
	rc := NewRetryClient(inner, RetryConfig{
		MaxAttempts: 2,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  time.Millisecond,
		Jitter:      false,
		Logger:      log,
	})

	_, err := rc.GetHealth(context.Background())
	require.NoError(t, err)

	out := buf.String()
	assert.Contains(t, out, `source=retry_after`)
	assert.Contains(t, out, `level=DEBUG`)
	assert.False(t, strings.Contains(out, "source=backoff"),
		"only one retry happened and it came from the hint")
}

// containsStr is the byte-exact substring scan behind isTransientHTTP's
// fragment list. The contract worth pinning down: an empty needle matches
// anything, a non-empty needle must appear verbatim — case-sensitively and
// in full, with no prefix or token matching — and an empty haystack (the
// string analog of an empty slice) can only match an empty needle. The
// table below locks in that boundary so a future rewrite cannot silently
// drift into case-insensitive or partial matching, which would
// misclassify errors as transient.
func TestContainsStr(t *testing.T) {
	tests := []struct {
		name     string
		haystack string
		needle   string
		want     bool
	}{
		{
			// The everyday case: the fragment the caller is looking for
			// appears inside a longer message, like "connection refused"
			// inside a dial error.
			name:     "a present value is found",
			haystack: "dial tcp: connection refused",
			needle:   "connection refused",
			want:     true,
		},
		{
			// A fragment may sit anywhere in the message, not just at a
			// word boundary — isTransientHTTP matches on substrings.
			name:     "a fragment inside a message is found",
			haystack: "i/o timeout after 30s",
			needle:   "timeout",
			want:     true,
		},
		{
			// The mirror case: a fragment that is not present must report
			// false, so an unrelated error is never misclassified as
			// transient.
			name:     "an absent value is not found",
			haystack: "no such host",
			needle:   "connection reset",
			want:     false,
		},
		{
			// An empty haystack (the string analog of an empty slice) can
			// contain nothing, so any non-empty needle is absent.
			name:     "an empty haystack holds no non-empty needle",
			haystack: "",
			needle:   "EOF",
			want:     false,
		},
		{
			// The documented degenerate case: an empty needle matches
			// anything — a fragment isTransientHTTP never passes, but the
			// behavior is pinned down anyway.
			name:     "an empty needle matches any haystack",
			haystack: "anything at all",
			needle:   "",
			want:     true,
		},
		{
			// Matching is exact and case-sensitive: a differently-cased
			// fragment must not match, or a capitalized "Connection
			// refused" would be classified as transient when the code
			// looks for the lowercase form.
			name:     "matching is exact, not case-insensitive",
			haystack: "Connection refused",
			needle:   "connection refused",
			want:     false,
		},
		{
			// The needle must appear in full — a shared prefix is not a
			// match ("timed out" is not "timeout").
			name:     "the full needle must appear, not a fragment of it",
			haystack: "timed out",
			needle:   "timeout",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsStr(tt.haystack, tt.needle)
			assert.Equal(t, tt.want, got)
		})
	}
}
