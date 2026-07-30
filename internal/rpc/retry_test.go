package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockClient implements Client for testing.
type mockClient struct {
	getEvents        func(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error)
	getLatestLedger  func(ctx context.Context) (LatestLedger, error)
	getHealth        func(ctx context.Context) (Health, error)
	getLedgerEntries func(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error)
}

func (m *mockClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	if m.getEvents != nil {
		return m.getEvents(ctx, req)
	}
	return GetEventsResponse{}, nil
}

func (m *mockClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	if m.getLatestLedger != nil {
		return m.getLatestLedger(ctx)
	}
	return LatestLedger{}, nil
}

func (m *mockClient) GetHealth(ctx context.Context) (Health, error) {
	if m.getHealth != nil {
		return m.getHealth(ctx)
	}
	return Health{}, nil
}

func (m *mockClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	if m.getLedgerEntries != nil {
		return m.getLedgerEntries(ctx, req)
	}
	return GetLedgerEntriesResponse{}, nil
}

func TestRetryClient_SuccessOnFirstAttempt(t *testing.T) {
	var calls int
	inner := &mockClient{
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
	inner := &mockClient{
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
	inner := &mockClient{
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
	inner := &mockClient{
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
	inner := &mockClient{
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
	inner := &mockClient{
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
	rc := NewRetryClient(&mockClient{}, RetryConfig{})
	assert.Equal(t, 3, rc.config.MaxAttempts)
	assert.Equal(t, 500*time.Millisecond, rc.config.BaseBackoff)
	assert.Equal(t, 30*time.Second, rc.config.MaxBackoff)
}
