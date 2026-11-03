package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

func TestIsUnavailable_Classification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, want: false},
		{name: "ErrNotFound", err: ErrNotFound, want: false},
		{name: "ErrInvalidCursor", err: ErrInvalidCursor, want: false},
		{name: "generic validation error", err: errors.New("invalid filter syntax"), want: false},
		{name: "pg syntax error 42601", err: &pgconn.PgError{Code: "42601", Message: "syntax error"}, want: false},
		{name: "ErrUnavailable sentinel", err: ErrUnavailable, want: true},
		{name: "wrapped ErrUnavailable", err: fmt.Errorf("query events: %w", ErrUnavailable), want: true},
		{name: "io.EOF", err: io.EOF, want: true},
		{name: "io.ErrUnexpectedEOF", err: io.ErrUnexpectedEOF, want: true},
		{name: "pg class 08 connection failure", err: &pgconn.PgError{Code: "08006", Message: "connection failure"}, want: true},
		{name: "pg admin shutdown 57P01", err: &pgconn.PgError{Code: "57P01", Message: "admin shutdown"}, want: true},
		{name: "pg crash shutdown 57P02", err: &pgconn.PgError{Code: "57P02", Message: "crash shutdown"}, want: true},
		{name: "pg cannot connect now 57P03", err: &pgconn.PgError{Code: "57P03", Message: "cannot connect now"}, want: true},
		{name: "pg too many connections 53300", err: &pgconn.PgError{Code: "53300", Message: "too many connections"}, want: true},
		{
			name: "net.OpError dial tcp",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
			},
			want: true,
		},
		{
			name: "net.DNSError lookup",
			err:  &net.DNSError{Err: "no such host", Name: "postgres.internal"},
			want: true,
		},
		{name: "connection refused message", err: errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"), want: true},
		{name: "pool unhealthy message", err: errors.New("pool unhealthy: connection reset by peer"), want: true},
		{name: "closed pool message", err: errors.New("closed pool"), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsUnavailable(tt.err))
		})
	}
}

func TestDatabaseHealth_DeduplicatesOutageLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	h := NewDatabaseHealth(logger)
	h.now = func() time.Time { return now }

	assert.False(t, h.Degraded())

	// Non-availability error must not flip degraded state or log.
	h.Observe(ErrNotFound)
	assert.False(t, h.Degraded())
	assert.Empty(t, buf.String())

	// First availability error transitions into degraded mode and logs once.
	connErr := errors.New("dial tcp 127.0.0.1:5432: connection refused")
	h.Observe(connErr)
	assert.True(t, h.Degraded())
	assert.Equal(t, connErr, h.LastError())
	assert.Equal(t, uint64(0), h.SuppressedCount())
	assert.Equal(t, 1, strings.Count(buf.String(), "database unavailable; entering degraded mode"))

	// Repeated outage errors do not emit additional warn lines.
	for i := 0; i < 20; i++ {
		now = now.Add(time.Second)
		h.Observe(connErr)
	}
	assert.Equal(t, uint64(20), h.SuppressedCount())
	assert.Equal(t, 1, strings.Count(buf.String(), "database unavailable; entering degraded mode"),
		"repeated outage errors must be suppressed so log volume stays bounded")

	// Recovery logs once with the outage duration and suppressed count.
	now = now.Add(5 * time.Second)
	h.Observe(nil)
	assert.False(t, h.Degraded())
	assert.Nil(t, h.LastError())
	assert.Equal(t, uint64(0), h.SuppressedCount())
	assert.Equal(t, 1, strings.Count(buf.String(), "database reachable again"))
	assert.Contains(t, buf.String(), "suppressed_errors=20")
}

func TestStoreBreaker_TripsHalfOpensAndRecovers(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	b := NewStoreBreaker(
		WithFailureThreshold(3),
		WithCoolDown(10*time.Second),
		WithMaxCoolDown(40*time.Second),
		WithBreakerClock(func() time.Time { return now }),
	)

	assert.Equal(t, BreakerClosed, b.State())
	assert.False(t, b.Degraded())
	assert.True(t, b.Allow())
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("closed")))
	assert.Equal(t, 0.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("open")))

	connErr := errors.New("connection refused")
	b.RecordFailure(connErr)
	b.RecordFailure(connErr)
	assert.Equal(t, BreakerClosed, b.State(), "below threshold must stay closed")
	assert.Equal(t, 2, b.ConsecutiveFailures())

	// Third consecutive failure trips the breaker open.
	b.RecordFailure(connErr)
	assert.Equal(t, BreakerOpen, b.State())
	assert.True(t, b.Degraded())
	assert.False(t, b.Allow(), "open breaker must short-circuit requests during cool-down")
	assert.Equal(t, 10*time.Second, b.CoolDownRemaining())
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("open")))
	assert.Equal(t, 0.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("closed")))

	// Advance past cool-down: state becomes half-open and a single probe is admitted.
	now = now.Add(10 * time.Second)
	assert.Equal(t, BreakerHalfOpen, b.State())
	assert.True(t, b.Allow(), "first caller after cool-down is admitted as the recovery probe")
	assert.False(t, b.Allow(), "concurrent callers must wait while the half-open probe is in flight")
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("half-open")))

	// Failed probe re-opens the breaker with doubled cool-down (20s).
	b.RecordFailure(connErr)
	assert.Equal(t, BreakerOpen, b.State())
	assert.Equal(t, 20*time.Second, b.CoolDownRemaining())

	// Advance past the doubled cool-down and succeed on the next probe.
	now = now.Add(20 * time.Second)
	require.True(t, b.Allow())
	b.RecordSuccess()

	assert.Equal(t, BreakerClosed, b.State())
	assert.False(t, b.Degraded())
	assert.Equal(t, 0, b.ConsecutiveFailures())
	assert.Nil(t, b.LastError())
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.DBCircuitBreakerState.WithLabelValues("closed")))
}

func TestStoreBreaker_ObserveIgnoresDomainErrors(t *testing.T) {
	b := NewStoreBreaker(WithFailureThreshold(1))

	b.Observe(ErrNotFound)
	b.Observe(ErrInvalidCursor)
	b.Observe(context.Canceled)
	assert.Equal(t, BreakerClosed, b.State())
	assert.Equal(t, 0, b.ConsecutiveFailures())

	b.Observe(ErrUnavailable)
	assert.Equal(t, BreakerOpen, b.State())
	assert.Equal(t, 1, b.ConsecutiveFailures())
}

func TestDefaultJitter_StaysWithinBounds(t *testing.T) {
	base := 10 * time.Second
	for i := 0; i < 50; i++ {
		got := DefaultJitter(base)
		assert.GreaterOrEqual(t, got, 5*time.Second)
		assert.LessOrEqual(t, got, 10*time.Second)
	}
	assert.Equal(t, time.Duration(0), DefaultJitter(0))
}
