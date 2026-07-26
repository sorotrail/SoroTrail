package store

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testGuardedStore struct {
	Store
	queryCalls int
}

func (s *testGuardedStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	s.queryCalls++
	<-ctx.Done()
	return nil, "", ctx.Err()
}

func TestGuardedStore_EnforcesContextTimeout(t *testing.T) {
	base := &testGuardedStore{}
	guarded := NewGuardedStore(base, GuardedStoreOptions{Timeout: 10 * time.Millisecond, SlowQueryThreshold: time.Millisecond, Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})

	_, _, err := guarded.QueryEvents(context.Background(), EventFilter{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, base.queryCalls)
}

func TestGuardedStore_LogsSlowQueries(t *testing.T) {
	var buf strings.Builder
	base := &testGuardedStore{}
	guarded := NewGuardedStore(base, GuardedStoreOptions{Timeout: 50 * time.Millisecond, SlowQueryThreshold: 1 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&buf, nil))})

	_, _, err := guarded.QueryEvents(context.Background(), EventFilter{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, buf.String(), "slow store query")
}

// errorStore is a stub that returns errors for every method so we can
// verify the query-error counter increments independently of the
// slow-query threshold.
type errorStore struct {
	Store
	err error
}

func (s *errorStore) Ping(ctx context.Context) error { return s.err }
func (s *errorStore) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	return nil, "", s.err
}
func (s *errorStore) GetEvent(ctx context.Context, id string) (Event, error)   { return Event{}, s.err }
func (s *errorStore) EventExists(ctx context.Context, id string) (bool, error) { return false, s.err }
func (s *errorStore) Stats(ctx context.Context) (Stats, error)                 { return Stats{}, s.err }

func TestGuardedStore_CountsQueryErrors(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(s Store) // runs queries that should fail
		expect uint64
	}{
		{
			name:   "no errors",
			setup:  func(s Store) {},
			expect: 0,
		},
		{
			name: "single QueryEvents error",
			setup: func(s Store) {
				_, _, _ = s.QueryEvents(context.Background(), EventFilter{})
			},
			expect: 1,
		},
		{
			name: "errors from multiple methods accumulate",
			setup: func(s Store) {
				_, _, _ = s.QueryEvents(context.Background(), EventFilter{})
				_, _ = s.GetEvent(context.Background(), "e1")
				_, _ = s.EventExists(context.Background(), "e2")
				_ = s.Ping(context.Background())
			},
			expect: 4,
		},
		{
			name: "Stats call error counted once via logSlowQuery",
			setup: func(s Store) {
				_, _ = s.Stats(context.Background())
			},
			expect: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := &errorStore{err: assert.AnError}
			guarded := NewGuardedStore(base, GuardedStoreOptions{
				Timeout:            time.Second,
				SlowQueryThreshold: time.Hour,
				Logger:             slog.New(slog.NewTextHandler(&strings.Builder{}, nil)),
			})

			tt.setup(guarded)

			// Now read back the counter through Stats().  The underlying
			// errorStore.Stats() also returns an error so this call itself
			// will increment the counter by one — account for it.
			stats, err := guarded.Stats(context.Background())
			require.Error(t, err)
			assert.Equal(t, tt.expect+1, stats.QueryErrors)
		})
	}
}
