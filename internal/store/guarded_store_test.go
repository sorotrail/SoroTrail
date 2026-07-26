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
