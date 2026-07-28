package store

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deadlineStore struct {
	Store
	deadline chan time.Time
}

func (s *deadlineStore) QueryEvents(ctx context.Context, _ EventFilter) ([]Event, string, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Time{}
	}
	select {
	case s.deadline <- deadline:
	default:
	}
	<-ctx.Done()
	return nil, "", ctx.Err()
}

func TestGuardedStore_PreservesEarlierCallerDeadline(t *testing.T) {
	base := &deadlineStore{deadline: make(chan time.Time, 1)}
	guarded := NewGuardedStore(base, GuardedStoreOptions{
		Timeout:            time.Second,
		SlowQueryThreshold: time.Hour,
		Logger:             slog.Default(),
	})

	callerDeadline := time.Now().Add(25 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	_, _, err := guarded.QueryEvents(ctx, EventFilter{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case queryDeadline := <-base.deadline:
		require.False(t, queryDeadline.IsZero(), "store query did not receive a context deadline")
		assert.WithinDuration(t, callerDeadline, queryDeadline, 5*time.Millisecond)
	default:
		t.Fatal("store query did not report its context deadline")
	}
}
