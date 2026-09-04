package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retentionStoreFake struct {
	cutoffs []time.Time
	deleted int64
	err     error
	calls   chan struct{}
}

func (f *retentionStoreFake) PruneEventsBefore(_ context.Context, cutoff time.Time) (int64, error) {
	f.cutoffs = append(f.cutoffs, cutoff)
	if f.calls != nil {
		f.calls <- struct{}{}
	}
	return f.deleted, f.err
}

func TestRetentionPruner_DefaultsAndInitialPass(t *testing.T) {
	fake := &retentionStoreFake{deleted: 4}
	pruner := NewRetentionPruner(fake, slog.New(slog.NewTextHandler(io.Discard, nil)), RetentionOptions{
		Age: 24 * time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pruner.Run(ctx) }()
	cancel()

	assert.ErrorIs(t, <-done, context.Canceled)
	require.Len(t, fake.cutoffs, 1)
	assert.WithinDuration(t, time.Now().Add(-24*time.Hour), fake.cutoffs[0], time.Second)
	assert.Equal(t, time.Hour, pruner.opts.PollInterval)
}

func TestRetentionPruner_DisabledAge(t *testing.T) {
	fake := &retentionStoreFake{}
	pruner := NewRetentionPruner(fake, slog.Default(), RetentionOptions{})

	assert.NoError(t, pruner.Run(context.Background()))
	assert.Empty(t, fake.cutoffs)
}

func TestRetentionPruner_ContinuesAfterStoreError(t *testing.T) {
	fake := &retentionStoreFake{err: errors.New("database unavailable"), calls: make(chan struct{}, 2)}
	pruner := NewRetentionPruner(fake, slog.New(slog.NewTextHandler(io.Discard, nil)), RetentionOptions{
		Age:          time.Hour,
		PollInterval: time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pruner.Run(ctx) }()
	for range 2 {
		select {
		case <-fake.calls:
		case <-time.After(time.Second):
			t.Fatal("retention pruner did not retry after store error")
		}
	}
	cancel()

	assert.ErrorIs(t, <-done, context.Canceled)
	assert.GreaterOrEqual(t, len(fake.cutoffs), 2)
}
