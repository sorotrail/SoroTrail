package workerhealth

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracker_RegisterAndSnapshot(t *testing.T) {
	tr := New()
	tr.Register("ingester")
	tr.Register("pruner")

	snap := tr.Snapshot()
	require.Len(t, snap, 2)

	byName := map[string]Status{}
	for _, s := range snap {
		byName[s.Worker] = s
	}

	assert.Equal(t, OutcomeIdle, byName["ingester"].Outcome,
		"a newly registered worker starts as idle")
	assert.True(t, byName["ingester"].LastRun.IsZero(),
		"a worker that hasn't run yet has a zero LastRun")
}

func TestTracker_ReportHealthy(t *testing.T) {
	tr := New()
	tr.Register("ingester")

	tr.Report("ingester", OutcomeHealthy, nil, "ingested 42 events")

	s := tr.Get("ingester")
	require.NotNil(t, s)
	assert.Equal(t, OutcomeHealthy, s.Outcome)
	assert.False(t, s.LastRun.IsZero())
	assert.Equal(t, "ingested 42 events", s.Details)
	assert.Empty(t, s.Error)
}

func TestTracker_ReportError(t *testing.T) {
	tr := New()
	tr.Register("pruner")

	err := errors.New("database connection lost")
	tr.Report("pruner", OutcomeError, err, "")

	s := tr.Get("pruner")
	require.NotNil(t, s)
	assert.Equal(t, OutcomeError, s.Outcome)
	assert.Equal(t, "database connection lost", s.Error)
}

func TestTracker_ReportLazilyRegisters(t *testing.T) {
	tr := New()
	// Don't pre-register "webhook".
	tr.Report("webhook", OutcomeHealthy, nil, "")

	s := tr.Get("webhook")
	require.NotNil(t, s)
	assert.Equal(t, OutcomeHealthy, s.Outcome)
}

func TestTracker_GetReturnsNilForUnknown(t *testing.T) {
	tr := New()
	assert.Nil(t, tr.Get("nonexistent"))
}

func TestTracker_SnapshotReturnsCopies(t *testing.T) {
	tr := New()
	tr.Register("ingester")
	tr.Report("ingester", OutcomeHealthy, nil, "")

	snap := tr.Snapshot()
	snap[0].Error = "mutated"
	snap[0].Outcome = OutcomeError

	s := tr.Get("ingester")
	assert.Equal(t, OutcomeHealthy, s.Outcome,
		"mutating the snapshot must not affect the tracker")
}

func TestTracker_WedgedWorkerDistinguishable(t *testing.T) {
	tr := New()
	tr.Register("auditor")

	// A wedged worker ran successfully once but hasn't run since.
	old := time.Now().Add(-10 * time.Minute)
	tr.mu.Lock()
	tr.workers["auditor"].LastRun = old
	tr.workers["auditor"].Outcome = OutcomeHealthy
	tr.mu.Unlock()

	s := tr.Get("auditor")
	require.NotNil(t, s)
	// The worker is technically "healthy" from its last outcome,
	// but LastRun is stale — callers can detect the gap.
	assert.Equal(t, OutcomeHealthy, s.Outcome)
	assert.InDelta(t, time.Now().Unix(), s.LastRun.Unix(), 660,
		"LastRun should be ~10 minutes ago")
}
