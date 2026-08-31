// Package workerhealth provides a shared mechanism for background workers to
// report their last-run time and outcome. The API layer reads these snapshots
// to surface worker health in /stats and /readyz, distinguishing healthy,
// idle and wedged workers.
//
// Concurrency: all methods are safe for concurrent use. A worker writes via
// Report; the API reads via Snapshot.
package workerhealth

import (
	"sync"
	"time"
)

// Outcome is the result of a worker's most recent run.
type Outcome string

const (
	OutcomeHealthy Outcome = "healthy"
	OutcomeError   Outcome = "error"
	OutcomeIdle    Outcome = "idle"
)

// Status is a point-in-time snapshot of a single worker's health.
type Status struct {
	Worker  string    `json:"worker"`
	Outcome Outcome   `json:"outcome"`
	LastRun time.Time `json:"last_run"`
	Error   string    `json:"error,omitempty"`
	Details string    `json:"details,omitempty"`
}

// Tracker holds health state for all registered background workers.
// It is created once at startup and shared between workers (writers)
// and the API (readers).
type Tracker struct {
	mu      sync.RWMutex
	workers map[string]*Status
}

// New creates a Tracker. Call Register before any worker starts so
// /stats always knows which workers exist.
func New() *Tracker {
	return &Tracker{workers: make(map[string]*Status)}
}

// Register announces a worker. A worker that hasn't run yet is shown
// as idle with a zero LastRun — this is distinguishable from a wedged
// worker (which has a non-zero LastRun but no recent outcome change).
func (t *Tracker) Register(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workers[name] = &Status{Worker: name, Outcome: OutcomeIdle}
}

// Report updates the named worker's health after a run completes.
// outcome is Healthy when the run succeeded, Error when it failed.
// details is free-form text (e.g. "ingested 42 events") and may be empty.
func (t *Tracker) Report(name string, outcome Outcome, err error, details string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.workers[name]
	if !ok {
		// Lazily register workers that weren't pre-registered.
		s = &Status{Worker: name}
		t.workers[name] = s
	}
	s.Outcome = outcome
	s.LastRun = time.Now().UTC()
	s.Details = details
	if err != nil {
		s.Error = err.Error()
	} else {
		s.Error = ""
	}
}

// Snapshot returns a copy of every registered worker's status. The
// caller gets its own slice so it can marshal it without holding the lock.
func (t *Tracker) Snapshot() []Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Status, 0, len(t.workers))
	for _, s := range t.workers {
		out = append(out, *s)
	}
	return out
}

// Get returns the status of a single worker, or nil if not registered.
func (t *Tracker) Get(name string) *Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.workers[name]
	if !ok {
		return nil
	}
	cp := *s
	return &cp
}
