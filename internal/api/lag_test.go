package api

import (
	"context"
	"testing"
	"time"
)

type fakeLedgerProvider struct {
	head  uint32
	err   error
	calls int
}

func (f *fakeLedgerProvider) GetLatestLedger(_ context.Context) (uint32, error) {
	f.calls++
	return f.head, f.err
}

func TestLagTrackerComputesLag(t *testing.T) {
	p := &fakeLedgerProvider{head: 1000}
	tr := NewLagTracker(p, 0)
	lag, err := tr.Lag(context.Background(), 950)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lag != 50 {
		t.Fatalf("expected lag 50, got %d", lag)
	}
}

func TestLagTrackerCaughtUp(t *testing.T) {
	p := &fakeLedgerProvider{head: 1000}
	tr := NewLagTracker(p, time.Minute)
	lag, err := tr.Lag(context.Background(), 1005)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lag != 0 {
		t.Fatalf("expected lag 0 when caught up, got %d", lag)
	}
}

func TestLagTrackerCachesLatestLedger(t *testing.T) {
	p := &fakeLedgerProvider{head: 1000}
	tr := NewLagTracker(p, time.Minute)
	if _, err := tr.LatestLedger(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := tr.LatestLedger(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.calls != 1 {
		t.Fatalf("expected 1 provider call (cached), got %d", p.calls)
	}
}
