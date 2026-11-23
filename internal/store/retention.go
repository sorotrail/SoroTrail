package store

import (
	"context"
	"log/slog"
	"time"
)

const DefaultRetentionPollInterval = time.Hour

// RetentionOptions configures the background event-pruning job.
type RetentionOptions struct {
	Age          time.Duration
	PollInterval time.Duration
	// Policies, when set, applies per-tenant retention (#1249) with Age as
	// the instance default, so this job never deletes a shared contract's
	// events before the longest horizon of the tenants entitled to it.
	Policies RetentionPolicyStore
}

// RetentionStore is the persistence surface required by the retention job.
type RetentionStore interface {
	PruneEventsBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// RetentionPruner periodically removes events older than the configured age.
// An age of zero disables the job; callers should avoid starting it in that
// case so the default deployment retains its existing behavior.
type RetentionPruner struct {
	store RetentionStore
	log   *slog.Logger
	opts  RetentionOptions
}

func NewRetentionPruner(st RetentionStore, log *slog.Logger, opts RetentionOptions) *RetentionPruner {
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultRetentionPollInterval
	}
	return &RetentionPruner{store: st, log: log, opts: opts}
}

// Run performs an initial pruning pass and then continues until ctx is done.
// Database errors are logged and retried on the next interval.
func (p *RetentionPruner) Run(ctx context.Context) error {
	if p.opts.Age <= 0 {
		return nil
	}

	p.prune(ctx)
	ticker := time.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.prune(ctx)
		}
	}
}

func (p *RetentionPruner) prune(ctx context.Context) {
	plan, err := LoadRetentionPlan(ctx, p.opts.Policies, p.opts.Age)
	if err != nil {
		p.log.Error("event retention pruning failed", "error", err)
		return
	}
	for _, g := range plan.Groups {
		// This job is age-only: a group with no age bound keeps its events.
		if g.MaxAge <= 0 {
			continue
		}
		cutoff := time.Now().Add(-g.MaxAge)
		var deleted int64
		if g.Contracts.All() {
			deleted, err = p.store.PruneEventsBefore(ctx, cutoff)
		} else {
			deleted, err = p.opts.Policies.PruneEventsBeforeIn(ctx, cutoff, g.Contracts)
		}
		if err != nil {
			p.log.Error("event retention pruning failed", "error", err, "policy", g.Policy)
			return
		}
		if deleted > 0 {
			p.log.Info("event retention pruning complete", "deleted", deleted, "cutoff", cutoff, "policy", g.Policy)
		}
	}
}
