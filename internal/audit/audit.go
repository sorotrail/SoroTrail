// Package audit runs a background auditor that verifies stored events
// against fresh getEvents fetches. The auditor walks recently-ingested
// ledger ranges (behind the ingest frontier, inside the RPC's retention
// window), reconciles per-ledger event counts and IDs, records any
// discrepancy as an audit_finding, and auto-repairs affected ranges.
//
// Repair is idempotent: orphaned rows (events the store holds that the RPC
// no longer reports) are deleted, and same-ID rows whose topic/value
// drifted on the RPC side are overwritten. A finding that the auditor
// cannot repair (e.g. RPC self-disagreement) is kept open with status
// "unrecoverable" after a bounded number of attempts.
//
// The auditor only ever flags events that should have been captured by
// the ingester's filter set, so watched-contract mode doesn't raise
// false alarms about events deliberately not ingested.
//
// The auditor is fully optional: when AUDIT_ENABLED is unset/false main.go
// never starts it, leaving the binary's behavior unchanged.
package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/sorotrail/sorotrail/internal/ingester"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Reingester is the surface the auditor needs from the ingester package.
// Defining it here keeps the audit package import-light and lets tests
// supply a fake.
type Reingester interface {
	BuildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error)
	ReingestRange(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32) (int, error)
	// PageLimit is the getEvents pagination cap the ingester uses; the
	// auditor reuses it so it can never silently disagree with the
	// ingester on page size for an RPC round trip.
	PageLimit() uint
}

// Options configure an Auditor. All fields have defaults applied by
// applyDefaults; zero values produce a workable auditor.
type Options struct {
	// PollInterval is the sleep between audit passes when the previous
	// pass found no work or finished cleanly. Default 30s.
	PollInterval time.Duration
	// BatchLedgers caps the ledger range audited in a single pass; the
	// auditor pages through it one cluster at a time and advances the
	// high-water mark per matching ledger. Default 100.
	BatchLedgers uint32
	// LagThreshold pauses the auditor when (lastIngested − verifiedThrough)
	// exceeds this many ledgers, so we don't race ingestion. Default 200.
	LagThreshold uint32
	// MaxRepairAttempts caps repair iterations for a single finding; the
	// finding is moved to "unrecoverable" after this and left visible.
	// Default 3.
	MaxRepairAttempts int
	// FindingMaxLedgers bounds how many ledgers a single finding spans;
	// mismatches that span more ledgers than this aren't opened as one
	// giant finding — instead the auditor logs and refuses to advance
	// the high-water mark past them until they resolve. Default 100.
	FindingMaxLedgers uint32
}

// DefaultValues exposes the documented defaults for documentation/tests.
const (
	DefaultPollInterval      = 30 * time.Second
	DefaultBatchLedgers      = uint32(100)
	DefaultLagThreshold      = uint32(200)
	DefaultMaxRepairAttempts = 3
	DefaultFindingMaxLedgers = uint32(100)
)

func (o *Options) applyDefaults() {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.BatchLedgers == 0 {
		o.BatchLedgers = DefaultBatchLedgers
	}
	if o.LagThreshold == 0 {
		o.LagThreshold = DefaultLagThreshold
	}
	if o.MaxRepairAttempts <= 0 {
		o.MaxRepairAttempts = DefaultMaxRepairAttempts
	}
	if o.FindingMaxLedgers == 0 {
		o.FindingMaxLedgers = DefaultFindingMaxLedgers
	}
}

// Auditor is the background verifier.
//
// AUDITORS RUN ON A SINGLE GOROUTINE. The metrics counters are mutated
// without synchronization; running two Auditor instances concurrently
// would silently lose increments. The schema is singleton so the side
// effects (audit_state rows, ledger findings) are still race-safe.
type Auditor struct {
	client   rpc.Client
	store    store.Store
	reingest Reingester
	log      *slog.Logger
	opts     Options
	metrics  Metrics
}

// Metrics is a snapshot of counters the auditor accumulates. They are
// surfaced by main.go via /stats (or similar) so the auditor's posture
// is observable without parsing logs.
type Metrics struct {
	PassesRun             uint64
	LedgersChecked        uint64
	FindingsOpened        uint64
	FindingsRepaired      uint64
	FindingsUnrecoverable uint64
	FindingsUnverifiable  uint64
	RPCRequests           uint64
}

// Metrics returns the auditor's accumulated counters.
func (a *Auditor) Metrics() Metrics { return a.metrics }

// New wires an Auditor. All dependencies are interfaces so tests can
// supply fakes.
func New(client rpc.Client, st store.Store, reingest Reingester, log *slog.Logger, opts Options) *Auditor {
	opts.applyDefaults()
	return &Auditor{
		client:   client,
		store:    st,
		reingest: reingest,
		log:      log,
		opts:     opts,
	}
}

// Run polls the auditor's pass loop until ctx is canceled. Like the
// ingester, errors are logged and retried with jittered exponential
// backoff; the only terminal condition is context cancellation.
func (a *Auditor) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		worked, err := a.PassOnce(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			sleep := backoff/2 + rand.N(backoff/2)
			a.log.Error("audit pass failed", "error", err, "retry_in", sleep)
			if !sleepCtx(ctx, sleep) {
				return ctx.Err()
			}
			if backoff *= 2; backoff > a.opts.PollInterval {
				backoff = a.opts.PollInterval
			}
		default:
			a.metrics.PassesRun++
			backoff = time.Second
			_ = worked // silence lints; future scheduling may use it
			if !sleepCtx(ctx, a.opts.PollInterval) {
				return ctx.Err()
			}
		}
	}
}

// PassOnce runs one audit pass and returns. It is exposed so tests can
// drive the auditor deterministically without sleeping.
func (a *Auditor) PassOnce(ctx context.Context) (worked bool, err error) {
	// 1. Pull state.
	state, err := a.store.GetAuditState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, fmt.Errorf("loading audit state: %w", err)
	}
	ing, err := a.store.GetIngestionState(ctx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, fmt.Errorf("loading ingestion state: %w", err)
	}
	health, err := a.client.GetHealth(ctx)
	if err != nil {
		return false, fmt.Errorf("getHealth: %w", err)
	}
	a.metrics.RPCRequests++

	// 2. Lag pause: nothing to do unless ingest has moved at least
	// LagThreshold ledgers past the verified mark, and the range still
	// lies within the RPC's retention.
	if ing.LastIngestedLedger <= 0 {
		a.log.Debug("audit idle: nothing ingested yet")
		return false, nil
	}
	lag := uint32(ing.LastIngestedLedger - state.VerifiedThroughLedger)
	if lag < a.opts.LagThreshold {
		a.log.Debug("audit idle: ingest not far enough ahead", "lag", lag, "threshold", a.opts.LagThreshold)
		return false, nil
	}

	// 3. Pick a bounded window behind the ingest frontier.
	from := uint32(state.VerifiedThroughLedger) + 1
	to := uint32(ing.LastIngestedLedger)
	if from > to {
		return false, nil
	}
	if span := to - from + 1; span > a.opts.BatchLedgers {
		to = from + a.opts.BatchLedgers - 1
	}
	if health.OldestLedger > 0 {
		if from <= health.OldestLedger {
			// The entire audit window falls below the RPC's oldest
			// retained ledger — auditing it would always return no
			// events from the RPC, which would flag every stored row
			// as an orphan. Stay paused until ingest catches up.
			a.log.Debug("audit window below RPC retention; sleeping",
				"from", from, "oldest_retained", health.OldestLedger)
			return false, nil
		}
		if to > health.OldestLedger+a.opts.LagThreshold {
			// Stay safely inside the retention window so a half-finished
			// pass doesn't race the RPC dropping data.
			to = health.OldestLedger + a.opts.LagThreshold
			if to < from {
				a.log.Debug("audit window below RPC retention; sleeping",
					"from", from, "oldest_retained", health.OldestLedger)
				return false, nil
			}
		}
	}

	a.log.Info("audit pass starting",
		"from_ledger", from, "to_ledger", to,
		"ingested_through", ing.LastIngestedLedger,
		"verified_through", state.VerifiedThroughLedger,
	)
	return a.reconcileRange(ctx, from, to)
}

// reconcileRange walks every ledger in [from, to], compares the stored
// per-ledger counts and IDs against a fresh RPC fetch, advances the
// high-water mark past clean prefixes, records a finding for the first
// mismatch cluster, and attempts repair.
func (a *Auditor) reconcileRange(ctx context.Context, from, to uint32) (bool, error) {
	rpcEvents, err := a.fetchRange(ctx, from, to)
	if err != nil {
		return false, fmt.Errorf("fetching RPC events [%d,%d]: %w", from, to, err)
	}
	// Index by ledger. RPC IDs are zero-padded so lex order == chrono order.
	rpcByLedger := make(map[uint32][]string)
	for _, e := range rpcEvents {
		if e.Ledger < from || e.Ledger > to {
			continue
		}
		rpcByLedger[e.Ledger] = append(rpcByLedger[e.Ledger], e.ID)
	}

	census, err := a.store.LedgerRangeCensus(ctx, int64(from), int64(to), false)
	if err != nil {
		return false, err
	}
	storedByLedger := make(map[uint32]int, len(census))
	for _, c := range census {
		storedByLedger[uint32(c.Ledger)] = c.Count
	}

	// Walk ledgers in order; advance HWM over the clean prefix, then stop
	// at the first mismatch and try to repair it.
	a.metrics.LedgersChecked += uint64(to - from + 1)

	verifiedThrough := uint32(0)
	for l := from; ; l++ {
		clean, err := a.ledgerMatches(l, rpcByLedger, storedByLedger)
		if err != nil {
			return false, err
		}
		if clean {
			verifiedThrough = l
		} else {
			break
		}
		if l == to {
			break
		}
	}
	if verifiedThrough > 0 {
		if err := a.advanceHWM(ctx, verifiedThrough); err != nil {
			return false, err
		}
	}
	if verifiedThrough >= to {
		return true, nil // entire range is clean
	}

	// Walk from verifiedThrough+1 looking for the first dirty ledger; the
	// ledgers between (verifiedThrough+1, first-dirty-1) are clean gaps and
	// aren't part of any finding. If no dirty ledger is found inside the
	// batch, advance HWM through `to` and return — don't open a finding
	// for an all-clean cluster.
	mismatchFrom := uint32(0)
	for l := verifiedThrough + 1; l <= to; l++ {
		clean, _ := a.ledgerMatches(l, rpcByLedger, storedByLedger)
		if !clean {
			mismatchFrom = l
			break
		}
	}
	if mismatchFrom == 0 {
		if err := a.advanceHWM(ctx, to); err != nil {
			return false, err
		}
		return true, nil
	}
	mismatchTo := to
	if span := mismatchTo - mismatchFrom + 1; span > a.opts.FindingMaxLedgers {
		mismatchTo = mismatchFrom + a.opts.FindingMaxLedgers - 1
	}

	return true, a.handleMismatch(ctx, rpcByLedger, storedByLedger, mismatchFrom, mismatchTo, to)
}

// ledgerMatches reports whether a single ledger's stored count equals the
// RPC count. It does NOT consider the ledger clean if there are zero
// RPC events — both sides agree on emptiness — so HWM advances across
// empty ledgers too.
func (a *Auditor) ledgerMatches(l uint32, rpcByLedger map[uint32][]string, storedByLedger map[uint32]int) (bool, error) {
	stored := storedByLedger[l]
	rpcIDs := rpcByLedger[l]
	// Empty RPC side: ledger has no events in our filter set; stored
	// matching zero means clean. If stored > 0 we have orphans, which
	// is a real mismatch worth surfacing.
	if len(rpcIDs) == 0 {
		return stored == 0, nil
	}
	return stored == len(rpcIDs), nil
}

// handleMismatch is called when the audited range has at least one
// ledger whose RPC count disagrees with the store. It records/repairs a
// single bounded finding for the first mismatch cluster.
func (a *Auditor) handleMismatch(ctx context.Context, rpcByLedger map[uint32][]string, storedByLedger map[uint32]int, mismatchFrom, mismatchTo, rangeTo uint32) error {
	// Compute expected/actual and missing IDs for the cluster.
	expected := 0
	actual := 0
	missing := []string{}
	for l := mismatchFrom; l <= mismatchTo; l++ {
		rpcIDs := rpcByLedger[l]
		stored := storedByLedger[l]
		expected += len(rpcIDs)
		actual += stored
		if len(rpcIDs) > 0 && stored != len(rpcIDs) {
			// Missing IDs requires per-ID lookup: cheap because the
			// cluster is bounded by FindingMaxLedgers.
			ids, err := a.storedIDs(ctx, l)
			if err != nil {
				return err
			}
			storedSet := make(map[string]bool, len(ids))
			for _, id := range ids {
				storedSet[id] = true
			}
			for _, id := range rpcIDs {
				if !storedSet[id] {
					missing = append(missing, id)
				}
			}
		}
	}

	// Cluster too wide? Don't open one giant finding; refuse to advance
	// HWM past it and let a human investigate.
	if mismatchTo-mismatchFrom+1 == a.opts.FindingMaxLedgers &&
		rangeTo > mismatchTo {
		a.log.Warn("audit found wide-spread mismatch; bounding finding and holding HWM",
			"from", mismatchFrom, "to", mismatchTo, "remaining", rangeTo-mismatchTo)
	}

	// Skip opening a finding for a cluster where the RPC and the store
	// both report no events (all-empty ledgers inside the batch). Anything
	// else — including "store has events the RPC no longer reports"
	// (orphan) — is worth a finding so the operator can see what changed.
	if expected == 0 && actual == 0 {
		return nil
	}

	finding, err := a.store.RecordAuditFinding(ctx, store.AuditFinding{
		FromLedger:    int64(mismatchFrom),
		ToLedger:      int64(mismatchTo),
		ExpectedCount: expected,
		ActualCount:   actual,
		MissingIDs:    missing,
		Status:        store.FindingOpen,
	})
	if err != nil {
		return fmt.Errorf("recording finding [%d,%d]: %w", mismatchFrom, mismatchTo, err)
	}
	a.metrics.FindingsOpened++
	a.log.Warn("audit finding opened",
		"finding_id", finding.ID,
		"from", mismatchFrom, "to", mismatchTo,
		"expected", expected, "actual", actual,
		"missing_count", len(missing),
	)

	// Attempt repair in the same pass; the auditor keeps the finding
	// open across retries (next pass re-attempts) so crashes here don't
	// lose the diagnostic record.
	a.repairFinding(ctx, &finding)
	return nil
}

// storedIDs returns the lexicographically-sorted stored event IDs in
// a single ledger.
func (a *Auditor) storedIDs(ctx context.Context, ledger uint32) ([]string, error) {
	rows, err := a.store.LedgerRangeCensus(ctx, int64(ledger), int64(ledger), true)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0].IDs, nil
}

// repairFinding calls ReingestRange and re-records the finding status.
// It is wrapped in a transaction-style loop: each attempt either advances
// us toward "repaired" or bumps the attempt count; after MaxRepairAttempts
// the status flips to "unrecoverable" so it surfaces in /stats and stays
// visible until manually cleared.
func (a *Auditor) repairFinding(ctx context.Context, f *store.AuditFinding) {
	for {
		f.Attempts++
		f.LastAttemptedAt = time.Now()
		_, err := a.reingest.ReingestRange(ctx, a.client, uint32(f.FromLedger), uint32(f.ToLedger))
		if err == nil {
			// Re-verify the cluster: a fresh census tells us whether
			// repair actually fixed it.
			clean, verr := a.clusterIsClean(ctx, uint32(f.FromLedger), uint32(f.ToLedger))
			if verr != nil {
				f.LastError = verr.Error()
			}
			if verr != nil {
				// Transient: try again until the cap.
			} else if clean {
				f.Status = store.FindingRepaired
				f.LastError = ""
				a.metrics.FindingsRepaired++
				a.log.Info("audit finding repaired",
					"finding_id", f.ID, "from", f.FromLedger, "to", f.ToLedger,
					"attempts", f.Attempts)
				if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
					a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
				}
				// The cluster is clean — advance HWM past it so we don't
				// redo this verified prefix on the next pass.
				if uerr := a.advanceHWM(ctx, uint32(f.ToLedger)); uerr != nil {
					a.log.Error("advancing HWM after repair", "finding_id", f.ID, "error", uerr)
				}
				return
			} else {
				f.LastError = "RPC disagreeing with itself across fetches"
			}
		} else {
			if rpc.IsLedgerOutOfRange(err) {
				// RPC retention no longer covers this range: leave a
				// permanent marker.
				f.Status = store.FindingUnverifiable
				f.LastError = err.Error()
				a.metrics.FindingsUnverifiable++
				a.log.Warn("audit finding unverifiable (range aged out of RPC retention)",
					"finding_id", f.ID, "from", f.FromLedger, "to", f.ToLedger)
				if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
					a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
				}
				return
			}
			f.LastError = err.Error()
		}

		if f.Attempts >= a.opts.MaxRepairAttempts {
			f.Status = store.FindingUnrecoverable
			a.metrics.FindingsUnrecoverable++
			a.log.Error("audit finding unrecoverable after max attempts",
				"finding_id", f.ID, "from", f.FromLedger, "to", f.ToLedger,
				"attempts", f.Attempts, "last_error", f.LastError)
			if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
				a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
			}
			return
		}

		// Persist intermediate state and back off briefly before the next
		// attempt. A short constant keeps the loop bounded in tests without
		// making the production repair path aggressive.
		if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
			a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return
		}
	}
}

// clusterIsClean re-fetches RPC events and the store census for the
// closed range [from, to] and reports whether every ledger in the
// cluster now matches.
func (a *Auditor) clusterIsClean(ctx context.Context, from, to uint32) (bool, error) {
	rpcEvents, err := a.fetchRange(ctx, from, to)
	if err != nil {
		return false, err
	}
	rpcByLedger := make(map[uint32]int)
	for _, e := range rpcEvents {
		if e.Ledger >= from && e.Ledger <= to {
			rpcByLedger[e.Ledger]++
		}
	}
	census, err := a.store.LedgerRangeCensus(ctx, int64(from), int64(to), false)
	if err != nil {
		return false, err
	}
	for _, c := range census {
		if rpcByLedger[uint32(c.Ledger)] != c.Count {
			return false, nil
		}
	}
	for l := from; l <= to; l++ {
		if rpcByLedger[l] != 0 {
			// store.LedgerRangeCensus only reports non-empty ledgers, so
			// a missing ledger means the cluster has RPC events but
			// zero stored events for it → unclean.
			found := false
			for _, c := range census {
				if uint32(c.Ledger) == l {
					found = true
					break
				}
			}
			if !found {
				return false, nil
			}
		}
	}
	return true, nil
}

// fetchRange pages all events in [from, to] matching the ingester's
// current filter set, draining every page.
func (a *Auditor) fetchRange(ctx context.Context, from, to uint32) ([]rpc.Event, error) {
	batches, err := a.reingest.BuildFilterBatches(ctx)
	if err != nil {
		return nil, err
	}
	pageLimit := a.reingest.PageLimit()
	if pageLimit == 0 {
		pageLimit = 1000
	}
	endExcl := to + 1
	var out []rpc.Event
	for _, batch := range batches {
		cursor := ""
		for {
			a.metrics.RPCRequests++
			resp, err := a.client.GetEvents(ctx, rpc.GetEventsRequest{
				StartLedger: from,
				EndLedger:   endExcl,
				Filters:     batch,
				Pagination:  &rpc.Pagination{Cursor: cursor, Limit: pageLimit},
			})
			if rpc.IsLedgerOutOfRange(err) {
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			// Defensive post-filter: cursor pagination strips endLedger
			// from the request, so the tail of a full page may include
			// events past `to`.
			for _, e := range resp.Events {
				if e.Ledger <= to {
					out = append(out, e)
				}
			}
			if uint(len(resp.Events)) < pageLimit {
				break
			}
			last := resp.Events[len(resp.Events)-1]
			if last.Ledger > to {
				break
			}
			cursor = resp.Cursor
			if cursor == "" {
				cursor = last.CursorValue()
			}
		}
	}
	return out, nil
}

// advanceHWM persists the new audit HWM if it's strictly greater than
// what's already recorded; the conditional UPDATE on the singleton row
// makes the operation race-free even if two auditors ever run.
func (a *Auditor) advanceHWM(ctx context.Context, ledger uint32) error {
	_, err := a.store.SaveAuditStateIfGreater(ctx, int64(ledger))
	return err
}

// Compile-time check that the real ingester satisfies our interface.
var _ Reingester = (*ingester.Ingester)(nil)

// sleepCtx is a copy of ingester.sleepCtx kept here so this package has
// no cycle through the ingester. It's intentionally tiny.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
