// Package audit runs a background auditor that verifies stored events
// against fresh getEvents fetches.
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
type Reingester interface {
	BuildFilterBatches(ctx context.Context) ([][]rpc.EventFilter, error)
	ReingestRange(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32) (int, error)
	PageLimit() uint
	Network() string
}

// Options configure an Auditor.
type Options struct {
	PollInterval      time.Duration
	BatchLedgers      uint32
	LagThreshold      uint32
	MaxRepairAttempts int
	FindingMaxLedgers uint32
	// Network is the logical network name the auditor is responsible for.
	Network string
}

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
type Auditor struct {
	client   rpc.Client
	store    store.Store
	reingest Reingester
	log      *slog.Logger
	opts     Options
	metrics  Metrics
}

// Metrics is a snapshot of counters the auditor accumulates.
type Metrics struct {
	PassesRun             uint64
	LedgersChecked        uint64
	FindingsOpened        uint64
	FindingsRepaired      uint64
	FindingsUnrecoverable uint64
	FindingsUnverifiable  uint64
	RPCRequests           uint64
}

func (a *Auditor) Metrics() Metrics { return a.metrics }

// New wires an Auditor.
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

// Network returns the network this auditor is responsible for.
func (a *Auditor) Network() string { return a.opts.Network }

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
			a.log.Error("audit pass failed", "network", a.opts.Network, "error", err, "retry_in", sleep)
			if !sleepCtx(ctx, sleep) {
				return ctx.Err()
			}
			if backoff *= 2; backoff > a.opts.PollInterval {
				backoff = a.opts.PollInterval
			}
		default:
			a.metrics.PassesRun++
			backoff = time.Second
			_ = worked
			if !sleepCtx(ctx, a.opts.PollInterval) {
				return ctx.Err()
			}
		}
	}
}

// PassOnce runs one audit pass and returns.
func (a *Auditor) PassOnce(ctx context.Context) (worked bool, err error) {
	// 1. Pull state.
	state, err := a.store.GetAuditState(ctx, a.opts.Network)
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

	if ing.LastIngestedLedger <= 0 {
		a.log.Debug("audit idle: nothing ingested yet", "network", a.opts.Network)
		return false, nil
	}
	lag := uint32(ing.LastIngestedLedger - state.VerifiedThroughLedger)
	if lag < a.opts.LagThreshold {
		a.log.Debug("audit idle: ingest not far enough ahead",
			"network", a.opts.Network, "lag", lag, "threshold", a.opts.LagThreshold)
		return false, nil
	}

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
			a.log.Debug("audit window below RPC retention; sleeping",
				"network", a.opts.Network, "from", from, "oldest_retained", health.OldestLedger)
			return false, nil
		}
		if to > health.OldestLedger+a.opts.LagThreshold {
			to = health.OldestLedger + a.opts.LagThreshold
			if to < from {
				a.log.Debug("audit window below RPC retention; sleeping",
					"network", a.opts.Network, "from", from, "oldest_retained", health.OldestLedger)
				return false, nil
			}
		}
	}

	a.log.Info("audit pass starting",
		"network", a.opts.Network,
		"from_ledger", from, "to_ledger", to,
		"ingested_through", ing.LastIngestedLedger,
		"verified_through", state.VerifiedThroughLedger,
	)
	return a.reconcileRange(ctx, a.opts.Network, from, to)
}

// reconcileRange walks every ledger in [from, to], compares the stored
// per-ledger counts and IDs against a fresh RPC fetch, advances the
// high-water mark past clean prefixes, records a finding for the first
// mismatch cluster, and attempts repair.
func (a *Auditor) reconcileRange(ctx context.Context, network string, from, to uint32) (bool, error) {
	rpcEvents, err := a.fetchRange(ctx, from, to)
	if err != nil {
		return false, fmt.Errorf("fetching RPC events [%d,%d]: %w", from, to, err)
	}
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
		if err := a.advanceHWM(ctx, network, verifiedThrough); err != nil {
			return false, err
		}
	}
	if verifiedThrough >= to {
		return true, nil
	}

	mismatchFrom := uint32(0)
	for l := verifiedThrough + 1; l <= to; l++ {
		clean, _ := a.ledgerMatches(l, rpcByLedger, storedByLedger)
		if !clean {
			mismatchFrom = l
			break
		}
	}
	if mismatchFrom == 0 {
		if err := a.advanceHWM(ctx, network, to); err != nil {
			return false, err
		}
		return true, nil
	}
	mismatchTo := to
	if span := mismatchTo - mismatchFrom + 1; span > a.opts.FindingMaxLedgers {
		mismatchTo = mismatchFrom + a.opts.FindingMaxLedgers - 1
	}

	return true, a.handleMismatch(ctx, network, rpcByLedger, storedByLedger, mismatchFrom, mismatchTo, to)
}

func (a *Auditor) ledgerMatches(l uint32, rpcByLedger map[uint32][]string, storedByLedger map[uint32]int) (bool, error) {
	stored := storedByLedger[l]
	rpcIDs := rpcByLedger[l]
	if len(rpcIDs) == 0 {
		return stored == 0, nil
	}
	return stored == len(rpcIDs), nil
}

func (a *Auditor) handleMismatch(ctx context.Context, network string, rpcByLedger map[uint32][]string, storedByLedger map[uint32]int, mismatchFrom, mismatchTo, rangeTo uint32) error {
	expected := 0
	actual := 0
	missing := []string{}
	for l := mismatchFrom; l <= mismatchTo; l++ {
		rpcIDs := rpcByLedger[l]
		stored := storedByLedger[l]
		expected += len(rpcIDs)
		actual += stored
		if len(rpcIDs) > 0 && stored != len(rpcIDs) {
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

	if mismatchTo-mismatchFrom+1 == a.opts.FindingMaxLedgers && rangeTo > mismatchTo {
		a.log.Warn("audit found wide-spread mismatch; bounding finding and holding HWM",
			"network", a.opts.Network, "from", mismatchFrom, "to", mismatchTo, "remaining", rangeTo-mismatchTo)
	}

	if expected == 0 && actual == 0 {
		return nil
	}

	finding, err := a.store.RecordAuditFinding(ctx, store.AuditFinding{
		Network:       a.opts.Network,
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
		"network", a.opts.Network,
		"finding_id", finding.ID,
		"from", mismatchFrom, "to", mismatchTo,
		"expected", expected, "actual", actual,
		"missing_count", len(missing),
	)

	a.repairFinding(ctx, network, &finding)
	return nil
}

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

func (a *Auditor) repairFinding(ctx context.Context, network string, f *store.AuditFinding) {
	for {
		f.Attempts++
		f.LastAttemptedAt = time.Now()
		_, err := a.reingest.ReingestRange(ctx, a.client, uint32(f.FromLedger), uint32(f.ToLedger))
		if err == nil {
			clean, verr := a.clusterIsClean(ctx, uint32(f.FromLedger), uint32(f.ToLedger))
			if verr != nil {
				f.LastError = verr.Error()
			} else if clean {
				f.Status = store.FindingRepaired
				f.LastError = ""
				a.metrics.FindingsRepaired++
				a.log.Info("audit finding repaired",
					"network", a.opts.Network,
					"finding_id", f.ID, "from", f.FromLedger, "to", f.ToLedger,
					"attempts", f.Attempts)
				if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
					a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
				}
				if uerr := a.advanceHWM(ctx, network, uint32(f.ToLedger)); uerr != nil {
					a.log.Error("advancing HWM after repair", "finding_id", f.ID, "error", uerr)
				}
				return
			} else {
				f.LastError = "RPC disagreeing with itself across fetches"
			}
		} else {
			if rpc.IsLedgerOutOfRange(err) {
				f.Status = store.FindingUnverifiable
				f.LastError = err.Error()
				a.metrics.FindingsUnverifiable++
				a.log.Warn("audit finding unverifiable (range aged out of RPC retention)",
					"network", a.opts.Network,
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
				"network", a.opts.Network,
				"finding_id", f.ID, "from", f.FromLedger, "to", f.ToLedger,
				"attempts", f.Attempts, "last_error", f.LastError)
			if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
				a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
			}
			return
		}

		if uerr := a.store.UpdateAuditFinding(ctx, *f); uerr != nil {
			a.log.Error("updating finding", "finding_id", f.ID, "error", uerr)
		}
		if !sleepCtx(ctx, 100*time.Millisecond) {
			return
		}
	}
}

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
func (a *Auditor) advanceHWM(ctx context.Context, network string, ledger uint32) error {
	_, err := a.store.SaveAuditStateIfGreater(ctx, a.opts.Network, int64(ledger))
	return err
}

var _ Reingester = (*ingester.Ingester)(nil)

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
