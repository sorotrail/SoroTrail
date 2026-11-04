package ingester

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sorotrail/sorotrail/internal/decode"
	"github.com/sorotrail/sorotrail/internal/rpc"
	"github.com/sorotrail/sorotrail/internal/store"
)

var (
	// ErrReorgDepthExceeded is returned when a detected chain reorganisation
	// spans more ledgers than the configured ReorgConfirmationWindow allows.
	ErrReorgDepthExceeded = errors.New("reorg depth exceeds configured confirmation window")

	// ErrReorgTouchesArchivedRange is returned when a chain reorganisation
	// attempts to retract or rewrite ledgers at or below the cold-archive
	// immutability boundary.
	ErrReorgTouchesArchivedRange = errors.New("reorg touches archived immutable ledger range")
)

var (
	// ReorgsDetectedTotal counts detected chain reorganisations that triggered
	// range reconciliation.
	ReorgsDetectedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_reorgs_detected_total",
		Help: "Total number of chain reorganisations detected and reconciled by the ingester.",
	})

	// ReorgDepthLedgers observes the depth (in ledgers) of each detected reorg.
	ReorgDepthLedgers = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sorotrail_reorg_depth_ledgers",
		Help:    "Depth in ledgers of detected chain reorganisations.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
	})

	// ReorgWindowLimitLedgers exposes the configured ReorgConfirmationWindow so
	// alert rules can compare observed reorg depths against the window limit.
	ReorgWindowLimitLedgers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_reorg_window_limit_ledgers",
		Help: "Configured reorg confirmation window in ledgers (0 when disabled).",
	})

	// ReorgWindowUtilizationRatio tracks how close the most recent reorg came
	// to exhausting ReorgConfirmationWindow (depth / window, 0..1+).
	ReorgWindowUtilizationRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sorotrail_reorg_window_utilization_ratio",
		Help: "Ratio of the most recent reorg depth to the configured confirmation window.",
	})

	// ReorgWindowExceededTotal counts reorgs whose depth exceeded the
	// configured ReorgConfirmationWindow.
	ReorgWindowExceededTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_reorg_window_exceeded_total",
		Help: "Total number of chain reorganisations that exceeded the configured confirmation window.",
	})

	// ReorgArchivedBoundaryViolationsTotal counts reorgs rejected because their
	// ledger range intersected already-archived cold storage.
	ReorgArchivedBoundaryViolationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sorotrail_reorg_archived_boundary_violations_total",
		Help: "Total number of chain reorganisations that touched an archived immutable ledger range.",
	})

	// ReorgRetractedRowsTotal counts rows retracted or rewound during reorg
	// reconciliation, labelled by table (events, event_addresses, dead_letters,
	// contract_cursors).
	ReorgRetractedRowsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sorotrail_reorg_retracted_rows_total",
		Help: "Total number of derived and primary rows retracted or rewound by chain reorganisations, labelled by table.",
	}, []string{"table"})
)

func init() {
	registerCollector := func(c prometheus.Collector) {
		if err := prometheus.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(err, &are) {
				panic(err)
			}
		}
	}
	registerCollector(ReorgsDetectedTotal)
	registerCollector(ReorgDepthLedgers)
	registerCollector(ReorgWindowLimitLedgers)
	registerCollector(ReorgWindowUtilizationRatio)
	registerCollector(ReorgWindowExceededTotal)
	registerCollector(ReorgArchivedBoundaryViolationsTotal)
	registerCollector(ReorgRetractedRowsTotal)
}

// ReorgRetractionNotice is emitted to subscribers when a chain reorganisation
// retracts or replaces previously persisted events in [FromLedger, ToLedger].
type ReorgRetractionNotice struct {
	Network             string        `json:"network"`
	FromLedger          uint32        `json:"from_ledger"`
	ToLedger            uint32        `json:"to_ledger"`
	NewHeadLedger       uint32        `json:"new_head_ledger,omitempty"`
	RetractedEventIDs   []string      `json:"retracted_event_ids"`
	ReplacementEventIDs []string      `json:"replacement_event_ids,omitempty"`
	ReplacementEvents   []store.Event `json:"replacement_events,omitempty"`
	DetectedAt          time.Time     `json:"detected_at"`
}

// ReorgNotifier is an optional callback interface implemented by webhook
// dispatchers or streaming consumers that handle explicit event retractions
// when a chain reorganisation invalidates previously delivered events.
type ReorgNotifier interface {
	NotifyReorgRetraction(ctx context.Context, notice ReorgRetractionNotice)
}

// ArchiveBoundaryFunc returns the highest ledger sequence that has already
// been flushed to immutable cold archive storage (0 when no ledgers are archived).
type ArchiveBoundaryFunc func(ctx context.Context) (uint32, error)

// DefaultReorgConfirmationWindow is the default number of recent ledgers
// treated as unconfirmed for reorg tracking when not explicitly overridden.
const DefaultReorgConfirmationWindow uint32 = 64

// ReorgTracker maintains a sliding window of ledger hashes/fingerprints across
// the recent confirmation window [highestLedger-window+1, highestLedger] and
// evaluates reorg depth against the configured limit.
type ReorgTracker struct {
	mu            sync.Mutex
	window        uint32
	highestLedger uint32
	hashes        map[uint32]string
}

// NewReorgTracker creates a ReorgTracker bounded to window ledgers.
func NewReorgTracker(window uint32) *ReorgTracker {
	ReorgWindowLimitLedgers.Set(float64(window))
	return &ReorgTracker{
		window: window,
		hashes: make(map[uint32]string),
	}
}

// Window returns the configured confirmation window in ledgers.
func (rt *ReorgTracker) Window() uint32 {
	if rt == nil {
		return 0
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.window
}

// HighestLedger returns the highest ledger sequence recorded by the tracker.
func (rt *ReorgTracker) HighestLedger() uint32 {
	if rt == nil {
		return 0
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.highestLedger
}

// CheckTipRegression reports whether latestLedger has regressed below the
// highest ledger previously observed by the tracker. When regressed is true,
// [fromLedger, toLedger] is the inclusive rollback range [latestLedger+1, highestLedger].
func (rt *ReorgTracker) CheckTipRegression(latestLedger uint32) (regressed bool, fromLedger, toLedger uint32) {
	if rt == nil || latestLedger == 0 {
		return false, 0, 0
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.highestLedger > latestLedger {
		return true, latestLedger + 1, rt.highestLedger
	}
	return false, 0, 0
}

// RecordLedgerHash records the canonical hash (or event-set fingerprint) for
// ledger. If a different hash was already recorded for ledger within the
// retained window, it returns mismatch=true and the previous hash.
func (rt *ReorgTracker) RecordLedgerHash(ledger uint32, hash string) (mismatch bool, previousHash string) {
	if rt == nil || ledger == 0 || hash == "" {
		return false, ""
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if prev, ok := rt.hashes[ledger]; ok && prev != hash {
		mismatch = true
		previousHash = prev
	}
	rt.hashes[ledger] = hash
	if ledger > rt.highestLedger {
		rt.highestLedger = ledger
	}
	rt.pruneLocked()
	return mismatch, previousHash
}

// LookupLedgerHash returns the recorded hash for ledger if it is still within
// the retained confirmation window.
func (rt *ReorgTracker) LookupLedgerHash(ledger uint32) (string, bool) {
	if rt == nil {
		return "", false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	h, ok := rt.hashes[ledger]
	return h, ok
}

// ResetAbove removes any tracked ledger hashes above maxLedger (used when a
// rollback reorg shrinks the canonical chain head).
func (rt *ReorgTracker) ResetAbove(maxLedger uint32) {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for l := range rt.hashes {
		if l > maxLedger {
			delete(rt.hashes, l)
		}
	}
	if rt.highestLedger > maxLedger {
		rt.highestLedger = maxLedger
	}
}

// EvaluateDepth computes the depth of a reorg starting at fromLedger relative
// to [fromLedger, toLedger] and frontierLedger, updates Prometheus
// depth/utilization metrics, and reports whether the depth exceeds the
// configured confirmation window.
func (rt *ReorgTracker) EvaluateDepth(fromLedger, toLedger, frontierLedger uint32) (depth uint32, utilization float64, exceeded bool) {
	if toLedger >= fromLedger {
		depth = toLedger - fromLedger + 1
	} else {
		depth = 1
	}
	if frontierLedger > toLedger+1 && frontierLedger > fromLedger {
		if d := frontierLedger - fromLedger; d > depth {
			depth = d
		}
	}

	var window uint32
	if rt != nil {
		rt.mu.Lock()
		window = rt.window
		if rt.highestLedger > toLedger+1 && rt.highestLedger > fromLedger {
			if d := rt.highestLedger - fromLedger; d > depth {
				depth = d
			}
		}
		rt.mu.Unlock()
	}
	ReorgDepthLedgers.Observe(float64(depth))
	if window > 0 {
		utilization = float64(depth) / float64(window)
		ReorgWindowUtilizationRatio.Set(utilization)
		if depth > window {
			exceeded = true
		}
	}
	return depth, utilization, exceeded
}

func (rt *ReorgTracker) pruneLocked() {
	if rt.window == 0 || rt.highestLedger <= rt.window {
		return
	}
	minKeep := rt.highestLedger - rt.window
	for l := range rt.hashes {
		if l < minKeep {
			delete(rt.hashes, l)
		}
	}
}

// ComputeLedgerFingerprint returns a deterministic SHA-256 digest over the
// canonical fields of all events belonging to ledger.
func ComputeLedgerFingerprint(ledger uint32, events []store.Event) string {
	var matching []store.Event
	for _, ev := range events {
		if uint32(ev.Ledger) == ledger {
			matching = append(matching, ev)
		}
	}
	sort.Slice(matching, func(i, j int) bool {
		return matching[i].ID < matching[j].ID
	})
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "ledger:%d;count:%d;", ledger, len(matching))
	for _, ev := range matching {
		_, _ = fmt.Fprintf(h, "id:%s|c:%s|tx:%s|txi:%d|opi:%d|t:%s|ok:%t|top:%s|val:%s;",
			ev.ID, ev.ContractID, ev.TxHash, ev.TxIndex, ev.OpIndex,
			ev.Type, ev.InSuccessfulCall, string(ev.Topics), string(ev.Value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// reorgCoordinator serializes and coalesces overlapping reorg repairs when a
// new reorg arrives while an earlier reorg is still being reconciled.
type reorgCoordinator struct {
	mu          sync.Mutex
	running     bool
	pending     bool
	pendingFrom uint32
	pendingTo   uint32
	pendingHead uint32
	waiters     []chan reorgWaitResult
	lastResult  store.ReorgReconcileResult
}

type reorgWaitResult struct {
	res store.ReorgReconcileResult
	err error
}

// SetReorgNotifier attaches an explicit ReorgNotifier callback invoked whenever
// a reorganised ledger range retracts or modifies previously persisted events.
func (ing *Ingester) SetReorgNotifier(n ReorgNotifier) {
	ing.reorgNotifier = n
}

// SetArchivedThroughLedger sets a static cold-archive immutability boundary.
// Any reorg touching a ledger <= ledger is rejected with ErrReorgTouchesArchivedRange.
func (ing *Ingester) SetArchivedThroughLedger(ledger uint32) {
	ing.archivedThroughLedger.Store(ledger)
}

// SetArchiveBoundary wires a dynamic callback returning the highest archived
// ledger sequence.
func (ing *Ingester) SetArchiveBoundary(fn ArchiveBoundaryFunc) {
	ing.archiveBoundaryFn = fn
}

// ReorgTracker returns the ingester's ledger-hash and confirmation-window tracker.
func (ing *Ingester) ReorgTracker() *ReorgTracker {
	return ing.reorgTracker
}

func (ing *Ingester) resolveArchivedBoundary(ctx context.Context) (uint32, error) {
	bound := ing.archivedThroughLedger.Load()
	if ing.archiveBoundaryFn != nil {
		dyn, err := ing.archiveBoundaryFn(ctx)
		if err != nil {
			return 0, fmt.Errorf("checking archive boundary: %w", err)
		}
		if dyn > bound {
			bound = dyn
		}
	}
	return bound, nil
}

// HandleReorgRange validates a detected reorganisation over [fromLedger, toLedger]
// against the cold-archive boundary and ReorgConfirmationWindow, fetches the
// canonical replacement events across all watched filter batches, atomically
// reconciles events and all derived state (event_addresses, dead_letters,
// contract_cursors, ingestion_state, audit_state), and emits a
// ReorgRetractionNotice if any previously stored events were retracted.
//
// If another reorg is already being processed concurrently, HandleReorgRange
// coalesces the overlapping range and waits for the follow-up pass to finish
// so concurrent reorgs never race on store mutations.
func (ing *Ingester) HandleReorgRange(ctx context.Context, client rpc.Client, fromLedger, toLedger, newHeadLedger uint32) (store.ReorgReconcileResult, error) {
	if fromLedger == 0 || toLedger == 0 || fromLedger > toLedger {
		return store.ReorgReconcileResult{}, fmt.Errorf("HandleReorgRange: invalid range [%d,%d]", fromLedger, toLedger)
	}
	if client == nil {
		client = ing.client
	}

	// 1. Check cold-archive immutability boundary first.
	archivedThrough, err := ing.resolveArchivedBoundary(ctx)
	if err != nil {
		return store.ReorgReconcileResult{}, err
	}
	if archivedThrough > 0 && fromLedger <= archivedThrough {
		ReorgArchivedBoundaryViolationsTotal.Inc()
		ing.log.Error("reorg touches archived immutable ledger range",
			"from_ledger", fromLedger,
			"to_ledger", toLedger,
			"archived_through_ledger", archivedThrough,
		)
		return store.ReorgReconcileResult{}, fmt.Errorf(
			"%w: reorg range [%d,%d] intersects archived ledgers (archived through %d)",
			ErrReorgTouchesArchivedRange, fromLedger, toLedger, archivedThrough,
		)
	}

	// 2. Check reorg depth against ReorgConfirmationWindow.
	var frontier uint32
	if st, serr := ing.getIngestionState(ctx); serr == nil && st.LastIngestedLedger > 0 {
		frontier = uint32(st.LastIngestedLedger)
	}
	if toLedger > frontier {
		frontier = toLedger
	}
	depth, utilization, exceeded := ing.reorgTracker.EvaluateDepth(fromLedger, toLedger, frontier)
	if exceeded {
		ReorgWindowExceededTotal.Inc()
		window := ing.reorgTracker.Window()
		ing.log.Error("reorg depth exceeded confirmation window",
			"from_ledger", fromLedger,
			"to_ledger", toLedger,
			"frontier_ledger", frontier,
			"reorg_depth", depth,
			"confirmation_window", window,
			"window_utilization", utilization,
		)
		return store.ReorgReconcileResult{}, fmt.Errorf(
			"%w: reorg depth %d (range [%d,%d] at frontier %d) exceeds confirmation window %d",
			ErrReorgDepthExceeded, depth, fromLedger, toLedger, frontier, window,
		)
	}

	// 3. Coordinate with any in-flight reorg processing: if another goroutine
	//    is currently reconciling a reorg, coalesce our range into pending and
	//    wait for the active worker to execute the coalesced pass.
	ing.reorgCoord.mu.Lock()
	if ing.reorgCoord.running {
		if !ing.reorgCoord.pending {
			ing.reorgCoord.pending = true
			ing.reorgCoord.pendingFrom = fromLedger
			ing.reorgCoord.pendingTo = toLedger
			ing.reorgCoord.pendingHead = newHeadLedger
		} else {
			if fromLedger < ing.reorgCoord.pendingFrom {
				ing.reorgCoord.pendingFrom = fromLedger
			}
			if toLedger > ing.reorgCoord.pendingTo {
				ing.reorgCoord.pendingTo = toLedger
			}
			if newHeadLedger > 0 {
				ing.reorgCoord.pendingHead = newHeadLedger
			}
		}
		waitCh := make(chan reorgWaitResult, 1)
		ing.reorgCoord.waiters = append(ing.reorgCoord.waiters, waitCh)
		ing.reorgCoord.mu.Unlock()

		select {
		case w := <-waitCh:
			return w.res, w.err
		case <-ctx.Done():
			return store.ReorgReconcileResult{}, ctx.Err()
		}
	}
	ing.reorgCoord.running = true
	ing.reorgCoord.mu.Unlock()

	curFrom := fromLedger
	curTo := toLedger
	curHead := newHeadLedger

	var combined store.ReorgReconcileResult
	for {
		res, execErr := ing.executeReorgPass(ctx, client, curFrom, curTo, curHead)
		if execErr == nil {
			combined = mergeReorgResults(combined, res)
		}

		ing.reorgCoord.mu.Lock()
		if execErr != nil || !ing.reorgCoord.pending {
			waiters := ing.reorgCoord.waiters
			ing.reorgCoord.waiters = nil
			ing.reorgCoord.pending = false
			ing.reorgCoord.running = false
			ing.reorgCoord.lastResult = combined
			ing.reorgCoord.mu.Unlock()

			for _, ch := range waiters {
				ch <- reorgWaitResult{res: combined, err: execErr}
			}
			return combined, execErr
		}

		// Drain the coalesced pending reorg that arrived while we were running.
		if ing.reorgCoord.pendingFrom < curFrom {
			curFrom = ing.reorgCoord.pendingFrom
		} else {
			curFrom = ing.reorgCoord.pendingFrom
		}
		curTo = ing.reorgCoord.pendingTo
		if ing.reorgCoord.pendingHead > 0 {
			curHead = ing.reorgCoord.pendingHead
		}
		ing.reorgCoord.pending = false
		ing.reorgCoord.mu.Unlock()
	}
}

func mergeReorgResults(a, b store.ReorgReconcileResult) store.ReorgReconcileResult {
	if a.FromLedger == 0 {
		return b
	}
	out := b
	if a.FromLedger < out.FromLedger {
		out.FromLedger = a.FromLedger
	}
	if a.ToLedger > out.ToLedger {
		out.ToLedger = a.ToLedger
	}
	retractedSet := make(map[string]struct{}, len(a.RetractedEventIDs)+len(b.RetractedEventIDs))
	for _, id := range a.RetractedEventIDs {
		retractedSet[id] = struct{}{}
	}
	for _, id := range b.RetractedEventIDs {
		retractedSet[id] = struct{}{}
	}
	out.RetractedEventIDs = make([]string, 0, len(retractedSet))
	for id := range retractedSet {
		out.RetractedEventIDs = append(out.RetractedEventIDs, id)
	}
	sort.Strings(out.RetractedEventIDs)
	out.RetractedAddressRefs = a.RetractedAddressRefs + b.RetractedAddressRefs
	out.RetractedDeadLetters = a.RetractedDeadLetters + b.RetractedDeadLetters
	out.RewoundContractCursors = a.RewoundContractCursors + b.RewoundContractCursors
	return out
}

func (ing *Ingester) executeReorgPass(ctx context.Context, client rpc.Client, fromLedger, toLedger, newHeadLedger uint32) (store.ReorgReconcileResult, error) {
	filters, err := ing.BuildFilterBatches(ctx)
	if err != nil {
		return store.ReorgReconcileResult{}, err
	}

	var allStoreEvents []store.Event
	for _, batch := range filters {
		collected, _, berr := ing.collectBatchEventsInRange(ctx, client, fromLedger, toLedger, batch, false)
		if berr != nil {
			return store.ReorgReconcileResult{}, berr
		}
		allStoreEvents = append(allStoreEvents, collected...)
	}
	allStoreEvents = removeDuplicateEvents(allStoreEvents)

	// If newHeadLedger is lower than toLedger (a rollback that shortened the
	// chain), drop any events above newHeadLedger.
	if newHeadLedger > 0 && newHeadLedger < toLedger {
		filtered := allStoreEvents[:0]
		for _, ev := range allStoreEvents {
			if uint32(ev.Ledger) <= newHeadLedger {
				filtered = append(filtered, ev)
			}
		}
		allStoreEvents = filtered
	}

	if ing.opts.DryRun {
		ids := make([]string, len(allStoreEvents))
		for i, event := range allStoreEvents {
			ids[i] = event.ID
		}
		ing.log.Info("dry-run: would replace events",
			"from_ledger", fromLedger,
			"to_ledger", toLedger,
			"count", len(allStoreEvents),
			"event_ids", ids)
		return store.ReorgReconcileResult{
			FromLedger:       int64(fromLedger),
			ToLedger:         int64(toLedger),
			InsertedEventIDs: ids,
		}, nil
	}

	// Extract canonical address references from the replacement events.
	refs := make([]store.AddressRef, 0, len(allStoreEvents)*2)
	decoded := make([]decode.AddressRef, 0, 8)
	for _, ev := range allStoreEvents {
		decoded = decode.AppendAddresses(decoded[:0], ev.ID, ev.Topics, ev.Value)
		for _, r := range decoded {
			refs = append(refs, store.AddressRef{
				Address: r.Address,
				EventID: ev.ID,
				Role:    r.Role,
			})
		}
	}

	res, err := store.ReconcileReorgRange(ctx, ing.store, store.ReorgReconcileOptions{
		Network:              ing.opts.Network,
		FromLedger:           int64(fromLedger),
		ToLedger:             int64(toLedger),
		CanonicalEvents:      allStoreEvents,
		CanonicalAddressRefs: refs,
		NewHeadLedger:        int64(newHeadLedger),
	})
	if err != nil {
		return res, fmt.Errorf("ReplaceEventsInRange [%d,%d]: %w", fromLedger, toLedger, err)
	}

	// Record canonical ledger fingerprints in the confirmation window tracker.
	if ing.reorgTracker != nil {
		if newHeadLedger > 0 && newHeadLedger < toLedger {
			ing.reorgTracker.ResetAbove(newHeadLedger)
		}
		maxTrack := toLedger
		if newHeadLedger > 0 && newHeadLedger < maxTrack {
			maxTrack = newHeadLedger
		}
		byLedger := make(map[uint32][]store.Event)
		for _, ev := range allStoreEvents {
			if ev.Ledger > 0 {
				l := uint32(ev.Ledger)
				byLedger[l] = append(byLedger[l], ev)
			}
		}
		for l := fromLedger; l <= maxTrack; l++ {
			fp := ComputeLedgerFingerprint(l, byLedger[l])
			ing.reorgTracker.RecordLedgerHash(l, fp)
		}
	}

	// Update Prometheus metrics when actual retractions or cursor rewinds occurred.
	reorgOccurred := len(res.RetractedEventIDs) > 0 ||
		res.RetractedDeadLetters > 0 ||
		res.RewoundContractCursors > 0 ||
		res.RewoundIngestionLedger > 0 ||
		res.RewoundAuditLedger > 0 ||
		len(res.PreviousEventIDs) != len(res.InsertedEventIDs)
	if reorgOccurred {
		ReorgsDetectedTotal.Inc()
	}
	if len(res.RetractedEventIDs) > 0 {
		ReorgRetractedRowsTotal.WithLabelValues("events").Add(float64(len(res.RetractedEventIDs)))
	}
	if res.RetractedAddressRefs > 0 {
		ReorgRetractedRowsTotal.WithLabelValues("event_addresses").Add(float64(res.RetractedAddressRefs))
	}
	if res.RetractedDeadLetters > 0 {
		ReorgRetractedRowsTotal.WithLabelValues("dead_letters").Add(float64(res.RetractedDeadLetters))
	}
	if res.RewoundContractCursors > 0 {
		ReorgRetractedRowsTotal.WithLabelValues("contract_cursors").Add(float64(res.RewoundContractCursors))
	}

	// Emit explicit subscription retraction notice when previously stored
	// events were retracted or modified.
	if len(res.RetractedEventIDs) > 0 {
		notice := ReorgRetractionNotice{
			Network:             ing.opts.Network,
			FromLedger:          fromLedger,
			ToLedger:            toLedger,
			NewHeadLedger:       newHeadLedger,
			RetractedEventIDs:   res.RetractedEventIDs,
			ReplacementEventIDs: res.InsertedEventIDs,
			ReplacementEvents:   allStoreEvents,
			DetectedAt:          time.Now().UTC(),
		}
		if ing.reorgNotifier != nil {
			ing.reorgNotifier.NotifyReorgRetraction(ctx, notice)
		} else if rn, ok := ing.notifier.(ReorgNotifier); ok {
			rn.NotifyReorgRetraction(ctx, notice)
		}
	}

	return res, nil
}

func (ing *Ingester) collectBatchEventsInRange(ctx context.Context, client rpc.Client, fromLedger, toLedger uint32, batch []rpc.EventFilter, allowOutOfRange bool) ([]store.Event, bool, error) {
	endLedgerExcl := toLedger + 1
	var collected []rpc.Event
	cursor := ""
	for {
		resp, err := client.GetEvents(ctx, rpc.GetEventsRequest{
			StartLedger: fromLedger,
			EndLedger:   endLedgerExcl,
			Filters:     batch,
			Pagination:  &rpc.Pagination{Cursor: cursor, Limit: ing.opts.PageLimit},
		})
		if rpc.IsLedgerOutOfRange(err) {
			if allowOutOfRange {
				return nil, true, nil
			}
			return nil, true, fmt.Errorf("HandleReorgRange getEvents [%d,%d]: %w", fromLedger, toLedger, err)
		}
		if err != nil {
			return nil, false, fmt.Errorf("ReingestRange getEvents [%d,%d]: %w", fromLedger, toLedger, err)
		}
		for _, e := range resp.Events {
			if e.Ledger <= toLedger {
				collected = append(collected, e)
			}
		}
		if len(resp.Events) == 0 {
			break
		}
		last := resp.Events[len(resp.Events)-1]
		if uint(len(resp.Events)) < ing.opts.PageLimit && resp.Cursor == "" {
			break
		}
		if last.Ledger > toLedger {
			break
		}
		next := resp.Cursor
		if next == "" {
			next = last.CursorValue()
		}
		if next == cursor {
			break
		}
		cursor = next
	}

	storeEvents := make([]store.Event, 0, len(collected))
	for _, re := range collected {
		if ing.skipContracts[re.ContractID] {
			continue
		}
		ev, err := ing.toStoreEvent(re)
		if err != nil {
			return nil, false, err
		}
		ev.Network = ing.opts.Network
		storeEvents = append(storeEvents, ev)
	}
	return storeEvents, false, nil
}

// maybeHandleTipRegression checks whether the RPC's reported latestLedger has
// regressed below the ingester's observed or persisted frontier and, if so,
// reconciles the rolled-back range [latestLedger+1, toLedger] via HandleReorgRange.
func (ing *Ingester) maybeHandleTipRegression(ctx context.Context, latestLedger uint32) (bool, error) {
	if ing.opts.DryRun || ing.opts.ReorgConfirmationWindow == 0 || latestLedger == 0 {
		return false, nil
	}
	regressed, fromLedger, toLedger := ing.reorgTracker.CheckTipRegression(latestLedger)
	if st, err := ing.getIngestionState(ctx); err == nil && st.LastIngestedLedger > int64(latestLedger) {
		persistedHead := uint32(st.LastIngestedLedger)
		if !regressed {
			regressed = true
			fromLedger = latestLedger + 1
			toLedger = persistedHead
		} else if persistedHead > toLedger {
			toLedger = persistedHead
		}
	}
	if !regressed {
		return false, nil
	}
	ing.log.Warn("detected chain tip regression; reconciling reorg range",
		"from_ledger", fromLedger,
		"to_ledger", toLedger,
		"new_head_ledger", latestLedger,
	)
	if _, err := ing.HandleReorgRange(ctx, ing.client, fromLedger, toLedger, latestLedger); err != nil {
		return true, err
	}
	return true, nil
}

// recordIngestedLedgerFingerprints updates the confirmation-window tracker as
// normal ingestion persists events. When checkDivergence is true and a ledger
// within the confirmation window produces a fingerprint different from the one
// previously recorded for that ledger, it triggers HandleReorgRange over the
// diverged range.
func (ing *Ingester) recordIngestedLedgerFingerprints(ctx context.Context, events []store.Event, latestLedger uint32, checkDivergence bool) error {
	if ing.reorgTracker == nil || len(events) == 0 {
		return nil
	}
	byLedger := make(map[uint32][]store.Event)
	seenLedgers := make([]uint32, 0, 8)
	for _, ev := range events {
		if ev.Ledger > 0 {
			l := uint32(ev.Ledger)
			if _, ok := byLedger[l]; !ok {
				seenLedgers = append(seenLedgers, l)
			}
			byLedger[l] = append(byLedger[l], ev)
		}
	}
	sort.Slice(seenLedgers, func(i, j int) bool { return seenLedgers[i] < seenLedgers[j] })

	var divergedFrom, divergedTo uint32
	for _, l := range seenLedgers {
		fp := ComputeLedgerFingerprint(l, byLedger[l])
		mismatch, _ := ing.reorgTracker.RecordLedgerHash(l, fp)
		if mismatch {
			if divergedFrom == 0 || l < divergedFrom {
				divergedFrom = l
			}
			if l > divergedTo {
				divergedTo = l
			}
		}
	}
	if checkDivergence && divergedFrom > 0 && ing.opts.ReorgConfirmationWindow > 0 && !ing.opts.DryRun {
		to := divergedTo
		if highest := ing.reorgTracker.HighestLedger(); highest > to {
			to = highest
		}
		ing.log.Warn("detected ledger fingerprint divergence; reconciling reorg range",
			"from_ledger", divergedFrom,
			"to_ledger", to,
			"latest_ledger", latestLedger,
		)
		if _, err := ing.HandleReorgRange(ctx, ing.client, divergedFrom, to, latestLedger); err != nil {
			return err
		}
	}
	return nil
}

// ensure atomic.Uint32 is imported for archivedThroughLedger
var _ = atomic.Uint32{}
