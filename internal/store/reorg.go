package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ReorgMaintainer is an optional extension interface implemented by Store
// backends (Postgres, SQLite, and simulation stores) to support atomic
// retraction of events and derived state across a reorganised ledger range
// even when the canonical replacement event slice is empty.
type ReorgMaintainer interface {
	// DeleteEventsInRange deletes all events in [fromLedger, toLedger] for
	// network and returns the sorted IDs of the deleted rows.
	DeleteEventsInRange(ctx context.Context, network string, fromLedger, toLedger int64) ([]string, error)
	// DeleteAddressRefsByEventIDs deletes all derived event_addresses rows
	// referencing any of the supplied event IDs and returns the number of
	// deleted rows.
	DeleteAddressRefsByEventIDs(ctx context.Context, eventIDs []string) (int64, error)
	// DeleteDeadLettersInRange deletes all unresolved dead_letters rows in
	// [fromLedger, toLedger] and returns the number of deleted rows.
	DeleteDeadLettersInRange(ctx context.Context, fromLedger, toLedger int64) (int64, error)
}

// TransactionalReorgMaintainer is implemented by SQL-backed Store backends
// (*Postgres, *SQLite) so that the existing-events snapshot and all primary
// and derived-state mutations during ReconcileReorgRange (event_addresses,
// events, dead_letters, contract_cursors, ingestion_state, and audit_state)
// execute atomically inside a single database transaction.
type TransactionalReorgMaintainer interface {
	ReconcileReorgTx(
		ctx context.Context,
		opts ReorgReconcileOptions,
		canonical []Event,
		canonicalByID map[string]Event,
		dedupRefs []AddressRef,
		insertedIDs []string,
	) (ReorgReconcileResult, error)
}

// ReorgReconcileOptions configures how ReconcileReorgRange retracts and
// rebuilds stored state across a reorganised ledger window [FromLedger, ToLedger].
type ReorgReconcileOptions struct {
	// Network identifies the Stellar network being reconciled ("default" when empty).
	Network string
	// FromLedger is the inclusive lower bound of the reorganised ledger range.
	FromLedger int64
	// ToLedger is the inclusive upper bound of the reorganised ledger range.
	ToLedger int64
	// CanonicalEvents is the complete set of canonical events for [FromLedger, ToLedger].
	// May be empty when the canonical fork has zero events in this range.
	CanonicalEvents []Event
	// CanonicalAddressRefs is the derived address-index set extracted from CanonicalEvents.
	CanonicalAddressRefs []AddressRef
	// NewHeadLedger, when > 0, is the canonical chain head after the reorg.
	// If the persisted ingestion frontier or per-contract cursors sit ahead
	// of NewHeadLedger (e.g., a rollback reorg), they are clamped down.
	NewHeadLedger int64
	// RewindCursorsToFromLedger, when true, rewinds any per-contract cursor
	// or ingestion frontier at or above FromLedger back to FromLedger-1 so
	// an in-progress backfill or live loop re-scans the reorganised range.
	RewindCursorsToFromLedger bool
}

// ReorgReconcileResult summarizes the state mutations performed by ReconcileReorgRange.
type ReorgReconcileResult struct {
	FromLedger             int64
	ToLedger               int64
	PreviousEventIDs       []string
	RetractedEventIDs      []string
	InsertedEventIDs       []string
	RetractedAddressRefs   int64
	InsertedAddressRefs    int64
	RetractedDeadLetters   int64
	RewoundContractCursors int
	RewoundIngestionLedger int64
	RewoundAuditLedger     int64
}

// ReconcileReorgRange atomically reconciles stored events and all derived state
// (event_addresses, dead_letters, contract_cursors, ingestion_state, and
// audit_state) across the inclusive ledger range [opts.FromLedger, opts.ToLedger].
//
// Unlike ReplaceEventsInRange alone, ReconcileReorgRange:
//  1. Deletes events in [FromLedger, ToLedger] even when CanonicalEvents is empty.
//  2. Retracts orphaned event_addresses for all removed or rewritten events and
//     upserts CanonicalAddressRefs for the canonical replacement events.
//  3. Retracts dead_letters recorded against orphaned ledgers in [FromLedger, ToLedger].
//  4. Clears stale RPC pagination cursors and clamps per-contract cursors and
//     ingestion_state when the chain head regresses or a backfill is rewound.
//  5. Clamps audit_state.verified_through_ledger back to FromLedger-1 whenever
//     it had already advanced into or past the reorganised window.
func ReconcileReorgRange(ctx context.Context, st Store, opts ReorgReconcileOptions) (ReorgReconcileResult, error) {
	if opts.FromLedger <= 0 || opts.ToLedger <= 0 || opts.FromLedger > opts.ToLedger {
		return ReorgReconcileResult{}, fmt.Errorf("invalid reorg ledger range [%d,%d]", opts.FromLedger, opts.ToLedger)
	}
	network := defaultNetwork(opts.Network)
	res := ReorgReconcileResult{
		FromLedger: opts.FromLedger,
		ToLedger:   opts.ToLedger,
	}

	// Deduplicate canonical events by ID (idempotency under re-applied ledgers)
	// and normalize their network.
	canonical := make([]Event, 0, len(opts.CanonicalEvents))
	canonicalByID := make(map[string]Event, len(opts.CanonicalEvents))
	insertedIDs := make([]string, 0, len(opts.CanonicalEvents))
	for _, ev := range opts.CanonicalEvents {
		if ev.Ledger < opts.FromLedger || ev.Ledger > opts.ToLedger {
			continue
		}
		if ev.Network == "" && opts.Network != "" {
			ev.Network = opts.Network
		}
		if _, dup := canonicalByID[ev.ID]; dup {
			continue
		}
		canonicalByID[ev.ID] = ev
		canonical = append(canonical, ev)
		insertedIDs = append(insertedIDs, ev.ID)
	}
	sort.Strings(insertedIDs)
	res.InsertedEventIDs = insertedIDs

	var dedupRefs []AddressRef
	if len(opts.CanonicalAddressRefs) > 0 {
		dedupRefs = deduplicateAddressRefs(opts.CanonicalAddressRefs, canonicalByID)
	}

	// Fast atomic path for SQL-backed stores (*Postgres, *SQLite): take the
	// existing-events snapshot and execute all primary and derived-state
	// mutations inside a single database transaction.
	if txMaint, ok := st.(TransactionalReorgMaintainer); ok {
		return txMaint.ReconcileReorgTx(ctx, opts, canonical, canonicalByID, dedupRefs, insertedIDs)
	}

	// Fallback path for non-SQL in-memory/mock stores.
	existingEvents, err := queryAllEventsInRange(ctx, st, opts.Network, opts.FromLedger, opts.ToLedger)
	if err != nil {
		return res, fmt.Errorf("snapshot events in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, err)
	}
	previousIDs, retractedIDs, canonical := reconcileSnapshotWithCanonical(existingEvents, canonical, canonicalByID)
	res.PreviousEventIDs = previousIDs
	res.RetractedEventIDs = retractedIDs

	// 1. Retract derived address refs for all previous events in the range
	//    before replacing events, so modified or removed events never leave
	//    stale address mappings behind.
	if maint, ok := st.(ReorgMaintainer); ok {
		if len(previousIDs) > 0 {
			deletedRefs, derr := maint.DeleteAddressRefsByEventIDs(ctx, previousIDs)
			if derr != nil {
				return res, fmt.Errorf("retracting address refs in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
			}
			res.RetractedAddressRefs = deletedRefs
		}
		if _, derr := maint.DeleteEventsInRange(ctx, network, opts.FromLedger, opts.ToLedger); derr != nil {
			return res, fmt.Errorf("retracting events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
		}
		if len(canonical) > 0 {
			if _, uerr := st.UpsertEvents(ctx, canonical); uerr != nil {
				return res, fmt.Errorf("upserting canonical events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, uerr)
			}
		}
		deletedDL, dlErr := maint.DeleteDeadLettersInRange(ctx, opts.FromLedger, opts.ToLedger)
		if dlErr != nil {
			return res, fmt.Errorf("retracting dead letters in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, dlErr)
		}
		res.RetractedDeadLetters = deletedDL
	} else {
		if err := st.ReplaceEventsInRange(ctx, canonical, opts.FromLedger, opts.ToLedger); err != nil {
			return res, fmt.Errorf("replacing events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, err)
		}
		deletedDL, dlErr := retractDeadLettersViaStore(ctx, st, opts.FromLedger, opts.ToLedger)
		if dlErr != nil {
			return res, fmt.Errorf("retracting dead letters in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, dlErr)
		}
		res.RetractedDeadLetters = deletedDL
	}

	// 2. Re-index canonical address references (idempotent ON CONFLICT DO NOTHING).
	if len(dedupRefs) > 0 {
		if err := st.UpsertAddressRefs(ctx, dedupRefs); err != nil && !errors.Is(err, ErrUnsupported) {
			return res, fmt.Errorf("upserting canonical address refs: %w", err)
		}
		res.InsertedAddressRefs = int64(len(dedupRefs))
	}

	// 3. Reconcile per-contract cursors.
	cursors, err := st.ListContractCursors(ctx)
	if err != nil && !errors.Is(err, ErrUnsupported) && !errors.Is(err, ErrNotFound) {
		return res, fmt.Errorf("listing contract cursors for reorg reconciliation: %w", err)
	}
	for _, cc := range cursors {
		if cc.LastIngestedLedger < opts.FromLedger {
			continue
		}
		targetLedger := cc.LastIngestedLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		if targetLedger != cc.LastIngestedLedger || cc.LastCursor != "" {
			if serr := st.SaveContractCursor(ctx, ContractCursor{
				ContractID:         cc.ContractID,
				LastIngestedLedger: targetLedger,
				LastCursor:         "",
			}); serr != nil {
				return res, fmt.Errorf("rewinding contract cursor %s: %w", cc.ContractID, serr)
			}
			res.RewoundContractCursors++
		}
	}

	// 4. Reconcile global ingestion_state when the frontier sits inside or
	//    ahead of the reorg range.
	ingState, err := getIngestionStateForNetwork(ctx, st, network)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return res, fmt.Errorf("loading ingestion state for reorg reconciliation: %w", err)
	}
	if err == nil && ingState.LastIngestedLedger >= opts.FromLedger {
		targetLedger := ingState.LastIngestedLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		clearCursor := ingState.LastCursor != "" && (ingState.LastIngestedLedger <= opts.ToLedger || targetLedger != ingState.LastIngestedLedger)
		if targetLedger != ingState.LastIngestedLedger || clearCursor {
			ingState.Network = network
			ingState.LastIngestedLedger = targetLedger
			if clearCursor {
				ingState.LastCursor = ""
			}
			if serr := st.SaveIngestionState(ctx, ingState); serr != nil {
				return res, fmt.Errorf("rewinding ingestion state: %w", serr)
			}
			res.RewoundIngestionLedger = targetLedger
		}
	}

	// 5. Reconcile audit_state: if verified_through_ledger has already passed
	//    into [FromLedger, ...], clamp it back to FromLedger-1 so the auditor
	//    re-verifies the reorganised ledgers.
	auditState, err := st.GetAuditState(ctx, network)
	if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrUnsupported) {
		return res, fmt.Errorf("loading audit state for reorg reconciliation: %w", err)
	}
	if err == nil && auditState.VerifiedThroughLedger >= opts.FromLedger {
		clamped := opts.FromLedger - 1
		if clamped < 0 {
			clamped = 0
		}
		if serr := st.SaveAuditState(ctx, AuditState{
			Network:               network,
			VerifiedThroughLedger: clamped,
		}); serr != nil {
			return res, fmt.Errorf("clamping audit state to %d: %w", clamped, serr)
		}
		res.RewoundAuditLedger = clamped
	}

	return res, nil
}

func reconcileSnapshotWithCanonical(existingEvents []Event, canonical []Event, canonicalByID map[string]Event) ([]string, []string, []Event) {
	existingByID := make(map[string]Event, len(existingEvents))
	previousIDs := make([]string, 0, len(existingEvents))
	keptXDR := make(map[string]rawXDR, len(existingEvents))
	for _, ev := range existingEvents {
		existingByID[ev.ID] = ev
		previousIDs = append(previousIDs, ev.ID)
		if len(ev.RawTopicXDR) > 0 || ev.RawValueXDR != "" {
			keptXDR[ev.ID] = rawXDR{
				topics: ev.RawTopicXDR,
				value:  ev.RawValueXDR,
			}
		}
	}
	sort.Strings(previousIDs)

	var retractedIDs []string
	for id, oldEv := range existingByID {
		newEv, ok := canonicalByID[id]
		if !ok || !sameCanonicalEventContent(oldEv, newEv) {
			retractedIDs = append(retractedIDs, id)
		}
	}
	sort.Strings(retractedIDs)

	merged := make([]Event, len(canonical))
	copy(merged, canonical)
	for i := range merged {
		if oldXDR, ok := keptXDR[merged[i].ID]; ok {
			if oldEv, exists := existingByID[merged[i].ID]; exists && sameCanonicalEventContent(oldEv, merged[i]) {
				if len(merged[i].RawTopicXDR) == 0 && len(oldXDR.topics) > 0 {
					merged[i].RawTopicXDR = oldXDR.topics
				}
				if merged[i].RawValueXDR == "" && oldXDR.value != "" {
					merged[i].RawValueXDR = oldXDR.value
				}
			}
		}
	}
	return previousIDs, retractedIDs, merged
}

func sameCanonicalEventContent(a, b Event) bool {
	return a.ID == b.ID &&
		a.ContractID == b.ContractID &&
		a.Ledger == b.Ledger &&
		a.Type == b.Type &&
		a.TxHash == b.TxHash &&
		a.TxIndex == b.TxIndex &&
		a.OpIndex == b.OpIndex &&
		a.InSuccessfulCall == b.InSuccessfulCall &&
		canonicalJSONEqual(a.Topics, b.Topics) &&
		canonicalJSONEqual(a.Value, b.Value)
}

func canonicalJSONEqual(a, b json.RawMessage) bool {
	if bytes.Equal(a, b) {
		return true
	}
	var ca, cb bytes.Buffer
	errA := json.Compact(&ca, a)
	errB := json.Compact(&cb, b)
	if errA == nil && errB == nil {
		return bytes.Equal(ca.Bytes(), cb.Bytes())
	}
	return bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b))
}

func deduplicateAddressRefs(refs []AddressRef, allowedEvents map[string]Event) []AddressRef {
	type key struct {
		addr    string
		eventID string
		role    string
	}
	seen := make(map[key]struct{}, len(refs))
	out := make([]AddressRef, 0, len(refs))
	for _, r := range refs {
		if _, ok := allowedEvents[r.EventID]; !ok {
			continue
		}
		k := key{addr: r.Address, eventID: r.EventID, role: r.Role}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, r)
	}
	return out
}

func queryAllEventsInRange(ctx context.Context, st Store, network string, fromLedger, toLedger int64) ([]Event, error) {
	var all []Event
	cursor := ""
	for {
		page, next, err := st.QueryEvents(ctx, EventFilter{
			Network:    network,
			FromLedger: fromLedger,
			ToLedger:   toLedger,
			Cursor:     cursor,
			Limit:      MaxQueryLimit,
			Order:      "asc",
			Scope:      WildcardScope(),
		})
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if next == "" || len(page) == 0 {
			break
		}
		cursor = next
	}
	return all, nil
}

func retractDeadLettersViaStore(ctx context.Context, st Store, fromLedger, toLedger int64) (int64, error) {
	var deleted int64
	cursor := ""
	for {
		items, next, err := st.ListDeadLetters(ctx, "", MaxQueryLimit, cursor)
		if err != nil {
			if errors.Is(err, ErrUnsupported) {
				return 0, nil
			}
			return deleted, err
		}
		for _, dl := range items {
			if dl.Ledger >= fromLedger && dl.Ledger <= toLedger {
				if derr := st.DeleteDeadLetter(ctx, dl.ID); derr != nil && !errors.Is(derr, ErrNotFound) {
					return deleted, derr
				}
				deleted++
			}
		}
		if next == "" || len(items) == 0 {
			break
		}
		cursor = next
	}
	return deleted, nil
}

func getIngestionStateForNetwork(ctx context.Context, st Store, network string) (IngestionState, error) {
	type networkStateReader interface {
		GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error)
	}
	if r, ok := st.(networkStateReader); ok {
		return r.GetIngestionStateForNetwork(ctx, network)
	}
	return st.GetIngestionState(ctx)
}

// --- SQLite ReorgMaintainer implementation ---

var (
	_ ReorgMaintainer              = (*SQLite)(nil)
	_ TransactionalReorgMaintainer = (*SQLite)(nil)
)

func (s *SQLite) ReconcileReorgTx(
	ctx context.Context,
	opts ReorgReconcileOptions,
	canonical []Event,
	canonicalByID map[string]Event,
	dedupRefs []AddressRef,
	insertedIDs []string,
) (ReorgReconcileResult, error) {
	network := defaultNetwork(opts.Network)
	res := ReorgReconcileResult{
		FromLedger:       opts.FromLedger,
		ToLedger:         opts.ToLedger,
		InsertedEventIDs: insertedIDs,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("begin reorg tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 0. Snapshot existing events in [FromLedger, ToLedger] for network inside
	//    the transaction so no concurrent write can interleave between reading
	//    the snapshot and replacing the rows.
	snapRows, qerr := tx.QueryContext(ctx,
		`SELECT `+eventColumnsSQLite+` FROM events WHERE (network = ? OR (? = 'default' AND network = '')) AND ledger >= ? AND ledger <= ? ORDER BY id ASC`,
		network, network, opts.FromLedger, opts.ToLedger)
	if qerr != nil {
		return res, fmt.Errorf("snapshot events in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, qerr)
	}
	var existingEvents []Event
	for snapRows.Next() {
		ev, serr := scanEventSQLite(snapRows)
		if serr != nil {
			_ = snapRows.Close()
			return res, fmt.Errorf("scanning snapshot event in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, serr)
		}
		existingEvents = append(existingEvents, ev)
	}
	if rerr := snapRows.Err(); rerr != nil {
		_ = snapRows.Close()
		return res, fmt.Errorf("iterating snapshot events in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, rerr)
	}
	_ = snapRows.Close()

	var previousIDs, retractedIDs []string
	previousIDs, retractedIDs, canonical = reconcileSnapshotWithCanonical(existingEvents, canonical, canonicalByID)
	res.PreviousEventIDs = previousIDs
	res.RetractedEventIDs = retractedIDs

	// 1. Delete old address refs for previous events in [FromLedger, ToLedger],
	//    excluding any event_id that still exists on a different network.
	if len(previousIDs) > 0 {
		placeholders := make([]string, len(previousIDs))
		args := make([]any, 0, len(previousIDs)+2)
		for i, id := range previousIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		args = append(args, network, network)
		query := `DELETE FROM event_addresses
			WHERE event_id IN (` + strings.Join(placeholders, ",") + `)
			  AND NOT EXISTS (
			      SELECT 1 FROM events e
			      WHERE e.id = event_addresses.event_id
			        AND NOT (e.network = ? OR (? = 'default' AND e.network = ''))
			  )`
		execRes, derr := tx.ExecContext(ctx, query, args...)
		if derr != nil && !isSQLiteMissingTable(derr) {
			return res, fmt.Errorf("retracting address refs in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
		}
		if derr == nil {
			res.RetractedAddressRefs, _ = execRes.RowsAffected()
		}
	}

	// 2. Delete previous events in [FromLedger, ToLedger] and upsert canonical events.
	if _, derr := tx.ExecContext(ctx,
		`DELETE FROM events WHERE (network = ? OR (? = 'default' AND network = '')) AND ledger >= ? AND ledger <= ?`,
		network, network, opts.FromLedger, opts.ToLedger); derr != nil {
		return res, fmt.Errorf("retracting events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
	}
	if len(canonical) > 0 {
		if _, uerr := upsertCanonicalEventsSQLiteTx(ctx, tx, canonical); uerr != nil {
			return res, fmt.Errorf("upserting canonical events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, uerr)
		}
	}

	// 3. Delete unresolved dead letters in [FromLedger, ToLedger] scoped to
	//    contracts not exclusively watched on a different network.
	dlRes, dlErr := tx.ExecContext(ctx, `
		DELETE FROM dead_letters
		WHERE ledger >= ? AND ledger <= ?
		  AND NOT EXISTS (
		      SELECT 1 FROM watched_contracts wc
		      WHERE wc.contract_id = dead_letters.contract_id
		        AND NOT (wc.network = ? OR (? = 'default' AND wc.network = ''))
		        AND NOT EXISTS (
		            SELECT 1 FROM watched_contracts wc2
		            WHERE wc2.contract_id = dead_letters.contract_id
		              AND (wc2.network = ? OR (? = 'default' AND wc2.network = ''))
		        )
		  )`,
		opts.FromLedger, opts.ToLedger, network, network, network, network)
	if dlErr != nil && !isSQLiteMissingTable(dlErr) {
		return res, fmt.Errorf("retracting dead letters in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, dlErr)
	}
	if dlErr == nil {
		res.RetractedDeadLetters, _ = dlRes.RowsAffected()
	}

	// 4. Re-index canonical address references.
	for _, r := range dedupRefs {
		if _, rerr := tx.ExecContext(ctx, `
			INSERT INTO event_addresses (address, event_id, role)
			VALUES (?, ?, ?)
			ON CONFLICT (address, event_id, role) DO NOTHING`,
			r.Address, r.EventID, r.Role); rerr != nil {
			if isSQLiteMissingTable(rerr) {
				break
			}
			return res, fmt.Errorf("upserting canonical address refs: %w", rerr)
		}
		res.InsertedAddressRefs++
	}

	now := formatTime(time.Now().UTC())

	// 5. Reconcile per-contract cursors for contracts belonging to this network
	//    (or unwatched contracts not exclusively bound to another network).
	cursorRows, cerr := tx.QueryContext(ctx, `
		SELECT contract_id, last_ingested_ledger, last_cursor
		FROM contract_cursors
		WHERE last_ingested_ledger >= ?
		  AND NOT EXISTS (
		      SELECT 1 FROM watched_contracts wc
		      WHERE wc.contract_id = contract_cursors.contract_id
		        AND NOT (wc.network = ? OR (? = 'default' AND wc.network = ''))
		        AND NOT EXISTS (
		            SELECT 1 FROM watched_contracts wc2
		            WHERE wc2.contract_id = contract_cursors.contract_id
		              AND (wc2.network = ? OR (? = 'default' AND wc2.network = ''))
		        )
		  )
		ORDER BY contract_id`,
		opts.FromLedger, network, network, network, network)
	if cerr != nil && !isSQLiteMissingTable(cerr) {
		return res, fmt.Errorf("listing contract cursors for reorg reconciliation: %w", cerr)
	}
	var cursors []ContractCursor
	if cerr == nil {
		for cursorRows.Next() {
			var cc ContractCursor
			if serr := cursorRows.Scan(&cc.ContractID, &cc.LastIngestedLedger, &cc.LastCursor); serr != nil {
				_ = cursorRows.Close()
				return res, serr
			}
			cursors = append(cursors, cc)
		}
		if rerr := cursorRows.Err(); rerr != nil {
			_ = cursorRows.Close()
			return res, rerr
		}
		_ = cursorRows.Close()
	}
	for _, cc := range cursors {
		if cc.LastIngestedLedger < opts.FromLedger {
			continue
		}
		targetLedger := cc.LastIngestedLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		if targetLedger != cc.LastIngestedLedger || cc.LastCursor != "" {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE contract_cursors SET last_ingested_ledger = ?, last_cursor = '', updated_at = ? WHERE contract_id = ?`,
				targetLedger, now, cc.ContractID); uerr != nil {
				return res, fmt.Errorf("rewinding contract cursor %s: %w", cc.ContractID, uerr)
			}
			res.RewoundContractCursors++
		}
	}

	// 6. Reconcile global ingestion_state.
	var (
		ingLedger int64
		ingCursor string
	)
	ierr := tx.QueryRowContext(ctx,
		`SELECT last_ingested_ledger, last_cursor FROM ingestion_state WHERE network = ?`,
		network).Scan(&ingLedger, &ingCursor)
	if ierr != nil && !errors.Is(ierr, sql.ErrNoRows) && !isSQLiteMissingTable(ierr) {
		return res, fmt.Errorf("loading ingestion state for reorg reconciliation: %w", ierr)
	}
	if ierr == nil && ingLedger >= opts.FromLedger {
		targetLedger := ingLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		clearCursor := ingCursor != "" && (ingLedger <= opts.ToLedger || targetLedger != ingLedger)
		if targetLedger != ingLedger || clearCursor {
			newCursor := ingCursor
			if clearCursor {
				newCursor = ""
			}
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE ingestion_state SET last_ingested_ledger = ?, last_cursor = ?, updated_at = ? WHERE network = ?`,
				targetLedger, newCursor, now, network); uerr != nil {
				return res, fmt.Errorf("rewinding ingestion state: %w", uerr)
			}
			res.RewoundIngestionLedger = targetLedger
		}
	}

	// 7. Reconcile audit_state.
	var verifiedThrough int64
	aerr := tx.QueryRowContext(ctx,
		`SELECT verified_through_ledger FROM audit_state WHERE network = ?`,
		network).Scan(&verifiedThrough)
	if aerr != nil && !errors.Is(aerr, sql.ErrNoRows) && !isSQLiteMissingTable(aerr) {
		return res, fmt.Errorf("loading audit state for reorg reconciliation: %w", aerr)
	}
	if aerr == nil && verifiedThrough >= opts.FromLedger {
		clamped := max(opts.FromLedger-1, int64(0))
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE audit_state SET verified_through_ledger = ?, updated_at = ? WHERE network = ?`,
			clamped, now, network); uerr != nil {
			return res, fmt.Errorf("clamping audit state to %d: %w", clamped, uerr)
		}
		res.RewoundAuditLedger = clamped
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("committing reorg tx: %w", err)
	}
	return res, nil
}

func (s *SQLite) DeleteEventsInRange(ctx context.Context, network string, fromLedger, toLedger int64) ([]string, error) {
	network = defaultNetwork(network)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM events WHERE (network = ? OR (? = 'default' AND network = '')) AND ledger >= ? AND ledger <= ? ORDER BY id`,
		network, network, fromLedger, toLedger)
	if err != nil {
		return nil, fmt.Errorf("selecting events in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM events WHERE (network = ? OR (? = 'default' AND network = '')) AND ledger >= ? AND ledger <= ?`,
		network, network, fromLedger, toLedger); err != nil {
		return nil, fmt.Errorf("deleting events in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	return ids, nil
}

func isSQLiteMissingTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

func upsertCanonicalEventsSQLiteTx(ctx context.Context, tx *sql.Tx, events []Event) (int64, error) {
	conflict := `ON CONFLICT (network, id) DO UPDATE SET
		contract_id        = excluded.contract_id,
		ledger             = excluded.ledger,
		type               = excluded.type,
		tx_hash            = excluded.tx_hash,
		tx_index           = excluded.tx_index,
		op_index           = excluded.op_index,
		in_successful_call = excluded.in_successful_call,
		topics             = excluded.topics,
		value              = excluded.value,
		created_at         = excluded.created_at,
		topics_xdr         = COALESCE(excluded.topics_xdr, topics_xdr),
		value_xdr          = COALESCE(excluded.value_xdr, value_xdr)`

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO events (`+eventColumnsSQLite+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`+conflict)
	if err != nil {
		return 0, fmt.Errorf("prepare canonical upsert tx: %w", err)
	}
	defer stmt.Close()

	var affected int64
	for _, e := range events {
		res, err := stmt.ExecContext(ctx,
			e.Network, e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value, formatTime(e.CreatedAt),
			nullableXDRTopics(e.RawTopicXDR), nullableText(e.RawValueXDR),
		)
		if err != nil {
			return affected, fmt.Errorf("inserting canonical event %s: %w", e.ID, err)
		}
		n, _ := res.RowsAffected()
		affected += n
	}
	return affected, nil
}

func (s *SQLite) DeleteAddressRefsByEventIDs(ctx context.Context, eventIDs []string) (int64, error) {
	if len(eventIDs) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(eventIDs))
	args := make([]any, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := `DELETE FROM event_addresses WHERE event_id IN (` + strings.Join(placeholders, ",") + `)`
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		if isSQLiteMissingTable(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("deleting address refs by event IDs: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *SQLite) DeleteDeadLettersInRange(ctx context.Context, fromLedger, toLedger int64) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM dead_letters WHERE ledger >= ? AND ledger <= ?`,
		fromLedger, toLedger)
	if err != nil {
		if isSQLiteMissingTable(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("deleting dead letters in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// --- Postgres ReorgMaintainer implementation ---

var (
	_ ReorgMaintainer              = (*Postgres)(nil)
	_ TransactionalReorgMaintainer = (*Postgres)(nil)
)

func (p *Postgres) ReconcileReorgTx(
	ctx context.Context,
	opts ReorgReconcileOptions,
	canonical []Event,
	canonicalByID map[string]Event,
	dedupRefs []AddressRef,
	insertedIDs []string,
) (ReorgReconcileResult, error) {
	network := defaultNetwork(opts.Network)
	res := ReorgReconcileResult{
		FromLedger:       opts.FromLedger,
		ToLedger:         opts.ToLedger,
		InsertedEventIDs: insertedIDs,
	}

	if len(canonical) > 0 {
		if err := p.ensureEventPartitions(ctx, canonical); err != nil {
			return res, err
		}
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin reorg tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 0. Snapshot existing events in [FromLedger, ToLedger] for network inside
	//    the transaction so no concurrent write can interleave between reading
	//    the snapshot and replacing the rows.
	snapRows, qerr := tx.Query(ctx,
		`SELECT `+eventColumns+` FROM events WHERE network = $1 AND ledger BETWEEN $2 AND $3 ORDER BY id ASC`,
		network, opts.FromLedger, opts.ToLedger)
	if qerr != nil {
		return res, fmt.Errorf("snapshot events in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, qerr)
	}
	var existingEvents []Event
	for snapRows.Next() {
		ev, serr := scanEvent(snapRows)
		if serr != nil {
			snapRows.Close()
			return res, fmt.Errorf("scanning snapshot event in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, serr)
		}
		existingEvents = append(existingEvents, ev)
	}
	if rerr := snapRows.Err(); rerr != nil {
		snapRows.Close()
		return res, fmt.Errorf("iterating snapshot events in reorg range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, rerr)
	}
	snapRows.Close()

	var previousIDs, retractedIDs []string
	previousIDs, retractedIDs, canonical = reconcileSnapshotWithCanonical(existingEvents, canonical, canonicalByID)
	res.PreviousEventIDs = previousIDs
	res.RetractedEventIDs = retractedIDs

	// 1. Delete old address refs for previous events in [FromLedger, ToLedger],
	//    excluding any event_id that still exists on a different network.
	if len(previousIDs) > 0 {
		tag, derr := tx.Exec(ctx, `
			DELETE FROM event_addresses
			WHERE event_id = ANY($1)
			  AND NOT EXISTS (
			      SELECT 1 FROM events e
			      WHERE e.id = event_addresses.event_id
			        AND e.network <> $2
			  )`,
			previousIDs, network)
		if derr != nil {
			return res, fmt.Errorf("retracting address refs in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
		}
		res.RetractedAddressRefs = tag.RowsAffected()
	}

	// 2. Delete previous events in [FromLedger, ToLedger] and upsert canonical events.
	if _, derr := tx.Exec(ctx,
		`DELETE FROM events WHERE network = $1 AND ledger BETWEEN $2 AND $3`,
		network, opts.FromLedger, opts.ToLedger); derr != nil {
		return res, fmt.Errorf("retracting events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, derr)
	}
	if len(canonical) > 0 {
		results := tx.SendBatch(ctx, insertEventsBatch(canonical, true))
		for range canonical {
			if _, berr := results.Exec(); berr != nil {
				_ = results.Close()
				return res, fmt.Errorf("upserting canonical events in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, berr)
			}
		}
		if berr := results.Close(); berr != nil {
			return res, fmt.Errorf("closing canonical events batch: %w", berr)
		}
	}

	// 3. Delete unresolved dead letters in [FromLedger, ToLedger] scoped to
	//    contracts not exclusively watched on a different network.
	dlTag, dlErr := tx.Exec(ctx, `
		DELETE FROM dead_letters
		WHERE ledger BETWEEN $1 AND $2
		  AND NOT EXISTS (
		      SELECT 1 FROM watched_contracts wc
		      WHERE wc.contract_id = dead_letters.contract_id
		        AND wc.network <> $3
		        AND NOT EXISTS (
		            SELECT 1 FROM watched_contracts wc2
		            WHERE wc2.contract_id = dead_letters.contract_id
		              AND wc2.network = $3
		        )
		  )`,
		opts.FromLedger, opts.ToLedger, network)
	if dlErr != nil {
		return res, fmt.Errorf("retracting dead letters in range [%d,%d]: %w", opts.FromLedger, opts.ToLedger, dlErr)
	}
	res.RetractedDeadLetters = dlTag.RowsAffected()

	// 4. Re-index canonical address references.
	if len(dedupRefs) > 0 {
		refBatch := &pgx.Batch{}
		const refStmt = `
			INSERT INTO event_addresses (address, event_id, role)
			VALUES ($1, $2, $3)
			ON CONFLICT (address, event_id, role) DO NOTHING`
		for _, r := range dedupRefs {
			refBatch.Queue(refStmt, r.Address, r.EventID, r.Role)
		}
		refResults := tx.SendBatch(ctx, refBatch)
		for range dedupRefs {
			if _, rerr := refResults.Exec(); rerr != nil {
				_ = refResults.Close()
				return res, fmt.Errorf("upserting canonical address refs: %w", rerr)
			}
		}
		if rerr := refResults.Close(); rerr != nil {
			return res, fmt.Errorf("closing address refs batch: %w", rerr)
		}
		res.InsertedAddressRefs = int64(len(dedupRefs))
	}

	// 5. Reconcile per-contract cursors for contracts belonging to this network
	//    (or unwatched contracts not exclusively bound to another network).
	cursorRows, cerr := tx.Query(ctx, `
		SELECT contract_id, last_ingested_ledger, last_cursor
		FROM contract_cursors
		WHERE last_ingested_ledger >= $1
		  AND NOT EXISTS (
		      SELECT 1 FROM watched_contracts wc
		      WHERE wc.contract_id = contract_cursors.contract_id
		        AND wc.network <> $2
		        AND NOT EXISTS (
		            SELECT 1 FROM watched_contracts wc2
		            WHERE wc2.contract_id = contract_cursors.contract_id
		              AND wc2.network = $2
		        )
		  )`,
		opts.FromLedger, network)
	if cerr != nil {
		return res, fmt.Errorf("listing contract cursors for reorg reconciliation: %w", cerr)
	}
	var cursors []ContractCursor
	for cursorRows.Next() {
		var cc ContractCursor
		if serr := cursorRows.Scan(&cc.ContractID, &cc.LastIngestedLedger, &cc.LastCursor); serr != nil {
			cursorRows.Close()
			return res, serr
		}
		cursors = append(cursors, cc)
	}
	if rerr := cursorRows.Err(); rerr != nil {
		cursorRows.Close()
		return res, rerr
	}
	cursorRows.Close()

	for _, cc := range cursors {
		targetLedger := cc.LastIngestedLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		if targetLedger != cc.LastIngestedLedger || cc.LastCursor != "" {
			if _, uerr := tx.Exec(ctx,
				`UPDATE contract_cursors SET last_ingested_ledger = $2, last_cursor = '', updated_at = now() WHERE contract_id = $1`,
				cc.ContractID, targetLedger); uerr != nil {
				return res, fmt.Errorf("rewinding contract cursor %s: %w", cc.ContractID, uerr)
			}
			res.RewoundContractCursors++
		}
	}

	// 6. Reconcile global ingestion_state.
	var (
		ingLedger int64
		ingCursor string
	)
	ierr := tx.QueryRow(ctx,
		`SELECT last_ingested_ledger, last_cursor FROM ingestion_state WHERE network = $1`,
		network).Scan(&ingLedger, &ingCursor)
	if ierr != nil && !errors.Is(ierr, pgx.ErrNoRows) {
		return res, fmt.Errorf("loading ingestion state for reorg reconciliation: %w", ierr)
	}
	if ierr == nil && ingLedger >= opts.FromLedger {
		targetLedger := ingLedger
		if opts.RewindCursorsToFromLedger {
			targetLedger = opts.FromLedger - 1
		} else if opts.NewHeadLedger > 0 && targetLedger > opts.NewHeadLedger {
			targetLedger = opts.NewHeadLedger
		}
		clearCursor := ingCursor != "" && (ingLedger <= opts.ToLedger || targetLedger != ingLedger)
		if targetLedger != ingLedger || clearCursor {
			newCursor := ingCursor
			if clearCursor {
				newCursor = ""
			}
			if _, uerr := tx.Exec(ctx,
				`UPDATE ingestion_state SET last_ingested_ledger = $2, last_cursor = $3, updated_at = now() WHERE network = $1`,
				network, targetLedger, newCursor); uerr != nil {
				return res, fmt.Errorf("rewinding ingestion state: %w", uerr)
			}
			res.RewoundIngestionLedger = targetLedger
		}
	}

	// 7. Reconcile audit_state.
	var verifiedThrough int64
	aerr := tx.QueryRow(ctx,
		`SELECT verified_through_ledger FROM audit_state WHERE network = $1`,
		network).Scan(&verifiedThrough)
	if aerr != nil && !errors.Is(aerr, pgx.ErrNoRows) {
		return res, fmt.Errorf("loading audit state for reorg reconciliation: %w", aerr)
	}
	if aerr == nil && verifiedThrough >= opts.FromLedger {
		clamped := max(opts.FromLedger-1, int64(0))
		if _, uerr := tx.Exec(ctx,
			`UPDATE audit_state SET verified_through_ledger = $2, updated_at = now() WHERE network = $1`,
			network, clamped); uerr != nil {
			return res, fmt.Errorf("clamping audit state to %d: %w", clamped, uerr)
		}
		res.RewoundAuditLedger = clamped
	}

	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("committing reorg tx: %w", err)
	}
	return res, nil
}

func (p *Postgres) DeleteEventsInRange(ctx context.Context, network string, fromLedger, toLedger int64) ([]string, error) {
	network = defaultNetwork(network)
	rows, err := p.pool.Query(ctx,
		`DELETE FROM events WHERE network = $1 AND ledger BETWEEN $2 AND $3 RETURNING id`,
		network, fromLedger, toLedger)
	if err != nil {
		return nil, fmt.Errorf("deleting events in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	return ids, nil
}

func (p *Postgres) DeleteAddressRefsByEventIDs(ctx context.Context, eventIDs []string) (int64, error) {
	if len(eventIDs) == 0 {
		return 0, nil
	}
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM event_addresses WHERE event_id = ANY($1)`,
		eventIDs)
	if err != nil {
		return 0, fmt.Errorf("deleting address refs by event IDs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (p *Postgres) DeleteDeadLettersInRange(ctx context.Context, fromLedger, toLedger int64) (int64, error) {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM dead_letters WHERE ledger BETWEEN $1 AND $2`,
		fromLedger, toLedger)
	if err != nil {
		return 0, fmt.Errorf("deleting dead letters in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	return tag.RowsAffected(), nil
}
