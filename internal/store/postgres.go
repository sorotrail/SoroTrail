package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a lookup matches no rows.
var ErrNotFound = errors.New("not found")

// ErrInvalidCursor is returned when a pagination cursor is malformed for the
// requested ordering. It is caller error: the API maps it to 400, not 500.
var ErrInvalidCursor = errors.New("invalid cursor")

// DefaultQueryLimit applies when EventFilter.Limit is unset; MaxQueryLimit
// caps requested page sizes.
const (
	DefaultQueryLimit         = 50
	MaxQueryLimit             = 200
	DefaultEventPartitionSpan = 120960
)

// Postgres implements Store on a pgx connection pool.
type Postgres struct {
	pool          *pgxpool.Pool
	partitionSpan int64
}

var _ Store = (*Postgres)(nil)

// NewPostgres wraps an existing pool. The caller owns the pool's lifecycle.
// partitionSpan is optional; when unset or non-positive, the production
// default is used.
func NewPostgres(pool *pgxpool.Pool, partitionSpan ...int64) *Postgres {
	span := int64(DefaultEventPartitionSpan)
	if len(partitionSpan) > 0 && partitionSpan[0] > 0 {
		span = partitionSpan[0]
	}
	return &Postgres{pool: pool, partitionSpan: span}
}

func (p *Postgres) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	rows, err := p.upsertEvents(ctx, events, false)
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// insertEventsBatch builds the single batch used by upsertEvents (idempotent
// ingest, DO NOTHING) and ReplaceEventsInRange (auditor repair, DO UPDATE).
// Both paths write all 13 columns of the events table so topics_xdr /
// value_xdr are never silently dropped on the way in; a repair that
// arrives without XDR preserves what was already stored via the coalesce()
// clauses in the UPDATE branch (`sorotrail replay` relies on that).
//
// onUpdate=false → ON CONFLICT DO NOTHING (idempotent ingest);
// onUpdate=true  → ON CONFLICT DO UPDATE SET … (auditor repair, correcting
// topic/value drift on the RPC side).
func insertEventsBatch(events []Event, onUpdate bool) *pgx.Batch {
	conflict := `ON CONFLICT (ledger, id) DO NOTHING`
	if onUpdate {
		conflict = `ON CONFLICT (ledger, id) DO UPDATE SET
			contract_id        = EXCLUDED.contract_id,
			ledger             = EXCLUDED.ledger,
			type               = EXCLUDED.type,
			tx_hash            = EXCLUDED.tx_hash,
			tx_index           = EXCLUDED.tx_index,
			op_index           = EXCLUDED.op_index,
			in_successful_call = EXCLUDED.in_successful_call,
			topics             = EXCLUDED.topics,
			value              = EXCLUDED.value,
			created_at         = EXCLUDED.created_at,
			raw_topic_xdr      = coalesce(EXCLUDED.raw_topic_xdr, events.raw_topic_xdr),
			raw_value_xdr      = coalesce(EXCLUDED.raw_value_xdr, events.raw_value_xdr)`
	}
	stmt := `
		INSERT INTO events
			(id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			 in_successful_call, topics, value, created_at,
			 raw_topic_xdr, raw_value_xdr)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		` + conflict
	batch := &pgx.Batch{}
	for _, e := range events {
		// 13 placeholders → 13 args. nullable helpers turn empty raw XDR
		// into SQL NULL so the column has one representation of "absent"
		// rather than two.
		batch.Queue(stmt,
			e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value, e.CreatedAt,
			nullableStringSlice(e.RawTopicXDR), nullableText(e.RawValueXDR),
		)
	}
	return batch
}

func (p *Postgres) ensureEventPartitions(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	minLedger, maxLedger := events[0].Ledger, events[0].Ledger
	for _, e := range events[1:] {
		if e.Ledger < minLedger {
			minLedger = e.Ledger
		}
		if e.Ledger > maxLedger {
			maxLedger = e.Ledger
		}
	}
	_, err := p.pool.Exec(ctx, `SELECT ensure_event_partitions($1, $2, $3)`, minLedger, maxLedger, p.partitionSpan)
	if err != nil {
		return fmt.Errorf("ensuring event partitions for ledger range [%d,%d]: %w", minLedger, maxLedger, err)
	}
	return nil
}

// upsertEvents is shared by UpsertEvents (idempotent insert) and
// ReplaceEventsInRange (insert-or-update, so topic/value drift on the RPC
// side is corrected). onUpdate=false → ON CONFLICT DO NOTHING;
// onUpdate=true → ON CONFLICT DO UPDATE SET ….
func (p *Postgres) upsertEvents(ctx context.Context, events []Event, onUpdate bool) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	if err := p.ensureEventPartitions(ctx, events); err != nil {
		return 0, err
	}
	results := p.pool.SendBatch(ctx, insertEventsBatch(events, onUpdate))
	defer results.Close()

	var affected int64
	for range events {
		tag, err := results.Exec()
		if err != nil {
			return affected, fmt.Errorf("upserting events: %w", err)
		}
		affected += tag.RowsAffected()
	}
	return affected, nil
}

// ReplaceEventsInRange makes [fromLedger, toLedger] match `events` exactly:
// orphans are deleted and same-ID rows are updated (correcting topic/value
// drift). The auditor calls this for targeted repair; ingest never does
// because UpsertEvents is idempotent and therefore cheaper.
//
// Raw XDR is carried across the replace: incoming XDR wins, but an event
// that arrives without any keeps whatever was already stored, so repairing
// a range never makes its rows unreplayable.
func (p *Postgres) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	if err := p.ensureEventPartitions(ctx, events); err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The delete below drops the rows the ON CONFLICT clause would otherwise
	// have preserved raw XDR from, so snapshot it first. A repair fetch that
	// came back without XDR must not cost surviving events their
	// replayability (see internal/replay).
	kept, err := rawXDRInRange(ctx, tx, fromLedger, toLedger)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM events WHERE ledger BETWEEN $1 AND $2`, fromLedger, toLedger); err != nil {
		return fmt.Errorf("deleting events in ledger range [%d,%d]: %w", fromLedger, toLedger, err)
	}

	if len(events) > 0 {
		events = restoreRawXDR(events, kept)
		// Same INSERT as upsertEvents with onUpdate=true, but routed through
		// the in-flight transaction so the delete + insert are atomic.
		results := tx.SendBatch(ctx, insertEventsBatch(events, true))
		for range events {
			if _, err := results.Exec(); err != nil {
				results.Close()
				return fmt.Errorf("inserting repaired events: %w", err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("closing batch in repair tx: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing repair tx: %w", err)
	}
	return nil
}

// rawXDR is the replayable payload kept for one event.
type rawXDR struct {
	topics []string
	value  string
}

// rawXDRInRange reads the raw XDR currently stored for a ledger range,
// keyed by event ID, so a delete-and-reinsert repair can put it back.
func rawXDRInRange(ctx context.Context, tx pgx.Tx, fromLedger, toLedger int64) (map[string]rawXDR, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, raw_topic_xdr, raw_value_xdr
		FROM events
		WHERE ledger BETWEEN $1 AND $2
		  AND (raw_topic_xdr IS NOT NULL OR raw_value_xdr IS NOT NULL)`,
		fromLedger, toLedger)
	if err != nil {
		return nil, fmt.Errorf("reading raw XDR in ledger range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	defer rows.Close()

	kept := map[string]rawXDR{}
	for rows.Next() {
		var (
			id     string
			topics []string
			value  *string
		)
		if err := rows.Scan(&id, &topics, &value); err != nil {
			return nil, fmt.Errorf("scanning raw XDR in ledger range: %w", err)
		}
		r := rawXDR{topics: topics}
		if value != nil {
			r.value = *value
		}
		kept[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading raw XDR in ledger range: %w", err)
	}
	return kept, nil
}

// restoreRawXDR fills in raw XDR the incoming events lack from what was
// already stored. Incoming XDR always wins — this only fills gaps, so a
// repair can add replayability but never remove it.
func restoreRawXDR(events []Event, kept map[string]rawXDR) []Event {
	if len(kept) == 0 {
		return events
	}
	out := make([]Event, len(events))
	copy(out, events)
	for i := range out {
		prev, ok := kept[out[i].ID]
		if !ok {
			continue
		}
		if len(out[i].RawTopicXDR) == 0 {
			out[i].RawTopicXDR = prev.topics
		}
		if out[i].RawValueXDR == "" {
			out[i].RawValueXDR = prev.value
		}
	}
	return out
}

// LedgerRangeCensus returns one row per ledger that holds at least one
// event in the inclusive [fromLedger, toLedger] range. idsOnly=false →
// counts only (cheap path for the common "all good" sweep); idsOnly=true
// → per-ledger sorted ID list (used to diff a ledger whose count
// disagrees with the RPC).
func (p *Postgres) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT ledger, count(*)::int, array_agg(id ORDER BY id)
		FROM events
		WHERE ledger BETWEEN $1 AND $2
		GROUP BY ledger
		ORDER BY ledger`,
		fromLedger, toLedger,
	)
	if err != nil {
		return nil, fmt.Errorf("ledger range census [%d,%d]: %w", fromLedger, toLedger, err)
	}
	defer rows.Close()

	var out []LedgerCensus
	for rows.Next() {
		var c LedgerCensus
		var ids []string
		if err := rows.Scan(&c.Ledger, &c.Count, &ids); err != nil {
			return nil, err
		}
		if idsOnly {
			c.IDs = ids
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

const eventColumns = `id, contract_id, ledger, type, tx_hash, tx_index, op_index,
	in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr`

// nullableText and nullableStringSlice store empty raw-XDR values as SQL NULL
// so "no raw XDR" is one representation, not two.
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableStringSlice(s []string) any {
	if len(s) == 0 {
		return nil
	}
	return s
}

func (p *Postgres) GetEvent(ctx context.Context, id string) (Event, error) {
	var e Event
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id)
		var err error
		e, err = scanEvent(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return Event{}, ErrNotFound
	}
	return e, err
}

// EventExists is the cheap 304 path used by the API's conditional GET:
// an index-only probe against the primary key that never deserializes
// the row, so it stays index-bound even on a wide row. Existence is a
// boolean because 304 responses carry no body — there's nothing to
// serialize. The call is what backs the "still here" check after a
// cache hit so that retention/pruning (#8) cannot leave a cache
// pretending a deleted event is still around; the full row is only
// loaded on a real cache miss via GetEvent.
func (p *Postgres) EventExists(ctx context.Context, id string) (bool, error) {
	var one int
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT 1 FROM events WHERE id = $1`, id).Scan(&one)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking event existence: %w", err)
	}
	return true, nil
}

// buildEventWhereClause builds the WHERE clause and arguments shared by
// QueryEvents and CountEvents. It does not include cursor, ordering, or
// limit — those are page-specific concerns.
func buildEventWhereClause(f EventFilter) ([]string, []any) {
	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		where = append(where, "type = ANY("+arg(f.Types)+")")
	}
	if len(f.Topic) > 0 {
		// Containment on the array matches the topic at any position.
		where = append(where, "topics @> "+arg(fmt.Sprintf("[%s]", f.Topic))+"::jsonb")
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where,
			fmt.Sprintf("topics->%d = %s::jsonb", i, arg(topic)))
	}
	if len(f.TopicContains) > 0 {
		// Direct containment — caller controls the shape (object wrapped in
		// array for element match, multi-element arrays for subset match).
		where = append(where, "topics @> "+arg(string(f.TopicContains))+"::jsonb")
	}
	if f.TxHash != "" {
		where = append(where, "tx_hash = "+arg(f.TxHash))
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}
	if !f.FromTime.IsZero() {
		where = append(where, "created_at >= "+arg(f.FromTime))
	}
	if !f.ToTime.IsZero() {
		where = append(where, "created_at <= "+arg(f.ToTime))
	}
	return where, args
}

func (p *Postgres) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}

	where, args := buildEventWhereClause(f)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	if !ValidOrderBy(f.OrderBy) {
		return nil, "", fmt.Errorf("unsupported order_by %q", f.OrderBy)
	}
	orderDir := "ASC"
	cursorOp := ">"
	if f.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}

	// Every ordering ends in id so the sort is total: ledger and created_at
	// both have duplicates, and without a tiebreaker the database may order
	// equal rows differently between two queries, which would let keyset
	// pagination skip or repeat rows at a page boundary.
	orderCols := "id " + orderDir
	if f.OrderBy != "" && f.OrderBy != OrderByID {
		orderCols = f.OrderBy + " " + orderDir + ", id " + orderDir
	}

	if f.Cursor != "" {
		switch f.OrderBy {
		case "", OrderByID:
			where = append(where, "id "+cursorOp+" "+arg(f.Cursor))
		default:
			sortValue, id, err := decodeCompositeCursor(f.Cursor)
			if err != nil {
				return nil, "", err
			}
			// Row-value comparison gives the correct "everything after this
			// (value, id) pair" semantics in one predicate, and Postgres can
			// still drive it from an index on (sort column, id).
			var typed string
			switch f.OrderBy {
			case OrderByLedger:
				typed = arg(sortValue) + "::bigint"
			case OrderByCreatedAt:
				typed = arg(sortValue) + "::timestamptz"
			}
			where = append(where, fmt.Sprintf("(%s, id) %s (%s, %s)",
				f.OrderBy, cursorOp, typed, arg(id)))
		}
	}

	query := `SELECT ` + eventColumns + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to know whether a next page exists.
	query += " ORDER BY " + orderCols + " LIMIT " + arg(limit+1)

	var events []Event
	next := ""
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("querying events: %w", err)
		}
		defer rows.Close()

		events = make([]Event, 0, limit)
		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				return err
			}
			events = append(events, e)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reading events: %w", err)
		}

		if len(events) > limit {
			events = events[:limit]
			next = EncodeCursor(f.OrderBy, events[limit-1])
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return events, next, nil
}

// CountEvents returns the total number of rows matching the filter,
// ignoring pagination (cursor, order, and limit). It reuses the same
// WHERE clause builder as QueryEvents so the two stay in lockstep.
func (p *Postgres) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	// Strip page-specific fields so the count represents the full match set.
	f.Cursor = ""
	f.Order = ""
	f.OrderBy = ""
	f.Limit = 0

	where, args := buildEventWhereClause(f)
	query := `SELECT count(*) FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	var total int64
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, args...).Scan(&total)
	})
	if err != nil {
		return 0, fmt.Errorf("counting events: %w", err)
	}
	return total, nil
}

func (p *Postgres) GetIngestionState(ctx context.Context) (IngestionState, error) {
	var s IngestionState
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT last_ingested_ledger, last_cursor, updated_at FROM ingestion_state WHERE id = 1`,
		).Scan(&s.LastIngestedLedger, &s.LastCursor, &s.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return IngestionState{}, ErrNotFound
	}
	if err != nil {
		return IngestionState{}, fmt.Errorf("loading ingestion state: %w", err)
	}
	return s, nil
}

func (p *Postgres) SaveIngestionState(ctx context.Context, s IngestionState) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingestion_state (id, last_ingested_ledger, last_cursor, updated_at)
		VALUES (1, $1, $2, now())
		ON CONFLICT (id) DO UPDATE SET
			last_ingested_ledger = EXCLUDED.last_ingested_ledger,
			last_cursor          = EXCLUDED.last_cursor,
			updated_at           = now()`,
		s.LastIngestedLedger, s.LastCursor,
	)
	if err != nil {
		return fmt.Errorf("saving ingestion state: %w", err)
	}
	return nil
}

func (p *Postgres) GetAuditState(ctx context.Context) (AuditState, error) {
	var s AuditState
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT verified_through_ledger, updated_at FROM audit_state WHERE id = 1`,
		).Scan(&s.VerifiedThroughLedger, &s.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return AuditState{}, ErrNotFound
	}
	if err != nil {
		return AuditState{}, fmt.Errorf("loading audit state: %w", err)
	}
	return s, nil
}

func (p *Postgres) SaveAuditState(ctx context.Context, s AuditState) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO audit_state (id, verified_through_ledger, updated_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET
			verified_through_ledger = EXCLUDED.verified_through_ledger,
			updated_at              = now()`,
		s.VerifiedThroughLedger,
	)
	if err != nil {
		return fmt.Errorf("saving audit state: %w", err)
	}
	return nil
}

func (p *Postgres) SaveAuditStateIfGreater(ctx context.Context, ledger int64) (AuditState, error) {
	// Single round trip — the WHERE on the singleton row makes the
	// upsert no-op when the candidate is not greater than what's stored.
	row := p.pool.QueryRow(ctx, `
		INSERT INTO audit_state (id, verified_through_ledger, updated_at)
		VALUES (1, $1, now())
		ON CONFLICT (id) DO UPDATE SET
			verified_through_ledger = EXCLUDED.verified_through_ledger,
			updated_at              = now()
		WHERE EXCLUDED.verified_through_ledger > audit_state.verified_through_ledger
		RETURNING verified_through_ledger, updated_at`,
		ledger,
	)
	var s AuditState
	if err := row.Scan(&s.VerifiedThroughLedger, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Candidate was not greater — return the existing state.
			return p.GetAuditState(ctx)
		}
		return AuditState{}, fmt.Errorf("saving audit state: %w", err)
	}
	return s, nil
}

func (p *Postgres) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT contract_id, added_at FROM watched_contracts ORDER BY contract_id`)
	if err != nil {
		return nil, fmt.Errorf("listing watched contracts: %w", err)
	}
	defer rows.Close()

	var out []WatchedContract
	for rows.Next() {
		var wc WatchedContract
		if err := rows.Scan(&wc.ContractID, &wc.AddedAt); err != nil {
			return nil, err
		}
		out = append(out, wc)
	}
	return out, rows.Err()
}

// RemoveWatchedContract stops future ingestion for the given contract by
// removing its row from watched_contracts. It does NOT delete any event
// rows already in storage — those remain queryable. ErrNotFound signals a
// typo so the API can respond 404.
func (p *Postgres) RemoveWatchedContract(ctx context.Context, contractID string) error {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM watched_contracts WHERE contract_id = $1`, contractID)
	if err != nil {
		return fmt.Errorf("removing watched contract: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) AddWatchedContract(ctx context.Context, contractID string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO watched_contracts (contract_id) VALUES ($1)
		ON CONFLICT (contract_id) DO NOTHING`, contractID)
	if err != nil {
		return fmt.Errorf("adding watched contract: %w", err)
	}
	return nil
}

func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	// COUNT(DISTINCT contract_id) scans the contract_id index; fine at MVP
	// scale. contributors: replace with a maintained summary table if it
	// becomes a bottleneck on large datasets.
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT
				(SELECT count(*) FROM events),
				(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
				(SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state),
				(SELECT coalesce(min(ledger), 0) FROM events),
				(SELECT count(DISTINCT contract_id) FROM events),
				(SELECT count(*) FROM watched_contracts)`,
		).Scan(&s.TotalEvents, &s.LastIngestedLedger, &s.VerifiedThroughLedger, &s.OldestStoredLedger, &s.ContractCount, &s.WatchedContracts)
	})
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return s, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *Postgres) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	var specJSON []byte
	err := p.pool.QueryRow(ctx,
		`SELECT spec_json FROM contract_specs WHERE wasm_hash = $1`, wasmHash,
	).Scan(&specJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("loading contract spec for wasm_hash %s: %w", wasmHash, err)
	}
	return specJSON, nil
}

func (p *Postgres) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO contract_specs (wasm_hash, contract_id, spec_json, fetched_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (wasm_hash) DO UPDATE SET
			spec_json  = EXCLUDED.spec_json,
			fetched_at = now()`,
		wasmHash, contractID, specJSON,
	)
	if err != nil {
		return fmt.Errorf("saving contract spec for %s: %w", wasmHash, err)
	}
	return nil
}

func (p *Postgres) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	err := p.pool.QueryRow(ctx, `
		INSERT INTO audit_findings
			(from_ledger, to_ledger, expected_count, actual_count,
			 missing_ids, status, attempts)
		VALUES ($1, $2, $3, $4, $5, $6, 0)
		RETURNING id, created_at`,
		f.FromLedger, f.ToLedger, f.ExpectedCount, f.ActualCount,
		f.MissingIDs, f.Status,
	).Scan(&f.ID, &f.CreatedAt)
	if err != nil {
		return AuditFinding{}, fmt.Errorf("recording audit finding: %w", err)
	}
	return f, nil
}

func (p *Postgres) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	// last_attempted_at is updated only when the auditor actually tried
	// to repair (not when only the metadata is tweaked).
	var lastAttempted any
	if !f.LastAttemptedAt.IsZero() {
		lastAttempted = f.LastAttemptedAt
	} else {
		lastAttempted = nil
	}
	_, err := p.pool.Exec(ctx, `
		UPDATE audit_findings SET
			status            = $2,
			attempts          = $3,
			last_attempted_at = $4,
			last_error        = $5,
			missing_ids       = $6
		WHERE id = $1`,
		f.ID, f.Status, f.Attempts, lastAttempted, nullableString(f.LastError), f.MissingIDs,
	)
	if err != nil {
		return fmt.Errorf("updating audit finding %d: %w", f.ID, err)
	}
	return nil
}

// ListOpenFindingsByRange returns the most recent finding whose range
// overlaps [fromLedger, toLedger] and is still in a working state (open
// or unrecoverable). ErrNotFound means no live finding spans the range.
func (p *Postgres) ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT id, from_ledger, to_ledger, expected_count, actual_count,
		       missing_ids, status, attempts, last_attempted_at, last_error, created_at
		FROM audit_findings
		WHERE status IN ('open', 'unrecoverable')
		  AND from_ledger <= $2
		  AND to_ledger   >= $1
		ORDER BY id DESC
		LIMIT 1`,
		fromLedger, toLedger,
	)
	var f AuditFinding
	var lastAttempted *time.Time
	err := row.Scan(&f.ID, &f.FromLedger, &f.ToLedger, &f.ExpectedCount, &f.ActualCount,
		&f.MissingIDs, &f.Status, &f.Attempts, &lastAttempted, &f.LastError, &f.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuditFinding{}, ErrNotFound
	}
	if err != nil {
		return AuditFinding{}, fmt.Errorf("listing findings for [%d,%d]: %w", fromLedger, toLedger, err)
	}
	if lastAttempted != nil {
		f.LastAttemptedAt = *lastAttempted
	}
	return f, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (p *Postgres) withStatementTimeoutTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin guarded tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if timeout, ok := statementTimeoutFromContext(ctx); ok && timeout > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", timeout.Milliseconds())); err != nil {
			return fmt.Errorf("setting statement_timeout: %w", err)
		}
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit guarded tx: %w", err)
	}
	return nil
}

func statementTimeoutFromContext(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return 0, false
	}
	if timeout < time.Millisecond {
		return time.Millisecond, true
	}
	return timeout, true
}

func scanEvent(row pgx.Row) (Event, error) {
	var (
		e         Event
		rawTopics []string
		rawValue  *string
	)
	err := row.Scan(&e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash,
		&e.TxIndex, &e.OpIndex, &e.InSuccessfulCall, &e.Topics, &e.Value,
		&e.CreatedAt, &rawTopics, &rawValue)
	if err != nil {
		return Event{}, err
	}
	e.RawTopicXDR = rawTopics
	if rawValue != nil {
		e.RawValueXDR = *rawValue
	}
	return e, nil
}
