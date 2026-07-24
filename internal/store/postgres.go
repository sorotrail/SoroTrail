package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a lookup matches no rows.
var ErrNotFound = errors.New("not found")

// DefaultQueryLimit applies when EventFilter.Limit is unset; MaxQueryLimit
// caps requested page sizes.
const (
	DefaultQueryLimit = 50
	MaxQueryLimit     = 200
)

// Postgres implements Store on a pgx connection pool.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Store = (*Postgres)(nil)

// NewPostgres wraps an existing pool. The caller owns the pool's lifecycle.
func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// bucketHour returns the UTC hour bucket for a ledger using the
// approximation that each Stellar ledger is ~5 seconds. Exact values
// don't matter for bucketing — the only requirement is consistency.
// The same ledger always maps to the same bucket.
func bucketHour(ledger int64) time.Time {
	// Reference: Soroban Protocol 20 launch ~2024-01-30 near ledger 53M.
	const refLedger = 53_000_000
	const refUnix = 1706572800 // 2024-01-30 00:00:00 UTC
	sec := refUnix + (ledger-refLedger)*5
	return time.Unix(sec, 0).UTC().Truncate(time.Hour)
}

// bucketKey is the compound key for rollup delta accumulation.
type bucketKey struct {
	bucketStart time.Time
	contractID  string
	eventType   string
}

// volDelta accumulates token transfer volume and unique addresses for one
// rollup bucket from newly inserted events.
type volDelta struct {
	volume    *big.Int
	addresses map[string]struct{}
}

func (p *Postgres) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin upsert tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Batch INSERT with RETURNING so only newly inserted rows (not
	// duplicate-ID skipped rows) are visible for rollup deltas.
	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(`
			INSERT INTO events
				(id, contract_id, ledger, type, tx_hash, tx_index, op_index,
				 in_successful_call, topics, value, raw_topic_xdr, raw_value_xdr)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (id) DO NOTHING
			RETURNING contract_id, type, ledger, topics, value`,
			e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value,
			nullableTextArray(e.RawTopicXDR), nullableText(e.RawValueXDR),
		)
	}

	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	// Aggregate deltas from newly inserted rows.
	eventDeltas := make(map[bucketKey]int64)
	volumeDeltas := make(map[bucketKey]*volDelta)

	var inserted int64
	for range events {
		var (
			contractID, eventType string
			ledger                int64
			topics, value         json.RawMessage
		)
		err := results.QueryRow().Scan(&contractID, &eventType, &ledger, &topics, &value)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // ON CONFLICT DO NOTHING — duplicate, not inserted
		}
		if err != nil {
			return inserted, fmt.Errorf("upserting events: %w", err)
		}
		inserted++

		key := bucketKey{
			bucketStart: bucketHour(ledger),
			contractID:  contractID,
			eventType:   eventType,
		}
		eventDeltas[key]++

		// Detect token transfer events for rollup_token_volume (#1).
		if amt, addrs, ok := tryExtractTransfer(topics, value); ok {
			vd, exists := volumeDeltas[key]
			if !exists {
				vd = &volDelta{volume: new(big.Int), addresses: make(map[string]struct{})}
				volumeDeltas[key] = vd
			}
			vd.volume.Add(vd.volume, amt)
			for _, a := range addrs {
				vd.addresses[a] = struct{}{}
			}
		}
	}

	if err := results.Close(); err != nil {
		return inserted, fmt.Errorf("closing insert batch: %w", err)
	}

	// Apply rollup_events deltas.
	if len(eventDeltas) > 0 {
		if err := applyEventRollups(ctx, tx, eventDeltas); err != nil {
			return inserted, err
		}
	}

	// Apply rollup_token_volume deltas.
	if len(volumeDeltas) > 0 {
		if err := applyVolumeRollups(ctx, tx, volumeDeltas); err != nil {
			return inserted, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return inserted, fmt.Errorf("committing upsert tx: %w", err)
	}

	return inserted, nil
}

// applyEventRollups upserts rollup_events deltas from newly inserted events.
func applyEventRollups(ctx context.Context, tx pgx.Tx, deltas map[bucketKey]int64) error {
	batch := &pgx.Batch{}
	for key, delta := range deltas {
		batch.Queue(`
			INSERT INTO rollup_events (bucket_start, contract_id, type, count)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (bucket_start, contract_id, type)
			DO UPDATE SET count = rollup_events.count + EXCLUDED.count`,
			key.bucketStart, key.contractID, key.eventType, delta,
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range deltas {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("applying rollup deltas: %w", err)
		}
	}
	return results.Close()
}

// applyVolumeRollups upserts rollup_token_volume deltas from newly inserted
// transfer events.
//
// contributors: unique_address_count is additive per-batch (COALESCE + COALESCE)
// rather than truly distinct across batches. Replace with a proper address-tracking
// table (e.g. rollup_addresses keyed by (bucket_start, contract_id, address)) when
// #1 lands so the count is an actual COUNT(DISTINCT).
func applyVolumeRollups(ctx context.Context, tx pgx.Tx, deltas map[bucketKey]*volDelta) error {
	batch := &pgx.Batch{}
	for key, vd := range deltas {
		batch.Queue(`
			INSERT INTO rollup_token_volume
				(bucket_start, contract_id, volume, unique_address_count)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (bucket_start, contract_id) DO UPDATE SET
				volume = rollup_token_volume.volume + EXCLUDED.volume,
				unique_address_count =
					rollup_token_volume.unique_address_count + EXCLUDED.unique_address_count`,
			key.bucketStart, key.contractID,
			vd.volume.String(), int64(len(vd.addresses)),
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range deltas {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("applying volume rollup deltas: %w", err)
		}
	}
	return results.Close()
}

// tryExtractTransfer inspects the decoded topics/value of an event to
// detect SEP-41-style transfer events. Returns (amount, addresses, true)
// when the event looks like a transfer, or (nil, nil, false) otherwise.
//
// Transfers are recognized by topics[0] == {"symbol":"transfer"} and
// value containing an i128 amount. Topics[1] and [2] carry address entries
// for the sender and receiver.
func tryExtractTransfer(topics, value json.RawMessage) (*big.Int, []string, bool) {
	var topicList []json.RawMessage
	if err := json.Unmarshal(topics, &topicList); err != nil || len(topicList) < 3 {
		return nil, nil, false
	}

	// First topic must be {"symbol":"transfer"}.
	var firstTopic map[string]string
	if err := json.Unmarshal(topicList[0], &firstTopic); err != nil || firstTopic["symbol"] != "transfer" {
		return nil, nil, false
	}

	// Value must carry an i128 amount.
	var valMap map[string]json.RawMessage
	if err := json.Unmarshal(value, &valMap); err != nil {
		return nil, nil, false
	}
	amountRaw, ok := valMap["i128"]
	if !ok {
		return nil, nil, false
	}
	var amountStr string
	if err := json.Unmarshal(amountRaw, &amountStr); err != nil {
		return nil, nil, false
	}
	amt, ok := new(big.Int).SetString(amountStr, 10)
	if !ok {
		return nil, nil, false
	}

	// Extract addresses from topics[1] and [2] (SEP-41 transfer has
	// exactly 3 topics: symbol, from, to). Limit to the first two
	// positions after the symbol to avoid picking up non-address
	// topics that happen to carry an "address" key.
	var addrs []string
	for _, t := range topicList[1:min(len(topicList), 3)] {
		var m map[string]string
		if err := json.Unmarshal(t, &m); err != nil {
			continue
		}
		if a, ok := m["address"]; ok && a != "" {
			addrs = append(addrs, a)
		}
	}

	return amt, addrs, true
}

// insertEventsBatch builds the event INSERT used by every write path, so
// the column list can't drift between them — notably raw_topic_xdr /
// raw_value_xdr, which `sorotrail replay` depends on: a path that forgot
// them would quietly make its rows unreplayable.
//
// onUpdate=false → ON CONFLICT DO NOTHING (idempotent ingest);
// onUpdate=true → ON CONFLICT DO UPDATE SET … (auditor repair, correcting
// topic/value drift on the RPC side).
func insertEventsBatch(events []Event, onUpdate bool) *pgx.Batch {
	conflict := "ON CONFLICT (id) DO NOTHING"
	if onUpdate {
		conflict = `ON CONFLICT (id) DO UPDATE SET
			contract_id = EXCLUDED.contract_id,
			ledger = EXCLUDED.ledger,
			type = EXCLUDED.type,
			tx_hash = EXCLUDED.tx_hash,
			tx_index = EXCLUDED.tx_index,
			op_index = EXCLUDED.op_index,
			in_successful_call = EXCLUDED.in_successful_call,
			topics = EXCLUDED.topics,
			value = EXCLUDED.value,
			-- Refresh the raw XDR, but never blank it out: a repair fetch
			-- that came back as JSON (xdrFormat "json") carries no XDR and
			-- must not destroy what an earlier ingest managed to keep.
			raw_topic_xdr = coalesce(EXCLUDED.raw_topic_xdr, events.raw_topic_xdr),
			raw_value_xdr = coalesce(EXCLUDED.raw_value_xdr, events.raw_value_xdr)`
	}
	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(`
			INSERT INTO events
				(id, contract_id, ledger, type, tx_hash, tx_index, op_index,
				 in_successful_call, topics, value, raw_topic_xdr, raw_value_xdr)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) `+conflict,
			e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value,
			nullableTextArray(e.RawTopicXDR), nullableText(e.RawValueXDR),
		)
	}
	return batch
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

// nullableText and nullableTextArray store empty raw-XDR values as SQL NULL
// so "no raw XDR" is one representation, not two.
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableTextArray(s []string) any {
	if len(s) == 0 {
		return nil
	}
	return s
}

func (p *Postgres) GetEvent(ctx context.Context, id string) (Event, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id)
	e, err := scanEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	return e, err
}

func (p *Postgres) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}

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
	if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if len(f.Topic) > 0 {
		// Containment on the array matches the topic at any position.
		where = append(where, "topics @> "+arg(fmt.Sprintf("[%s]", f.Topic))+"::jsonb")
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}

	// if f.Cursor != "" {
	// 	where = append(where, "id > "+arg(f.Cursor))
	// }
	orderDir := "ASC"
	cursorOp := ">"
	if f.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}

	if f.Cursor != "" {
		where = append(where, "id "+cursorOp+" "+arg(f.Cursor))
	}

	query := `SELECT ` + eventColumns + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to know whether a next page exists.
	// query += " ORDER BY id ASC LIMIT " + arg(limit+1)
	query += " ORDER BY id " + orderDir + " LIMIT " + arg(limit+1)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("querying events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, "", err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading events: %w", err)
	}

	next := ""
	if len(events) > limit {
		events = events[:limit]
		next = events[limit-1].ID
	}
	return events, next, nil
}

func (p *Postgres) GetIngestionState(ctx context.Context) (IngestionState, error) {
	var s IngestionState
	err := p.pool.QueryRow(ctx,
		`SELECT last_ingested_ledger, last_cursor, updated_at FROM ingestion_state WHERE id = 1`,
	).Scan(&s.LastIngestedLedger, &s.LastCursor, &s.UpdatedAt)
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
	err := p.pool.QueryRow(ctx,
		`SELECT verified_through_ledger, updated_at FROM audit_state WHERE id = 1`,
	).Scan(&s.VerifiedThroughLedger, &s.UpdatedAt)
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

func (p *Postgres) ListWatchedContracts(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT contract_id FROM watched_contracts ORDER BY contract_id`)
	if err != nil {
		return nil, fmt.Errorf("listing watched contracts: %w", err)
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
	return ids, rows.Err()
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

// QueryAnalyticsEvents returns time-bucketed event counts from the
// rollup_events table. Bucket="day" aggregates hourly buckets to daily
// at query time. Empty buckets are omitted (not zero-filled).
func (p *Postgres) QueryAnalyticsEvents(ctx context.Context, f AnalyticsFilter) ([]AnalyticsEventBucket, error) {
	trunc := "hour"
	if f.Bucket == "day" {
		trunc = "day"
	}

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
	if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if !f.From.IsZero() {
		where = append(where, "bucket_start >= "+arg(f.From.UTC()))
	}
	if !f.To.IsZero() {
		where = append(where, "bucket_start < "+arg(f.To.UTC()))
	}

	query := fmt.Sprintf(`
		SELECT date_trunc('%s', bucket_start) AS bucket,
		       contract_id, type,
		       sum(count)::bigint AS count
		FROM rollup_events`, trunc)
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY bucket, contract_id, type ORDER BY bucket ASC"

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying analytics events: %w", err)
	}
	defer rows.Close()

	var out []AnalyticsEventBucket
	for rows.Next() {
		var b AnalyticsEventBucket
		if err := rows.Scan(&b.BucketStart, &b.ContractID, &b.Type, &b.Count); err != nil {
			return nil, fmt.Errorf("scanning analytics event bucket: %w", err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// QueryAnalyticsTokenVolume returns time-bucketed transfer volume from the
// rollup_token_volume table. Populated only when the ingester recognizes
// SEP-41 transfer-shaped events (#1). Bucket="day" aggregates hourly.
func (p *Postgres) QueryAnalyticsTokenVolume(ctx context.Context, f AnalyticsFilter) ([]AnalyticsTokenVolume, error) {
	trunc := "hour"
	if f.Bucket == "day" {
		trunc = "day"
	}

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
	if !f.From.IsZero() {
		where = append(where, "bucket_start >= "+arg(f.From.UTC()))
	}
	if !f.To.IsZero() {
		where = append(where, "bucket_start < "+arg(f.To.UTC()))
	}

	query := fmt.Sprintf(`
		SELECT date_trunc('%s', bucket_start) AS bucket,
		       contract_id,
		       sum(volume)::text AS volume,
		       sum(unique_address_count)::bigint AS unique_address_count
		FROM rollup_token_volume`, trunc)
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY bucket, contract_id ORDER BY bucket ASC"

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying analytics token volume: %w", err)
	}
	defer rows.Close()

	var out []AnalyticsTokenVolume
	for rows.Next() {
		var v AnalyticsTokenVolume
		if err := rows.Scan(&v.BucketStart, &v.ContractID, &v.Volume, &v.UniqueAddressCount); err != nil {
			return nil, fmt.Errorf("scanning analytics token volume: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	// COUNT(DISTINCT contract_id) scans the contract_id index; fine at MVP
	// scale. contributors: replace with a maintained summary table if it
	// becomes a bottleneck on large datasets.
	err := p.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM events),
			(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
			(SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state),
			(SELECT count(DISTINCT contract_id) FROM events),
			(SELECT count(*) FROM watched_contracts)`,
	).Scan(&s.TotalEvents, &s.LastIngestedLedger, &s.VerifiedThroughLedger, &s.ContractCount, &s.WatchedContracts)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return s, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
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
