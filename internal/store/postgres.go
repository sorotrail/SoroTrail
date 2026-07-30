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
// Both paths write all 14 columns of the events table including network.
func insertEventsBatch(events []Event, onUpdate bool) *pgx.Batch {
	conflict := `ON CONFLICT (network, ledger, id) DO NOTHING`
	if onUpdate {
		conflict = `ON CONFLICT (network, ledger, id) DO UPDATE SET
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
			raw_topic_xdr      = COALESCE(EXCLUDED.raw_topic_xdr, events.raw_topic_xdr),
			raw_value_xdr      = COALESCE(EXCLUDED.raw_value_xdr, events.raw_value_xdr)`
	}
	batch := &pgx.Batch{}
	sql := `
		INSERT INTO events
			(network, id, contract_id, ledger, type, tx_hash, tx_index, op_index,
			 in_successful_call, topics, value, created_at,
			 raw_topic_xdr, raw_value_xdr)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		` + conflict
	for _, e := range events {
		batch.Queue(sql,
			e.Network, e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value, e.CreatedAt,
			nullableTextArray(e.RawTopicXDR), nullableText(e.RawValueXDR),
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

func (p *Postgres) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	if err := p.ensureEventPartitions(ctx, events); err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	kept, err := rawXDRInRange(ctx, tx, fromLedger, toLedger)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM events WHERE ledger BETWEEN $1 AND $2`, fromLedger, toLedger); err != nil {
		return fmt.Errorf("deleting events in ledger range [%d,%d]: %w", fromLedger, toLedger, err)
	}

	if len(events) > 0 {
		events = restoreRawXDR(events, kept)
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

type rawXDR struct {
	topics []string
	value  string
}

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

const eventColumns = `network, id, contract_id, ledger, type, tx_hash, tx_index, op_index,
	in_successful_call, topics, value, created_at, raw_topic_xdr, raw_value_xdr`

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

func (p *Postgres) EventExists(ctx context.Context, id string) (bool, error) {
	var one int
	err := p.pool.QueryRow(ctx, `SELECT 1 FROM events WHERE id = $1`, id).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking event existence: %w", err)
	}
	return true, nil
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
	if f.Network != "" {
		where = append(where, "network = "+arg(f.Network))
	}
	if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if len(f.Topic) > 0 {
		where = append(where, "topics @> "+arg(fmt.Sprintf("[%s]", f.Topic))+"::jsonb")
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where,
			fmt.Sprintf("topics->%d = %s::jsonb", i, arg(topic)))
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

func (p *Postgres) GetIngestionState(ctx context.Context, network string) (IngestionState, error) {
	var s IngestionState
	err := p.pool.QueryRow(ctx,
		`SELECT network, last_ingested_ledger, last_cursor, updated_at FROM ingestion_state WHERE network = $1`,
		network,
	).Scan(&s.Network, &s.LastIngestedLedger, &s.LastCursor, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return IngestionState{}, ErrNotFound
	}
	if err != nil {
		return IngestionState{}, fmt.Errorf("loading ingestion state for network %q: %w", network, err)
	}
	return s, nil
}

func (p *Postgres) SaveIngestionState(ctx context.Context, s IngestionState) error {
	if s.Network == "" {
		s.Network = "default"
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (network) DO UPDATE SET
			last_ingested_ledger = EXCLUDED.last_ingested_ledger,
			last_cursor          = EXCLUDED.last_cursor,
			updated_at           = now()`,
		s.Network, s.LastIngestedLedger, s.LastCursor,
	)
	if err != nil {
		return fmt.Errorf("saving ingestion state for network %q: %w", s.Network, err)
	}
	return nil
}

func (p *Postgres) GetAuditState(ctx context.Context, network string) (AuditState, error) {
	var s AuditState
	err := p.pool.QueryRow(ctx,
		`SELECT network, verified_through_ledger, updated_at FROM audit_state WHERE network = $1`,
		network,
	).Scan(&s.Network, &s.VerifiedThroughLedger, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AuditState{}, ErrNotFound
	}
	if err != nil {
		return AuditState{}, fmt.Errorf("loading audit state for network %q: %w", network, err)
	}
	return s, nil
}

func (p *Postgres) SaveAuditState(ctx context.Context, s AuditState) error {
	if s.Network == "" {
		s.Network = "default"
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO audit_state (network, verified_through_ledger, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (network) DO UPDATE SET
			verified_through_ledger = EXCLUDED.verified_through_ledger,
			updated_at              = now()`,
		s.Network, s.VerifiedThroughLedger,
	)
	if err != nil {
		return fmt.Errorf("saving audit state for network %q: %w", s.Network, err)
	}
	return nil
}

func (p *Postgres) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error) {
	if network == "" {
		network = "default"
	}
	row := p.pool.QueryRow(ctx, `
		INSERT INTO audit_state (network, verified_through_ledger, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (network) DO UPDATE SET
			verified_through_ledger = EXCLUDED.verified_through_ledger,
			updated_at              = now()
		WHERE EXCLUDED.verified_through_ledger > audit_state.verified_through_ledger
		RETURNING network, verified_through_ledger, updated_at`,
		network, ledger,
	)
	var s AuditState
	if err := row.Scan(&s.Network, &s.VerifiedThroughLedger, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return p.GetAuditState(ctx, network)
		}
		return AuditState{}, fmt.Errorf("saving audit state for network %q: %w", network, err)
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

func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	var s Stats
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

	// Per-network stats.
	s.PerNetwork = make(map[string]PerNetworkStats)
	rows, err := p.pool.Query(ctx, `
		SELECT
			network,
			count(*)::bigint,
			COALESCE(i.last_ingested_ledger, 0),
			COALESCE(a.verified_through_ledger, 0)
		FROM events
		JOIN LATERAL (
			SELECT last_ingested_ledger FROM ingestion_state WHERE ingestion_state.network = events.network
		) i ON true
		JOIN LATERAL (
			SELECT verified_through_ledger FROM audit_state WHERE audit_state.network = events.network
		) a ON true
		GROUP BY network, i.last_ingested_ledger, a.verified_through_ledger`)
	if err != nil {
		return Stats{}, fmt.Errorf("loading per-network stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pns PerNetworkStats
		var network string
		if err := rows.Scan(&network, &pns.TotalEvents, &pns.LastIngestedLedger, &pns.VerifiedThroughLedger); err != nil {
			return Stats{}, fmt.Errorf("scanning per-network stats: %w", err)
		}
		s.PerNetwork[network] = pns
	}
	if err := rows.Err(); err != nil {
		return Stats{}, fmt.Errorf("reading per-network stats: %w", err)
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
	err := row.Scan(&e.Network, &e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash,
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
