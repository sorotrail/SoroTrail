package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// ErrNotFound is returned by [Store] lookup methods when no matching row
// exists. Callers should use errors.Is to check for it.
//
// Returned by: [Store.GetEvent], [Store.GetIngestionState],
// [Store.GetAuditState], [Store.ListOpenFindingsByRange],
// [Postgres.GetReplayState].
var ErrNotFound = errors.New("not found")

// ErrInvalidCursor is returned when a pagination cursor is malformed for the
// requested ordering. It is caller error: the API maps it to 400, not 500.
var ErrInvalidCursor = errors.New("invalid cursor")

const (
	DefaultEventPartitionSpan = 120960

	poolHealthCheckInterval  = 5 * time.Second
	poolHealthCheckTimeout   = 2 * time.Second
	poolReconnectBaseBackoff = 100 * time.Millisecond
	poolReconnectMaxBackoff  = 30 * time.Second
)

// NewPool creates a pgx pool for databaseURL, applying optional sizing knobs
// (zero means "leave pgx's default"). Use this instead of pgxpool.New so the
// DB connection pool size is configurable from the environment.
func NewPool(ctx context.Context, databaseURL string, maxConns, minConns int32, maxConnLifetime, maxConnIdleTime time.Duration) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		poolConfig.MaxConns = maxConns
	}
	if minConns > 0 {
		poolConfig.MinConns = minConns
	}
	if maxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = maxConnLifetime
	}
	if maxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = maxConnIdleTime
	}
	return pgxpool.NewWithConfig(ctx, poolConfig)
}

// Postgres implements [Store] on a pgx connection pool. It is the only
// production implementation of the [Store] interface.
//
// Create an instance with [NewPostgres]. The caller owns the pool's
// lifecycle and must close it when done.
type Postgres struct {
	pool          *pgxpool.Pool
	partitionSpan int64

	// Health monitoring
	healthMu      sync.RWMutex
	healthyAtomic atomic.Bool
	lastHealthErr error
	databaseURL   string
	stopHealthCk  context.CancelFunc
}

// Compile-time assertion that *Postgres implements Store.
var _ Store = (*Postgres)(nil)

// NewPostgres wraps an existing pool. The caller owns the pool's lifecycle.
func NewPostgres(pool *pgxpool.Pool, partitionSpan ...int64) *Postgres {
	span := int64(DefaultEventPartitionSpan)
	if len(partitionSpan) > 0 && partitionSpan[0] > 0 {
		span = partitionSpan[0]
	}
	pg := &Postgres{pool: pool, partitionSpan: span}
	pg.healthyAtomic.Store(true)
	return pg
}

// NewPostgresWithHealthCheck creates a Postgres instance with health monitoring.
// It wraps the pool and starts a periodic health check goroutine that will
// automatically reconnect on transient failures. The context is used to stop
// the health check; callers should ensure it's cancelled on shutdown.
func NewPostgresWithHealthCheck(ctx context.Context, pool *pgxpool.Pool, databaseURL string, partitionSpan ...int64) *Postgres {
	pg := NewPostgres(pool, partitionSpan...)
	pg.databaseURL = databaseURL

	healthCtx, cancel := context.WithCancel(ctx)
	pg.stopHealthCk = cancel

	go pg.runHealthCheck(healthCtx)

	return pg
}

// StopHealthCheck stops the health check goroutine. The caller is responsible for
// closing the underlying pool.
func (p *Postgres) StopHealthCheck() {
	if p.stopHealthCk != nil {
		p.stopHealthCk()
	}
}

func (p *Postgres) runHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(poolHealthCheckInterval)
	defer ticker.Stop()

	logger := slog.Default()
	reconnectAttempt := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck(ctx, logger, &reconnectAttempt)
		}
	}
}

func (p *Postgres) performHealthCheck(ctx context.Context, logger *slog.Logger, reconnectAttempt *int) {
	pingCtx, cancel := context.WithTimeout(ctx, poolHealthCheckTimeout)
	defer cancel()

	err := p.pool.Ping(pingCtx)

	p.healthMu.Lock()
	wasHealthy := p.healthyAtomic.Load()
	p.healthMu.Unlock()

	if err == nil {
		if !wasHealthy {
			p.healthMu.Lock()
			p.healthyAtomic.Store(true)
			p.lastHealthErr = nil
			*reconnectAttempt = 0
			p.healthMu.Unlock()
			logger.Info("postgres pool recovered")
		}
		return
	}

	p.healthMu.Lock()
	p.lastHealthErr = err
	if wasHealthy {
		p.healthyAtomic.Store(false)
		p.healthMu.Unlock()
		logger.Warn("postgres pool unhealthy, starting reconnect attempts",
			"error", err,
		)
	} else {
		p.healthMu.Unlock()
	}

	*reconnectAttempt++

	backoff := time.Duration(*reconnectAttempt) * poolReconnectBaseBackoff
	if backoff > poolReconnectMaxBackoff {
		backoff = poolReconnectMaxBackoff
	}

	logger.Warn("postgres pool health check failed, attempting reconnect",
		"attempt", *reconnectAttempt,
		"backoff", backoff,
		"error", err,
	)

	time.Sleep(backoff)

	if err := p.attemptReconnect(ctx, logger); err != nil {
		logger.Warn("postgres pool reconnect failed, will retry",
			"attempt", *reconnectAttempt,
			"error", err,
		)
		return
	}

	p.healthMu.Lock()
	p.healthyAtomic.Store(true)
	p.lastHealthErr = nil
	*reconnectAttempt = 0
	p.healthMu.Unlock()
	logger.Info("postgres pool reconnected successfully",
		"attempts", *reconnectAttempt,
	)
}

func (p *Postgres) attemptReconnect(ctx context.Context, logger *slog.Logger) error {
	if p.databaseURL == "" {
		return fmt.Errorf("database URL not available for reconnection")
	}

	newPool, err := pgxpool.New(ctx, p.databaseURL)
	if err != nil {
		return fmt.Errorf("creating new pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, poolHealthCheckTimeout)
	defer cancel()

	if err := newPool.Ping(pingCtx); err != nil {
		newPool.Close()
		return fmt.Errorf("pinging new pool: %w", err)
	}

	p.healthMu.Lock()
	oldPool := p.pool
	p.pool = newPool
	p.healthMu.Unlock()
	oldPool.Close()

	return nil
}

func (p *Postgres) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	return p.upsertEvents(ctx, events, false)
}

func (p *Postgres) PruneEventsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := p.pool.Exec(ctx, `DELETE FROM events WHERE created_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("pruning events before %s: %w", cutoff.Format(time.RFC3339Nano), err)
	}
	return tag.RowsAffected(), nil
}

// insertEventsBatch builds the single batch used by upsertEvents (idempotent
// ingest, DO NOTHING) and ReplaceEventsInRange (auditor repair, DO UPDATE).
// Both paths write the full events row so raw XDR is never silently dropped
// on the way in; a repair that arrives without XDR preserves what was already
// stored via the coalesce() clauses in the UPDATE branch (`sorotrail replay`
// relies on that).
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
	stmt := `
		INSERT INTO events
			(id, network, contract_id, ledger, type, tx_hash, tx_index, op_index,
			 in_successful_call, topics, value, created_at,
			 raw_topic_xdr, raw_value_xdr)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		` + conflict
	batch := &pgx.Batch{}
	for _, e := range events {
		// 14 placeholders → 14 args. nullable helpers turn empty raw XDR
		// into SQL NULL so the column has one representation of "absent"
		// rather than two.
		batch.Queue(stmt,
			e.ID, defaultNetwork(e.Network), e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
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

func (p *Postgres) upsertEvents(ctx context.Context, events []Event, onUpdate bool) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	if err := p.ensureEventPartitions(ctx, events); err != nil {
		return 0, err
	}
	results := p.pool.SendBatch(ctx, insertEventsBatch(events, onUpdate))
	defer results.Close()

	var inserted int64
	for range events {
		tag, err := results.Exec()
		if err != nil {
			return 0, fmt.Errorf("upserting events: %w", err)
		}
		if tag.RowsAffected() > 0 {
			inserted++
		}
	}
	return inserted, nil
}

func (p *Postgres) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	if len(events) == 0 {
		return nil
	}
	timer := prometheus.NewTimer(metrics.DBWriteLatency)
	defer timer.ObserveDuration()

	if err := p.ensureEventPartitions(ctx, events); err != nil {
		return err
	}
	// Need a network for the delete — all events in a batch share the same network.
	network := events[0].Network
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The delete below drops the rows the ON CONFLICT clause would otherwise
	// have preserved raw XDR from, so snapshot it first. A repair fetch that
	// came back without XDR must not cost surviving events their
	// replayability (see internal/replay).
	kept, err := rawXDRInRange(ctx, tx, network, fromLedger, toLedger)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM events WHERE network = $1 AND ledger BETWEEN $2 AND $3`, network, fromLedger, toLedger); err != nil {
		return fmt.Errorf("deleting events in ledger range [%d,%d] for network %s: %w", fromLedger, toLedger, network, err)
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

// rawXDRInRange reads the raw XDR currently stored for a ledger range,
// keyed by event ID, so a delete-and-reinsert repair can put it back.
func rawXDRInRange(ctx context.Context, tx pgx.Tx, network string, fromLedger, toLedger int64) (map[string]rawXDR, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, raw_topic_xdr, raw_value_xdr
		FROM events
		WHERE network = $1 AND ledger BETWEEN $2 AND $3
		  AND (raw_topic_xdr IS NOT NULL OR raw_value_xdr IS NOT NULL)`,
		network, fromLedger, toLedger)
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

// LedgerRangeCensus returns one row per ledger that holds at least one
// event in the inclusive [fromLedger, toLedger] range. idsOnly=false →
// counts only (cheap path for the common "all good" sweep); idsOnly=true
// → per-ledger sorted ID list (used to diff a ledger whose count
// disagrees with the RPC).
func (p *Postgres) ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}
	if !ValidContractsSortKey(f.SortKey) {
		return nil, "", fmt.Errorf("unsupported sort_key %q", f.SortKey)
	}
	orderDir := "DESC"
	cursorOp := "<"
	sortCol, sortExpr, sortType := contractsSortParts(f.SortKey)
	// contract_id is the documented default listing order and reads
	// naturally ascending; activity-style keys keep their historical
	// "busiest first" descending default.
	if strings.EqualFold(f.Order, "asc") ||
		(f.Order == "" && sortCol == "contract_id") {
		orderDir = "ASC"
		cursorOp = ">"
	}
	orderBy := fmt.Sprintf("%s %s, contract_id %s", sortExpr, orderDir, orderDir)
	if sortCol == "contract_id" {
		// Sorting by the tiebreaker itself: a single term says it all.
		orderBy = fmt.Sprintf("contract_id %s", orderDir)
	}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where := []string{}
	if f.ContractIDPrefix != "" {
		where = append(where, "contract_id LIKE "+arg(f.ContractIDPrefix+"%"))
	}
	if f.Cursor != "" {
		sortValue, contractID, err := DecodeContractsCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		typed := arg(sortValue)
		switch sortType {
		case "bigint":
			typed += "::bigint"
		case "timestamptz":
			typed += "::timestamptz"
		}
		where = append(where,
			fmt.Sprintf("(%s, contract_id) %s (%s, %s)",
				sortCol, cursorOp, typed, arg(contractID)))
	}
	query := fmt.Sprintf(`
		SELECT contract_id, count(*) AS event_count,
		       min(ledger) AS first_ledger, max(ledger) AS last_ledger,
		       max(created_at) AS last_seen
		FROM events
		%s
		GROUP BY contract_id
		ORDER BY %s
		LIMIT %d`,
		contractsWhere(where),
		orderBy,
		limit+1,
	)
	var out []ContractSummary
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("listing contracts: %w", err)
		}
		defer rows.Close()
		out = make([]ContractSummary, 0, limit)
		for rows.Next() {
			var c ContractSummary
			if err := rows.Scan(&c.ContractID, &c.EventCount,
				&c.FirstLedger, &c.LastLedger, &c.LastSeen); err != nil {
				return err
			}
			out = append(out, c)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reading contracts: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next = EncodeContractsCursor(f.SortKey, contractSortValue(last, f.SortKey), last.ContractID)
	}
	return out, next, nil
}

// GetContractSummary returns a single contract's summary row. It queries
// the events table directly so it reflects the same data as ListContracts.
func (p *Postgres) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	var c ContractSummary
	err := p.pool.QueryRow(ctx,
		`SELECT contract_id,
		        COUNT(*)::bigint,
		        MIN(ledger)::bigint,
		        MAX(ledger)::bigint,
		        MAX(created_at)
		   FROM events
		  WHERE contract_id = $1
		  GROUP BY contract_id`, contractID,
	).Scan(&c.ContractID, &c.EventCount, &c.FirstLedger, &c.LastLedger, &c.LastSeen)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c, ErrNotFound
		}
		return c, fmt.Errorf("get contract summary %s: %w", contractID, err)
	}
	return c, nil
}

// ContractEventTypeCounts returns per-type event counts for a single
// contract, ordered by count descending.
func (p *Postgres) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT type, COUNT(*)::bigint
		   FROM events
		  WHERE contract_id = $1
		  GROUP BY type
		  ORDER BY count DESC`, contractID)
	if err != nil {
		return nil, fmt.Errorf("contract event type counts %s: %w", contractID, err)
	}
	defer rows.Close()

	var out []ContractEventTypeCount
	for rows.Next() {
		var c ContractEventTypeCount
		if err := rows.Scan(&c.Type, &c.Count); err != nil {
			return nil, fmt.Errorf("scanning contract event type count: %w", err)
		}
		out = append(out, c)
	}
	if out == nil {
		out = []ContractEventTypeCount{}
	}
	return out, rows.Err()
}

func (p *Postgres) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where := []string{}
	if f.ContractIDPrefix != "" {
		where = append(where, "contract_id LIKE "+arg(f.ContractIDPrefix+"%"))
	}
	q := `SELECT count(*) FROM (SELECT contract_id FROM events ` + contractsWhere(where) + ` GROUP BY contract_id) AS sub`
	var total int64
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, q, args...).Scan(&total)
	})
	if err != nil {
		return 0, fmt.Errorf("counting contracts: %w", err)
	}
	return total, nil
}

func ValidContractsSortKey(s string) bool {
	switch s {
	case "", SortByContractID, SortByActivity, SortByFirstLedger, SortByLastLedger, SortByLastSeen:
		return true
	}
	return false
}

func contractsSortParts(sortKey string) (col, expr, typ string) {
	switch sortKey {
	case "", SortByContractID:
		return "contract_id", "contract_id", ""
	case SortByFirstLedger:
		return "min(ledger)", "min(ledger)", "bigint"
	case SortByLastLedger:
		return "max(ledger)", "max(ledger)", "bigint"
	case SortByLastSeen:
		return "max(created_at)", "max(created_at)", "timestamptz"
	default:
		return "count(*)", "count(*)", ""
	}
}

func contractSortValue(c ContractSummary, sortKey string) string {
	switch sortKey {
	case "", SortByContractID:
		return c.ContractID
	case SortByFirstLedger:
		return fmt.Sprint(c.FirstLedger)
	case SortByLastLedger:
		return fmt.Sprint(c.LastLedger)
	case SortByLastSeen:
		return c.LastSeen.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(c.EventCount)
	}
}

func contractsWhere(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(parts, " AND ")
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

func (p *Postgres) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	// A scope that grants nothing cannot match a row; skip the round trip
	// and report the same ErrNotFound the query would have produced, so
	// the two paths are indistinguishable to a caller probing for events.
	if sc.DeniesAll() {
		return Event{}, ErrNotFound
	}
	query := `SELECT ` + eventColumns + ` FROM events WHERE id = $1`
	args := []any{id}
	if !sc.IsWildcard() {
		query += ` AND contract_id = ANY($2)`
		args = append(args, sc.Contracts())
	}
	var e Event
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, query, args...)
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

// GetEventsByTxHash returns all events emitted by the transaction
// identified by txHash, excluding the event with id excludeID (when
// non-empty). Events are returned in ascending ID order.
func (p *Postgres) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	query := `SELECT ` + eventColumns + ` FROM events WHERE tx_hash = $1`
	args := []any{txHash}
	if excludeID != "" {
		query += ` AND id != $2`
		args = append(args, excludeID)
	}
	query += ` ORDER BY id ASC`

	var events []Event
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("querying events by tx hash: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			e, err := scanEvent(rows)
			if err != nil {
				return err
			}
			events = append(events, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

// EventExists is the cheap 304 path used by the API's conditional GET:
// an index-only probe against the primary key that never deserializes
// the row, so it stays index-bound even on a wide row. Existence is a
// boolean because 304 responses carry no body — there's nothing to
// serialize. The call is what backs the "still here" check after a
// cache hit so that retention/pruning (#8) cannot leave a cache
// pretending a deleted event is still around; the full row is only
// loaded on a real cache miss via GetEvent.
func (p *Postgres) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	if sc.DeniesAll() {
		return false, nil
	}
	query := `SELECT 1 FROM events WHERE id = $1`
	args := []any{id}
	if !sc.IsWildcard() {
		query += ` AND contract_id = ANY($2)`
		args = append(args, sc.Contracts())
	}
	var one int
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, args...).Scan(&one)
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
//
// Callers must reject a denying scope before calling this: an empty scope
// produces `contract_id = ANY('{}')`, which is correct but still issues SQL.
func buildEventWhereClause(f EventFilter) ([]string, []any) {
	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if f.Network != "" {
		where = append(where, "network = "+arg(defaultNetwork(f.Network)))
	}
	// Scope is appended first so it is present in every generated
	// statement, including ones where a later filter returns early. It is
	// ANDed with the caller's filters, never ORed, so widening the request
	// cannot widen the authorization.
	if !f.Scope.IsWildcard() {
		where = append(where, "contract_id = ANY("+arg(f.Scope.Contracts())+")")
	}
	// When ContractIDs is non-empty, use = ANY(...) (SQL IN). When both
	// ContractID and ContractIDs are set, combine them via a single
	// = ANY(...) that includes both — the union matches an event for either.
	if len(f.ContractIDs) > 0 {
		ids := f.ContractIDs
		if f.ContractID != "" {
			// Deduplicate: ContractID might already be in ContractIDs.
			has := false
			for _, id := range ids {
				if id == f.ContractID {
					has = true
					break
				}
			}
			if !has {
				ids = append(ids, f.ContractID)
			}
		}
		where = append(where, "contract_id = ANY("+arg(ids)+")")
	} else if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if f.ContractIDPrefix != "" {
		where = append(where, "contract_id LIKE "+arg(f.ContractIDPrefix+"%"))
	}
	if len(f.Types) > 0 {
		where = append(where, "type = ANY("+arg(f.Types)+")")
	}
	if f.TxHash != "" {
		where = append(where, "tx_hash = "+arg(f.TxHash))
	}
	if f.InSuccessfulCall != nil {
		where = append(where, "in_successful_call = "+arg(*f.InSuccessfulCall))
	}
	if len(f.Topic) > 0 {
		where = append(where, "topics @> "+arg(fmt.Sprintf("[%s]", f.Topic))+"::jsonb")
	}
	if len(f.TopicContains) > 0 {
		// Direct containment — caller controls the shape (object wrapped in
		// array for element match, multi-element arrays for subset match).
		where = append(where, "topics @> "+arg(string(f.TopicContains))+"::jsonb")
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where,
			fmt.Sprintf("topics->%d = %s::jsonb", i, arg(topic)))
	}
	if f.HasValue != nil {
		if *f.HasValue {
			where = append(where, "value IS NOT NULL")
		} else {
			where = append(where, "value IS NULL")
		}
	}
	if f.TxIndex != nil {
		where = append(where, "tx_index = "+arg(*f.TxIndex))
	}
	if f.OpIndex != nil {
		where = append(where, "op_index = "+arg(*f.OpIndex))
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
	// The tenant boundary is evaluated before anything else, and an empty
	// scope returns an empty page without issuing SQL. No user-supplied
	// filter is consulted first, so no filter can influence whether the
	// boundary is applied.
	if f.Scope.DeniesAll() {
		return nil, "", nil
	}

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

// AggregateEvents returns event counts grouped by ledger or by a
// time interval. Buckets with zero events are omitted. Filters
// (contract_id, type, etc.) are applied to the aggregation query.
func (p *Postgres) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	f.Cursor = ""
	f.Order = ""
	f.OrderBy = ""
	f.Limit = 0

	where, args := buildEventWhereClause(f)

	var (
		groupCol  string
		bucketCol string
	)

	switch bucket {
	case "ledger":
		groupCol = "ledger"
		bucketCol = "ledger::text"
	default:
		dur, err := time.ParseDuration(bucket)
		if err != nil {
			return nil, fmt.Errorf("invalid bucket %q: %w", bucket, err)
		}
		if dur <= 0 {
			return nil, fmt.Errorf("bucket duration must be positive")
		}
		bucketSeconds := int64(dur.Seconds())
		groupCol = fmt.Sprintf("to_timestamp(floor(extract(epoch from created_at) / %d) * %d)", bucketSeconds, bucketSeconds)
		bucketCol = fmt.Sprintf("to_char(%s, 'YYYY-MM-DD\"T\"HH24:MI:SS')", groupCol)
	}

	query := "SELECT " + bucketCol + " AS bucket, count(*)::bigint FROM events"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY " + groupCol + " ORDER BY " + groupCol

	var buckets []AggregateBucket
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("aggregating events: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var b AggregateBucket
			if err := rows.Scan(&b.Bucket, &b.Count); err != nil {
				return err
			}
			buckets = append(buckets, b)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("reading aggregation: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

// CountEvents returns the total number of rows matching the filter,
// ignoring pagination (cursor, order, and limit). It reuses the same
// WHERE clause builder as QueryEvents so the two stay in lockstep.
func (p *Postgres) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	// Same boundary as QueryEvents, evaluated before any filter: a count is
	// an aggregate, and an unscoped one tells a tenant how much data exists
	// outside its grants even though it can read none of it.
	if f.Scope.DeniesAll() {
		return 0, nil
	}

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
	return p.GetIngestionStateForNetwork(ctx, "default")
}

func (p *Postgres) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	var s IngestionState
	network = defaultNetwork(network)
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT network, last_ingested_ledger, last_cursor, last_successful_poll, updated_at FROM ingestion_state WHERE network = $1`, network,
		).Scan(&s.Network, &s.LastIngestedLedger, &s.LastCursor, &s.LastSuccessfulPoll, &s.UpdatedAt)
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
	if s.Network == "" {
		s.Network = "default"
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, last_successful_poll, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (network) DO UPDATE SET
			last_ingested_ledger = EXCLUDED.last_ingested_ledger,
			last_cursor          = EXCLUDED.last_cursor,
			last_successful_poll = EXCLUDED.last_successful_poll,
			updated_at           = now()`,
		s.Network, s.LastIngestedLedger, s.LastCursor, s.LastSuccessfulPoll,
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

// watchedContractsUnion is the set of contracts ingestion must follow: the
// operator's own list plus every tenant's requests. UNION (not UNION ALL)
// deduplicates, so two tenants watching the same contract produce one entry
// and therefore one set of ingested rows that both of them read.
//
// This is also the whole of the removal-refcounting story. There is no
// counter to keep consistent: the union is recomputed on read, so a
// contract is watched for exactly as long as at least one row somewhere
// still names it, and one tenant dropping its claim cannot stop ingestion
// for another that still holds one.
// added_at is aggregated rather than carried through the set operation:
// once the timestamp is in the projection, two tenants who claimed the same
// contract at different moments are distinct rows and a plain UNION stops
// deduplicating. Grouping restores one row per contract, and min() reads as
// "watched since" — the earliest claim is what ingestion has actually been
// following.
const watchedContractsUnion = `
	SELECT contract_id, min(added_at) AS added_at FROM (
		SELECT contract_id, added_at FROM watched_contracts
		UNION ALL
		SELECT contract_id, added_at FROM tenant_watched_contracts
	) w GROUP BY contract_id`

func (p *Postgres) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	rows, err := p.pool.Query(ctx,
		watchedContractsUnion+` ORDER BY contract_id`)
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

// GetContractCursor returns the stored resume position for one watched
// contract, or ErrNotFound when the contract has no cursor row yet.
func (p *Postgres) GetContractCursor(ctx context.Context, contractID string) (ContractCursor, error) {
	var (
		c  ContractCursor
		ts time.Time
	)
	err := p.pool.QueryRow(ctx,
		`SELECT contract_id, last_ingested_ledger, last_cursor, updated_at
		 FROM contract_cursors WHERE contract_id = $1`, contractID,
	).Scan(&c.ContractID, &c.LastIngestedLedger, &c.LastCursor, &ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return ContractCursor{}, ErrNotFound
	}
	if err != nil {
		return ContractCursor{}, fmt.Errorf("loading cursor for contract %s: %w", contractID, err)
	}
	c.UpdatedAt = ts
	return c, nil
}

// SaveContractCursor upserts one contract's resume position.
func (p *Postgres) SaveContractCursor(ctx context.Context, c ContractCursor) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO contract_cursors (contract_id, last_ingested_ledger, last_cursor, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (contract_id) DO UPDATE SET
			last_ingested_ledger = EXCLUDED.last_ingested_ledger,
			last_cursor          = EXCLUDED.last_cursor,
			updated_at           = now()`, c.ContractID, c.LastIngestedLedger, c.LastCursor)
	if err != nil {
		return fmt.Errorf("saving cursor for contract %s: %w", c.ContractID, err)
	}
	return nil
}

// DeleteContractCursor removes a contract's cursor row (used when a
// contract leaves the watch list).
func (p *Postgres) DeleteContractCursor(ctx context.Context, contractID string) error {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM contract_cursors WHERE contract_id = $1`, contractID)
	if err != nil {
		return fmt.Errorf("deleting cursor for contract %s: %w", contractID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListContractCursors returns every tracked per-contract resume position.
func (p *Postgres) ListContractCursors(ctx context.Context) ([]ContractCursor, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT contract_id, last_ingested_ledger, last_cursor, updated_at FROM contract_cursors`)
	if err != nil {
		return nil, fmt.Errorf("listing contract cursors: %w", err)
	}
	defer rows.Close()
	var out []ContractCursor
	for rows.Next() {
		var (
			c  ContractCursor
			ts time.Time
		)
		if err := rows.Scan(&c.ContractID, &c.LastIngestedLedger, &c.LastCursor, &ts); err != nil {
			return nil, fmt.Errorf("scanning contract cursor: %w", err)
		}
		c.UpdatedAt = ts
		out = append(out, c)
	}
	return out, rows.Err()
}

// Stats aggregates within sc. The event-derived counters (total, oldest
// ledger, distinct contracts, watched contracts) are restricted to the
// caller's contracts; the ingestion and audit frontiers are not, because
// they describe the instance's progress rather than anyone's data and are
// already visible through /health.
func (p *Postgres) Stats(ctx context.Context, sc Scope) (Stats, error) {
	if sc.DeniesAll() {
		// Frontier values are still reported: they leak nothing about
		// other tenants' rows, and zeroing them would make a scoped /stats
		// look like a broken instance rather than an empty one.
		return p.frontierStats(ctx)
	}
	if sc.IsWildcard() {
		return p.statsWhere(ctx, "", nil)
	}
	return p.statsWhere(ctx, "WHERE contract_id = ANY($1)", []any{sc.Contracts()})
}

// statsWhere runs the stats aggregate with an optional predicate applied to
// every events-derived subquery. The predicate is a constant string chosen
// by Stats — never caller input — with values passed as bind parameters.
func (p *Postgres) statsWhere(ctx context.Context, pred string, args []any) (Stats, error) {
	var s Stats
	// COUNT(DISTINCT contract_id) scans the contract_id index; fine at MVP
	// scale. contributors: replace with a maintained summary table if it
	// becomes a bottleneck on large datasets.
	//
	// The watch-list count runs through the same predicate — the union
	// subquery exposes a contract_id column, so the identical WHERE clause
	// applies — keeping a tenant's view of "how many contracts is this
	// instance following" limited to the ones it can read.
	query := fmt.Sprintf(`
		SELECT
			(SELECT count(*) FROM events %[1]s),
			(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
			(SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state),
			(SELECT coalesce(min(ledger), 0) FROM events %[1]s),
			(SELECT count(DISTINCT contract_id) FROM events %[1]s),
			(SELECT count(*) FROM (%[2]s) w %[1]s),
			%[3]s`,
		pred, watchedContractsUnion, tableSizeExpr(pred == ""))
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, query, args...).Scan(
			&s.TotalEvents, &s.LastIngestedLedger, &s.VerifiedThroughLedger,
			&s.OldestStoredLedger, &s.ContractCount, &s.WatchedContracts,
			&s.TableSizeBytes)
	})
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return s, nil
}

// DeleteEventsBeforeLedger deletes all events with a ledger strictly less than
// the given ledger number. Returns the number of rows deleted.
func (p *Postgres) DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error) {
	tag, err := p.pool.Exec(ctx, `DELETE FROM events WHERE ledger < $1`, beforeLedger)
	if err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", beforeLedger, err)
	}
	return tag.RowsAffected(), nil
}

// MigrationVersion returns the currently applied migration version by querying
// the schema_migrations table. When the table does not exist or returns no rows,
// it returns (0, false, nil).
func (p *Postgres) MigrationVersion(ctx context.Context) (version int, dirty bool, err error) {
	err = p.pool.QueryRow(ctx,
		`SELECT version, dirty FROM schema_migrations`,
	).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil // no migrations applied yet
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading migration version: %w", err)
	}
	return version, dirty, nil
}

// tableSizeExpr returns the SELECT expression for events table size on
// disk. unscoped is true only for the wildcard caller (Stats passes an
// empty predicate for it). The figure covers every partition, so handing
// it to a tenant would disclose the instance's total data volume — the
// same leak the scoped counts above exist to prevent.
func tableSizeExpr(unscoped bool) string {
	if !unscoped {
		return "0::bigint"
	}
	return `(SELECT coalesce(sum(pg_total_relation_size(inhrelid)), 0)
			 FROM pg_inherits WHERE inhparent = 'events'::regclass)`
}

// frontierStats reports only the instance-progress counters, for a caller
// whose scope matches no rows at all.
func (p *Postgres) frontierStats(ctx context.Context) (Stats, error) {
	var s Stats
	err := p.pool.QueryRow(ctx, `
		SELECT
			(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
			(SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state)`,
	).Scan(&s.LastIngestedLedger, &s.VerifiedThroughLedger)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return s, nil
}

// CountEventsBefore counts events strictly below maxLedger and (if
// beforeTime is non-zero) older than beforeTime. The count is limited
// by limit, matching DeleteEventsBefore's batching contract.
func (p *Postgres) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	var where string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where = "ledger < " + arg(maxLedger)
	if !beforeTime.IsZero() {
		where += " AND created_at < " + arg(beforeTime)
	}
	q := fmt.Sprintf(`SELECT count(*) FROM (SELECT 1 FROM events WHERE %s LIMIT %d) sub`, where, limit)
	var total int64
	err := p.pool.QueryRow(ctx, q, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting events before ledger %d: %w", maxLedger, err)
	}
	return total, nil
}

// DeleteEventsBefore deletes up to limit events that are strictly below
// maxLedger and (if beforeTime is non-zero) older than beforeTime. It is
// designed for the background pruner and intentionally never touches rows
// at or above maxLedger, which must be ≤ last_ingested_ledger.
//
// When beforeTime is zero the time clause is omitted — deletion is based
// solely on ledger, which is useful when RETENTION_MIN_LEDGER is set.
//
// The limit prevents a single DELETE from holding a long lock. The caller
// should loop with a pause between calls until the return is < limit.
//
// Implementation note: PostgreSQL's DELETE grammar has no LIMIT clause
// (it is a MySQL extension). The capped set of rows is picked by an inner
// SELECT id … LIMIT and then deleted by id. The id-based outer DELETE
// also keeps the FK ON DELETE CASCADE story simple for the future
// `token_events` table — Postgres cascades from the outer DELETE using
// real row identities, not a DELETE result set, so dependent rows come
// along for free once that table lands.
func (p *Postgres) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	var where string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	// Always scope by ledger: never delete at or above maxLedger.
	where = "ledger < " + arg(maxLedger)

	if !beforeTime.IsZero() {
		where += " AND created_at < " + arg(beforeTime)
	}

	q := fmt.Sprintf(`DELETE FROM events WHERE id IN (SELECT id FROM events WHERE %s LIMIT %d)`, where, limit)
	tag, err := p.pool.Exec(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", maxLedger, err)
	}
	return tag.RowsAffected(), nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	p.healthMu.RLock()
	defer p.healthMu.RUnlock()

	if !p.healthyAtomic.Load() {
		if p.lastHealthErr != nil {
			return fmt.Errorf("pool unhealthy: %w", p.lastHealthErr)
		}
		return errors.New("pool unhealthy")
	}

	pool := p.pool
	return pool.Ping(ctx)
}

// AdvisoryLockKey hashes an RPC URL into an int64 suitable for use with
// Postgres advisory locks. Different RPC URLs produce different keys with
// high probability, ensuring two deployments targeting different networks
// (or different RPC providers) never contend on the same lock.
func AdvisoryLockKey(rpcURL string) int64 {
	h := fnv.New64a()
	h.Write([]byte(rpcURL))
	// Fold the uint64 into int64, keeping the full entropy of the hash.
	return int64(h.Sum64())
}

// TryAdvisoryLock attempts to acquire a session-level Postgres advisory
// lock keyed by the given int64. It acquires a dedicated connection from
// the pool so the lock persists for the lifetime of the returned
// connection. When acquired=true the caller must call conn.Release() to
// release both the lock and the connection back to the pool.
//
// Returns acquired=false when another session already holds the lock on
// this key — the caller should skip ingestion but may continue serving.
func (p *Postgres) TryAdvisoryLock(ctx context.Context, key int64) (conn *pgxpool.Conn, acquired bool, err error) {
	conn, err = p.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquiring connection for advisory lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("pg_try_advisory_lock(%d): %w", key, err)
	}
	if !locked {
		conn.Release()
		return nil, false, nil
	}
	return conn, true, nil
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
			(network, from_ledger, to_ledger, expected_count, actual_count,
			 missing_ids, status, attempts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 0)
		RETURNING id, created_at`,
		f.Network, f.FromLedger, f.ToLedger, f.ExpectedCount, f.ActualCount,
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

// ListOpenFindingsByRange returns the most recent finding whose range
// overlaps [fromLedger, toLedger] and is still in a working state (open
// or unrecoverable). ErrNotFound means no live finding spans the range.
func (p *Postgres) ListOpenFindingsByRange(ctx context.Context, network string, fromLedger, toLedger int64) (AuditFinding, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT id, network, from_ledger, to_ledger, expected_count, actual_count,
		       missing_ids, status, attempts, last_attempted_at, last_error, created_at
		FROM audit_findings
		WHERE network = $1
		  AND status IN ('open', 'unrecoverable')
		  AND from_ledger <= $3
		  AND to_ledger   >= $2
		ORDER BY id DESC
		LIMIT 1`,
		network, fromLedger, toLedger,
	)
	var f AuditFinding
	var lastAttempted *time.Time
	err := row.Scan(&f.ID, &f.Network, &f.FromLedger, &f.ToLedger, &f.ExpectedCount, &f.ActualCount,
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

// DeadLetterEvent records a poison event into the dead_letters table.
// Re-submitting the same event ID is treated as a retry: the existing
// row's attempts counter is incremented, last_attempt and error
// columns are updated, and the row's raw payload is overwritten with
// the most recent attempt (the latest error message is the most
// useful context for debugging).
func (p *Postgres) DeadLetterEvent(ctx context.Context, in DeadLetterInput) (DeadLetter, error) {
	errStr := ""
	if in.Err != nil {
		errStr = in.Err.Error()
	}
	var d DeadLetter
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO dead_letters
			    (event_id, contract_id, ledger, type, tx_hash,
			     topic_xdr, value_xdr, error, attempts, last_attempt)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, now())
			ON CONFLICT (event_id) DO UPDATE SET
			    error        = EXCLUDED.error,
			    attempts     = dead_letters.attempts + 1,
			    last_attempt = now(),
			    topic_xdr    = EXCLUDED.topic_xdr,
			    value_xdr    = EXCLUDED.value_xdr
			RETURNING id, event_id, contract_id, ledger, type, tx_hash,
			          topic_xdr, value_xdr, error, attempts, last_attempt, created_at`,
			in.EventID, in.ContractID, in.Ledger, in.Type, nullableText(in.TxHash),
			nullableStringSlice(in.TopicXDR), nullableText(in.ValueXDR), errStr,
		)
		var txHash *string
		var valueXDR *string
		scanErr := row.Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger, &d.Type, &txHash,
			&d.TopicXDR, &valueXDR, &d.Error, &d.Attempts, &d.LastAttempt, &d.CreatedAt)
		if scanErr != nil {
			return scanErr
		}
		if txHash != nil {
			d.TxHash = *txHash
		}
		if valueXDR != nil {
			d.ValueXDR = *valueXDR
		}
		return nil
	})
	if err != nil {
		// The ON CONFLICT clause targets event_id, and the migration
		// adds a UNIQUE constraint on event_id so the retry path
		// emits a row-level conflict instead of crashing.
		return DeadLetter{}, fmt.Errorf("recording dead letter: %w", err)
	}
	return d, nil
}

// ListDeadLetters returns dead-letter rows newest-first. contractID ""
// means all contracts; limit==0 means DefaultQueryLimit.
//
// Pagination is keyset by id (the bigserial primary key). The cursor
// is the encoded id of the last row from the previous page; passing ""
// resumes from the newest.
func (p *Postgres) ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error) {
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}
	args := []any{}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where := []string{}
	if contractID != "" {
		where = append(where, "contract_id = "+arg(contractID))
	}
	if cursor != "" {
		sv, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("%w: dead letter cursor", ErrInvalidContractsCursor)
		}
		idIdx := len(args) + 1
		args = append(args, string(sv))
		where = append(where, fmt.Sprintf("dead_letters.id < $%d", idIdx))
	}
	query := `SELECT id, event_id, contract_id, ledger, type, tx_hash,
	                 topic_xdr, value_xdr, error, attempts, last_attempt, created_at
	          FROM dead_letters`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id DESC LIMIT " + arg(limit+1)
	var rows []DeadLetter
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		r, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("listing dead letters: %w", err)
		}
		defer r.Close()
		for r.Next() {
			var d DeadLetter
			var txh *string
			var vxdr *string
			if err := r.Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger,
				&d.Type, &txh, &d.TopicXDR, &vxdr, &d.Error, &d.Attempts,
				&d.LastAttempt, &d.CreatedAt); err != nil {
				return err
			}
			if txh != nil {
				d.TxHash = *txh
			}
			if vxdr != nil {
				d.ValueXDR = *vxdr
			}
			rows = append(rows, d)
		}
		return r.Err()
	})
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(rows) > limit {
		last := rows[limit-1]
		rows = rows[:limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprint(last.ID)))
	}
	return rows, next, nil
}

// CountDeadLetters returns the total number of dead-letter rows matching
// the same contract filter as ListDeadLetters ("" means all contracts).
// Pagination is deliberately ignored: the count is the full match set
// behind any page, so a client can show a total without walking pages.
func (p *Postgres) CountDeadLetters(ctx context.Context, contractID string) (int64, error) {
	var total int64
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		if contractID != "" {
			return tx.QueryRow(ctx,
				`SELECT count(*) FROM dead_letters WHERE contract_id = $1`,
				contractID,
			).Scan(&total)
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM dead_letters`).Scan(&total)
	})
	if err != nil {
		return 0, fmt.Errorf("counting dead letters: %w", err)
	}
	return total, nil
}

func (p *Postgres) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error) {
	var d DeadLetter
	var txHash *string
	var valueXDR *string
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT id, event_id, contract_id, ledger, type, tx_hash,
			       topic_xdr, value_xdr, error, attempts, last_attempt, created_at
			FROM dead_letters WHERE id = $1`,
			id,
		).Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger, &d.Type, &txHash,
			&d.TopicXDR, &valueXDR, &d.Error, &d.Attempts, &d.LastAttempt, &d.CreatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DeadLetter{}, ErrNotFound
	}
	if err != nil {
		return DeadLetter{}, fmt.Errorf("loading dead letter %d: %w", id, err)
	}
	if txHash != nil {
		d.TxHash = *txHash
	}
	if valueXDR != nil {
		d.ValueXDR = *valueXDR
	}
	return d, nil
}

func (p *Postgres) DeleteDeadLetter(ctx context.Context, id int64) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM dead_letters WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting dead letter %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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
		e Event
		// raw_value_xdr is nullable and Event.RawValueXDR is a plain
		// string, so it has to land in a pointer first - the same shape
		// rawXDRInRange uses. raw_topic_xdr needs no such care: a NULL
		// text[] scans into a nil slice.
		rawValueXDR *string
	)
	// The destinations must line up 1:1 with eventColumns, which ends in
	// raw_topic_xdr and raw_value_xdr; omitting them here makes pgx reject
	// every row with "number of field descriptions must equal number of
	// destinations".
	err := row.Scan(&e.Network, &e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash,
		&e.TxIndex, &e.OpIndex, &e.InSuccessfulCall, &e.Topics, &e.Value,
		&e.CreatedAt, &e.RawTopicXDR, &rawValueXDR)
	if err != nil {
		return Event{}, err
	}
	if rawValueXDR != nil {
		e.RawValueXDR = *rawValueXDR
	}
	return e, nil
}

// UpsertAddressRefs inserts address→event index rows idempotently.
// Duplicate (address, event_id, role) combinations are silently ignored
// by the ON CONFLICT DO NOTHING clause, so re-ingesting an event never
// duplicates its address rows.
func (p *Postgres) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	if len(refs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range refs {
		batch.Queue(`
			INSERT INTO event_addresses (address, event_id, role)
			VALUES ($1, $2, $3)
			ON CONFLICT (address, event_id, role) DO NOTHING`,
			r.Address, r.EventID, r.Role)
	}
	results := p.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range refs {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upserting address refs: %w", err)
		}
	}
	return nil
}

// QueryAddressEvents returns events involving the given address, in
// chronological order (by event_id), cursor-paginated. Supports the same
// filter params as the main events listing (contract_id, type, ledger range
// via from_ledger/to_ledger).
func (p *Postgres) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	// The tenant boundary is evaluated before anything else, and an empty
	// scope returns an empty page without issuing SQL — same contract as
	// QueryEvents.
	if f.Scope.DeniesAll() {
		return nil, "", nil
	}

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

	// Join with event_addresses to find events for this address.
	where = append(where, "ea.address = "+arg(address))
	where = append(where, "e.id = ea.event_id")

	// Scope is applied before any other filter, exactly like QueryEvents.
	if !f.Scope.IsWildcard() {
		where = append(where, "e.contract_id = ANY("+arg(f.Scope.Contracts())+")")
	}

	if f.ContractID != "" {
		where = append(where, "e.contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		where = append(where, "e.type = ANY("+arg(f.Types)+")")
	}
	if f.FromLedger > 0 {
		where = append(where, "e.ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "e.ledger <= "+arg(f.ToLedger))
	}

	// Cursor: event_id-based pagination (paging by event ID in chronological order).
	orderDir := "ASC"
	cursorOp := ">"
	if f.Order == "desc" {
		orderDir = "DESC"
		cursorOp = "<"
	}
	orderCols := "e.id " + orderDir

	if f.Cursor != "" {
		where = append(where, "e.id "+cursorOp+" "+arg(f.Cursor))
	}

	query := `SELECT ` + eventColumns + `
		FROM events e
		INNER JOIN event_addresses ea ON ea.event_id = e.id`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY " + orderCols + " LIMIT " + arg(limit+1)

	var events []Event
	next := ""
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("querying address events for %s: %w", address, err)
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
			return fmt.Errorf("reading address events: %w", err)
		}

		if len(events) > limit {
			events = events[:limit]
			next = events[limit-1].ID
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return events, next, nil
}

// CountAddressEvents returns the total number of events involving the given
// address (ignoring pagination).
func (p *Postgres) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	var total int64
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM events e
			INNER JOIN event_addresses ea ON ea.event_id = e.id
			WHERE ea.address = $1`, address,
		).Scan(&total)
	})
	if err != nil {
		return 0, fmt.Errorf("counting address events for %s: %w", address, err)
	}
	return total, nil
}

// GetAddressSummary returns aggregate information about an address\'s event
// history: first/last seen ledger, total event count, and distinct contracts
// interacted with.
func (p *Postgres) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	var s AddressSummary
	s.Address = address
	err := p.withStatementTimeoutTx(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT
				min(e.ledger),
				max(e.ledger),
				count(*),
				array_agg(DISTINCT e.contract_id ORDER BY e.contract_id) FILTER (WHERE e.contract_id IS NOT NULL)
			FROM events e
			INNER JOIN event_addresses ea ON ea.event_id = e.id
			WHERE ea.address = $1`, address,
		).Scan(&s.FirstSeenLedger, &s.LastSeenLedger, &s.EventCount, &s.DistinctContracts)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrNotFound
	}
	if err != nil {
		return AddressSummary{}, fmt.Errorf("loading address summary for %s: %w", address, err)
	}
	return s, nil
}
