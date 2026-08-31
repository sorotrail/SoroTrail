package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLite implements Store on a pure-Go SQLite database via modernc.org/sqlite.
// It uses WAL journal mode and a 5-second busy timeout to allow concurrent
// readers during ingestion writes.
type SQLite struct {
	db *sql.DB
}

var _ Store = (*SQLite)(nil)

// NewSQLite wraps a *sql.DB opened with the "sqlite" driver. The caller
// owns the DB's lifecycle. Pragma settings for WAL mode and busy timeout
// are applied automatically.
func NewSQLite(db *sql.DB) *SQLite {
	return &SQLite{db: db}
}

const eventColumnsSQLite = `network, id, contract_id, ledger, type, tx_hash, tx_index, op_index,
	in_successful_call, topics, value, created_at, topics_xdr, value_xdr`

func (s *SQLite) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	return s.upsertEvents(ctx, events, false)
}

func (s *SQLite) upsertEvents(ctx context.Context, events []Event, onUpdate bool) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	conflict := "ON CONFLICT (network, id) DO NOTHING"
	if onUpdate {
		conflict = `ON CONFLICT (network, id) DO UPDATE SET
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
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO events (`+eventColumnsSQLite+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`+conflict)
	if err != nil {
		return 0, fmt.Errorf("prepare upsert: %w", err)
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
			return affected, fmt.Errorf("inserting event %s: %w", e.ID, err)
		}
		n, _ := res.RowsAffected()
		affected += n
	}

	if err := tx.Commit(); err != nil {
		return affected, fmt.Errorf("commit upsert: %w", err)
	}
	return affected, nil
}

func (s *SQLite) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	for _, r := range refs {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO event_addresses (address, event_id, role)
			VALUES (?, ?, ?)
			ON CONFLICT (address, event_id, role) DO NOTHING`,
			r.Address, r.EventID, r.Role)
		if err != nil {
			return fmt.Errorf("upserting address refs: %w", err)
		}
	}
	return nil
}

func (s *SQLite) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	kept, err := rawXDRInRangeSQLite(ctx, tx, fromLedger, toLedger)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM events WHERE ledger >= ? AND ledger <= ?`, fromLedger, toLedger); err != nil {
		return fmt.Errorf("deleting events in ledger range [%d,%d]: %w", fromLedger, toLedger, err)
	}

	if len(events) > 0 {
		events = restoreRawXDR(events, kept)
		if _, err := s.upsertEventsTx(ctx, tx, events, true); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replace: %w", err)
	}
	return nil
}

func (s *SQLite) upsertEventsTx(ctx context.Context, tx *sql.Tx, events []Event, onUpdate bool) (int64, error) {
	conflict := "ON CONFLICT (network, id) DO NOTHING"
	if onUpdate {
		conflict = `ON CONFLICT (network, id) DO UPDATE SET
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
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO events (`+eventColumnsSQLite+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`+conflict)
	if err != nil {
		return 0, fmt.Errorf("prepare upsert tx: %w", err)
	}
	defer stmt.Close()

	var affected int64
	for _, e := range events {
		res, err := stmt.ExecContext(ctx,
			defaultNetwork(e.Network), e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex,
			e.OpIndex, e.InSuccessfulCall, e.Topics, e.Value, formatTime(e.CreatedAt),
			nullableXDRTopics(e.RawTopicXDR), nullableText(e.RawValueXDR),
		)
		if err != nil {
			return affected, fmt.Errorf("inserting event %s: %w", e.ID, err)
		}
		n, _ := res.RowsAffected()
		affected += n
	}
	return affected, nil
}

func rawXDRInRangeSQLite(ctx context.Context, tx *sql.Tx, fromLedger, toLedger int64) (map[string]rawXDR, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, topics_xdr, value_xdr
		FROM events
		WHERE ledger >= ? AND ledger <= ?
		  AND (topics_xdr IS NOT NULL OR value_xdr IS NOT NULL)`,
		fromLedger, toLedger)
	if err != nil {
		return nil, fmt.Errorf("reading raw XDR in ledger range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	defer rows.Close()

	kept := map[string]rawXDR{}
	for rows.Next() {
		var (
			id        string
			rawTopics *string
			rawValue  sql.NullString
		)
		if err := rows.Scan(&id, &rawTopics, &rawValue); err != nil {
			return nil, fmt.Errorf("scanning raw XDR: %w", err)
		}
		r := rawXDR{}
		if rawTopics != nil {
			_ = json.Unmarshal([]byte(*rawTopics), &r.topics)
		}
		if rawValue.Valid {
			r.value = rawValue.String
		}
		kept[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading raw XDR rows: %w", err)
	}
	return kept, nil
}

func (s *SQLite) GetEvent(ctx context.Context, id string, _ Scope) (Event, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+eventColumnsSQLite+` FROM events WHERE id = ?`, id)
	e, err := scanEventSQLite(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	return e, err
}

func (s *SQLite) EventExists(ctx context.Context, id string, _ Scope) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM events WHERE id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking event existence: %w", err)
	}
	return true, nil
}

func (s *SQLite) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	query := "SELECT " + eventColumnsSQLite + " FROM events WHERE tx_hash = ?"
	args := []any{txHash}
	if excludeID != "" {
		query += " AND id != ?"
		args = append(args, excludeID)
	}
	query += " ORDER BY id ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying events by tx hash: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		e, err := scanEventSQLite(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading events by tx hash: %w", err)
	}
	return events, nil
}

func (s *SQLite) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
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
		return "?"
	}
	if f.Network != "" {
		where = append(where, "network = "+arg(defaultNetwork(f.Network)))
	}
	if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		// SQLite has no ANY(); expand to an IN list.
		ph := make([]string, 0, len(f.Types))
		for _, t := range f.Types {
			ph = append(ph, arg(t))
		}
		where = append(where, "type IN ("+strings.Join(ph, ", ")+")")
	}
	if len(f.Topic) > 0 {
		where = append(where, topicContainsExpr(arg(f.Topic)))
	}
	if len(f.TopicContains) > 0 {
		tc := f.TopicContains
		var arr []json.RawMessage
		if json.Unmarshal(tc, &arr) == nil {
			for _, elem := range arr {
				where = append(where, topicContainsExpr(arg(elem)))
			}
		} else {
			where = append(where, "1=0")
		}
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where, fmt.Sprintf("json_extract(topics, '$[%d]') = json(?)", i))
		args = append(args, string(topic))
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}
	if !f.FromTime.IsZero() {
		where = append(where, "created_at >= "+arg(formatTime(f.FromTime)))
	}
	if !f.ToTime.IsZero() {
		where = append(where, "created_at <= "+arg(formatTime(f.ToTime)))
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

	query := `SELECT ` + eventColumnsSQLite + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id " + orderDir + " LIMIT " + arg(limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("querying events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		e, err := scanEventSQLite(rows)
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

// topicContainsExpr returns a SQLite expression that checks if the topics
// JSON array contains a JSON element that structurally equals the given value.
func topicContainsExpr(placeholder string) string {
	return fmt.Sprintf(`json_type(topics) = 'array' AND EXISTS (
		SELECT 1 FROM json_each(topics) AS _te WHERE json(_te.value) = json(%s)
	)`, placeholder)
}

func (s *SQLite) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	f.Cursor = ""
	f.Order = ""
	f.OrderBy = ""
	f.Limit = 0

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "?"
	}
	if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		where = append(where, "type = ANY("+arg(f.Types)+")")
	}
	if len(f.Topic) > 0 {
		where = append(where, topicContainsExpr(arg(f.Topic)))
	}
	if len(f.TopicContains) > 0 {
		tc := f.TopicContains
		var arr []json.RawMessage
		if json.Unmarshal(tc, &arr) == nil {
			for _, elem := range arr {
				where = append(where, topicContainsExpr(arg(elem)))
			}
		} else {
			where = append(where, "1=0")
		}
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where, fmt.Sprintf("json_extract(topics, '$[%d]') = json(?)", i))
		args = append(args, string(topic))
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}
	if !f.FromTime.IsZero() {
		where = append(where, "created_at >= "+arg(formatTime(f.FromTime)))
	}
	if !f.ToTime.IsZero() {
		where = append(where, "created_at <= "+arg(formatTime(f.ToTime)))
	}

	var groupCol, bucketCol string
	switch bucket {
	case "ledger":
		groupCol = "ledger"
		bucketCol = "ledger"
	default:
		dur, err := time.ParseDuration(bucket)
		if err != nil {
			return nil, fmt.Errorf("invalid bucket %q: %w", bucket, err)
		}
		if dur <= 0 {
			return nil, fmt.Errorf("bucket duration must be positive")
		}
		groupCol = "strftime('%Y-%m-%dT%H:%M:%S', datetime(created_at, 'unixepoch'))"
		bucketCol = groupCol
	}

	query := "SELECT " + bucketCol + " AS bucket, count(*) AS count FROM events"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY " + groupCol + " ORDER BY " + groupCol

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("aggregating events: %w", err)
	}
	defer rows.Close()

	var buckets []AggregateBucket
	for rows.Next() {
		var b AggregateBucket
		if err := rows.Scan(&b.Bucket, &b.Count); err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading aggregation: %w", err)
	}
	return buckets, nil
}

func (s *SQLite) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	f.Cursor = ""
	f.Order = ""
	f.OrderBy = ""
	f.Limit = 0

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "?"
	}
	if f.ContractID != "" {
		where = append(where, "contract_id = "+arg(f.ContractID))
	}
	if len(f.Types) > 0 {
		where = append(where, "type = ANY("+arg(f.Types)+")")
	}
	if len(f.Topic) > 0 {
		where = append(where, topicContainsExpr(arg(f.Topic)))
	}
	if len(f.TopicContains) > 0 {
		tc := f.TopicContains
		var arr []json.RawMessage
		if json.Unmarshal(tc, &arr) == nil {
			for _, elem := range arr {
				where = append(where, topicContainsExpr(arg(elem)))
			}
		} else {
			where = append(where, "1=0")
		}
	}
	for i, topic := range []json.RawMessage{f.Topic0, f.Topic1, f.Topic2, f.Topic3} {
		if len(topic) == 0 {
			continue
		}
		where = append(where, fmt.Sprintf("json_extract(topics, '$[%d]') = json(?)", i))
		args = append(args, string(topic))
	}
	if f.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(f.ToLedger))
	}
	if !f.FromTime.IsZero() {
		where = append(where, "created_at >= "+arg(formatTime(f.FromTime)))
	}
	if !f.ToTime.IsZero() {
		where = append(where, "created_at <= "+arg(formatTime(f.ToTime)))
	}

	query := "SELECT count(*) FROM events"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	var total int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting events: %w", err)
	}
	return total, nil
}

func (s *SQLite) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
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
		return "?"
	}

	// Join with event_addresses to find events for this address.
	where = append(where, "ea.address = "+arg(address))
	where = append(where, "e.id = ea.event_id")

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

	query := `SELECT ` + eventColumnsSQLite + `
		FROM events e
		INNER JOIN event_addresses ea ON ea.event_id = e.id`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY " + orderCols + " LIMIT " + arg(limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("querying address events for %s: %w", address, err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		e, err := scanEventSQLite(rows)
		if err != nil {
			return nil, "", err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading address events: %w", err)
	}

	next := ""
	if len(events) > limit {
		events = events[:limit]
		next = events[limit-1].ID
	}
	return events, next, nil
}

func (s *SQLite) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	var total int64
	err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FROM events e
		INNER JOIN event_addresses ea ON ea.event_id = e.id
		WHERE ea.address = ?`, address,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting address events for %s: %w", address, err)
	}
	return total, nil
}

func (s *SQLite) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "?"
	}
	if f.ContractIDPrefix != "" {
		where = append(where, "contract_id LIKE "+arg(f.ContractIDPrefix+"%"))
	}
	query := "SELECT count(*) FROM (SELECT contract_id FROM events"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY contract_id) AS sub"

	var total int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting contracts: %w", err)
	}
	return total, nil
}

func (s *SQLite) DeadLetterEvent(ctx context.Context, in DeadLetterInput) (DeadLetter, error) {
	errStr := ""
	if in.Err != nil {
		errStr = in.Err.Error()
	}
	var d DeadLetter
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO dead_letters
		    (event_id, contract_id, ledger, type, tx_hash,
		     topic_xdr, value_xdr, error, attempts, last_attempt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'))
		ON CONFLICT (event_id) DO UPDATE SET
		    error        = excluded.error,
		    attempts     = dead_letters.attempts + 1,
		    last_attempt = datetime('now'),
		    topic_xdr    = excluded.topic_xdr,
		    value_xdr    = excluded.value_xdr
		RETURNING id, event_id, contract_id, ledger, type, tx_hash,
		          topic_xdr, value_xdr, error, attempts, last_attempt, created_at`,
		in.EventID, in.ContractID, in.Ledger, in.Type, in.TxHash,
		in.TopicXDR, in.ValueXDR, errStr,
	).Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger, &d.Type, &d.TxHash,
		&d.TopicXDR, &d.ValueXDR, &d.Error, &d.Attempts, &d.LastAttempt, &d.CreatedAt)
	if err != nil {
		return DeadLetter{}, fmt.Errorf("recording dead letter: %w", err)
	}
	return d, nil
}

func (s *SQLite) ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error) {
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
		return "?"
	}
	if contractID != "" {
		where = append(where, "contract_id = "+arg(contractID))
	}
	if cursor != "" {
		sv, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("%w: dead letter cursor", ErrInvalidContractsCursor)
		}
		where = append(where, "dead_letters.id < "+arg(string(sv)))
	}
	query := "SELECT id, event_id, contract_id, ledger, type, tx_hash, topic_xdr, value_xdr, error, attempts, last_attempt, created_at FROM dead_letters"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY id DESC LIMIT " + arg(limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("listing dead letters: %w", err)
	}
	defer rows.Close()

	var letters []DeadLetter
	for rows.Next() {
		var d DeadLetter
		var txHash, valueXDR *string
		if err := rows.Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger, &d.Type, &txHash,
			&d.TopicXDR, &valueXDR, &d.Error, &d.Attempts, &d.LastAttempt, &d.CreatedAt); err != nil {
			return nil, "", err
		}
		if txHash != nil {
			d.TxHash = *txHash
		}
		if valueXDR != nil {
			d.ValueXDR = *valueXDR
		}
		letters = append(letters, d)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading dead letters: %w", err)
	}
	next := ""
	if len(letters) > limit {
		letters = letters[:limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", letters[limit-1].ID)))
	}
	return letters, next, nil
}

// CountDeadLetters returns the total number of dead-letter rows matching
// the same contract filter as ListDeadLetters ("" means all contracts).
// Pagination is ignored: the count is the full match set behind any page.
func (s *SQLite) CountDeadLetters(ctx context.Context, contractID string) (int64, error) {
	var total int64
	var err error
	if contractID != "" {
		err = s.db.QueryRowContext(ctx,
			`SELECT count(*) FROM dead_letters WHERE contract_id = ?`,
			contractID,
		).Scan(&total)
	} else {
		err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM dead_letters`).Scan(&total)
	}
	if err != nil {
		return 0, fmt.Errorf("counting dead letters: %w", err)
	}
	return total, nil
}

func (s *SQLite) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error) {
	var d DeadLetter
	var txHash, valueXDR *string
	err := s.db.QueryRowContext(ctx, `
		SELECT id, event_id, contract_id, ledger, type, tx_hash,
		       topic_xdr, value_xdr, error, attempts, last_attempt, created_at
		FROM dead_letters WHERE id = ?`, id,
	).Scan(&d.ID, &d.EventID, &d.ContractID, &d.Ledger, &d.Type, &txHash,
		&d.TopicXDR, &valueXDR, &d.Error, &d.Attempts, &d.LastAttempt, &d.CreatedAt)
	if err == sql.ErrNoRows {
		return DeadLetter{}, ErrNotFound
	}
	if err != nil {
		return DeadLetter{}, fmt.Errorf("getting dead letter: %w", err)
	}
	if txHash != nil {
		d.TxHash = *txHash
	}
	if valueXDR != nil {
		d.ValueXDR = *valueXDR
	}
	return d, nil
}

func (s *SQLite) DeleteDeadLetter(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM dead_letters WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("deleting dead letter: %w", err)
	}
	return nil
}

func (s *SQLite) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	var (
		where string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "?"
	}
	where = "ledger < " + arg(maxLedger)
	if !beforeTime.IsZero() {
		where += " AND created_at < " + arg(beforeTime)
	}
	q := fmt.Sprintf("DELETE FROM events WHERE id IN (SELECT id FROM events WHERE %s LIMIT %d)", where, limit)
	result, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", maxLedger, err)
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

func (s *SQLite) DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error) {
	result, err := s.db.ExecContext(ctx, "DELETE FROM events WHERE ledger < ?", beforeLedger)
	if err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", beforeLedger, err)
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}
func (s *SQLite) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	var where string
	var args []interface{}
	where = "ledger < ?"
	args = append(args, maxLedger)
	if !beforeTime.IsZero() {
		where += " AND created_at < ?"
		args = append(args, beforeTime)
	}
	var total int64
	query := "SELECT count(*) FROM (SELECT 1 FROM events WHERE " + where + " LIMIT " + fmt.Sprintf("%d", limit) + ")"
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting events before ledger %d: %w", maxLedger, err)
	}
	return total, nil
}

func (s *SQLite) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	var sum AddressSummary
	sum.Address = address
	err := s.db.QueryRowContext(ctx, `
		SELECT
			min(e.ledger),
			max(e.ledger),
			count(*),
			group_concat(DISTINCT e.contract_id)
		FROM events e
		INNER JOIN event_addresses ea ON ea.event_id = e.id
		WHERE ea.address = ?`, address,
	).Scan(&sum.FirstSeenLedger, &sum.LastSeenLedger, &sum.EventCount, &sum.DistinctContracts)
	if err == sql.ErrNoRows {
		return sum, ErrNotFound
	}
	if err != nil {
		return AddressSummary{}, fmt.Errorf("loading address summary for %s: %w", address, err)
	}
	return sum, nil
}

func (s *SQLite) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ledger, count(*) AS count, group_concat(id ORDER BY id)
		FROM events
		WHERE ledger >= ? AND ledger <= ?
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
		var (
			c     LedgerCensus
			ids   sql.NullString
			count int
		)
		if err := rows.Scan(&c.Ledger, &count, &ids); err != nil {
			return nil, err
		}
		c.Count = count
		if idsOnly && ids.Valid && ids.String != "" {
			c.IDs = strings.Split(ids.String, ",")
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *SQLite) GetIngestionState(ctx context.Context) (IngestionState, error) {
	return s.GetIngestionStateForNetwork(ctx, "default")
}

func (s *SQLite) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	var st IngestionState
	var ts string
	var poll *string
	err := s.db.QueryRowContext(ctx,
		`SELECT network, last_ingested_ledger, last_cursor, last_successful_poll, updated_at FROM ingestion_state WHERE network = ?`, defaultNetwork(network),
	).Scan(&st.Network, &st.LastIngestedLedger, &st.LastCursor, &poll, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return IngestionState{}, ErrNotFound
	}
	if err != nil {
		return IngestionState{}, fmt.Errorf("loading ingestion state: %w", err)
	}
	st.UpdatedAt = parseTime(ts)
	if poll != nil {
		parsed := parseTime(*poll)
		st.LastSuccessfulPoll = &parsed
	}
	return st, nil
}

func (s *SQLite) SaveIngestionState(ctx context.Context, st IngestionState) error {
	st.Network = defaultNetwork(st.Network)
	now := formatTime(time.Now().UTC())
	// last_successful_poll mirrors the Postgres backend: the UPSERT sets
	// it to EXCLUDED.last_successful_poll, so a nil value overwrites with
	// NULL (not the previous timestamp). A non-nil poll is stored as an
	// RFC3339Nano string that parseTime round-trips on read.
	var poll any
	if st.LastSuccessfulPoll != nil {
		poll = formatTime(*st.LastSuccessfulPoll)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ingestion_state (network, last_ingested_ledger, last_cursor, last_successful_poll, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (network) DO UPDATE SET
			last_ingested_ledger = excluded.last_ingested_ledger,
			last_cursor          = excluded.last_cursor,
			last_successful_poll = excluded.last_successful_poll,
			updated_at           = excluded.updated_at`,
		st.Network, st.LastIngestedLedger, st.LastCursor, poll, now,
	)
	if err != nil {
		return fmt.Errorf("saving ingestion state: %w", err)
	}
	return nil
}

func (s *SQLite) GetAuditState(ctx context.Context, _ string) (AuditState, error) {
	var (
		st AuditState
		ts string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT verified_through_ledger, updated_at FROM audit_state WHERE id = 1`,
	).Scan(&st.VerifiedThroughLedger, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return AuditState{}, ErrNotFound
	}
	if err != nil {
		return AuditState{}, fmt.Errorf("loading audit state: %w", err)
	}
	st.UpdatedAt = parseTime(ts)
	return st, nil
}

func (s *SQLite) SaveAuditState(ctx context.Context, st AuditState) error {
	now := formatTime(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_state (id, verified_through_ledger, updated_at)
		VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			verified_through_ledger = excluded.verified_through_ledger,
			updated_at              = excluded.updated_at`,
		st.VerifiedThroughLedger, now,
	)
	if err != nil {
		return fmt.Errorf("saving audit state: %w", err)
	}
	return nil
}

func (s *SQLite) SaveAuditStateIfGreater(ctx context.Context, _ string, ledger int64) (AuditState, error) {
	now := formatTime(time.Now().UTC())

	// First INSERT will succeed when the table is empty.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO audit_state (id, verified_through_ledger, updated_at)
		VALUES (1, ?, ?)`,
		ledger, now)
	if err == nil {
		return AuditState{VerifiedThroughLedger: ledger, UpdatedAt: time.Now().UTC()}, nil
	}

	// Row exists — update only when the candidate is greater than the stored value.
	res, err := s.db.ExecContext(ctx, `
		UPDATE audit_state
		SET verified_through_ledger = ?, updated_at = ?
		WHERE id = 1 AND ? > verified_through_ledger`,
		ledger, now, ledger)
	if err != nil {
		return AuditState{}, fmt.Errorf("saving audit state (if greater): %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		return s.GetAuditState(ctx, "")
	}
	// Candidate was not greater — return the current stored state.
	return s.GetAuditState(ctx, "")
}

func (s *SQLite) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT contract_id FROM watched_contracts ORDER BY contract_id`)
	if err != nil {
		return nil, fmt.Errorf("listing watched contracts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []WatchedContract
	for rows.Next() {
		var wc WatchedContract
		if err := rows.Scan(&wc.ContractID); err != nil {
			return nil, err
		}
		out = append(out, wc)
	}
	return out, rows.Err()
}

func (s *SQLite) AddWatchedContract(ctx context.Context, contractID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO watched_contracts (contract_id) VALUES (?)
		ON CONFLICT (contract_id) DO NOTHING`, contractID)
	if err != nil {
		return fmt.Errorf("adding watched contract: %w", err)
	}
	return nil
}

func (s *SQLite) RemoveWatchedContract(ctx context.Context, contractID string) error {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM watched_contracts WHERE contract_id = ?`, contractID)
	if err != nil {
		return fmt.Errorf("removing watched contract: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	missingIDs := "[]"
	if len(f.MissingIDs) > 0 {
		b, _ := json.Marshal(f.MissingIDs)
		missingIDs = string(b)
	}
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO audit_findings
			(from_ledger, to_ledger, expected_count, actual_count,
			 missing_ids, status, attempts)
		VALUES (?, ?, ?, ?, ?, ?, 0)
		RETURNING id, created_at`,
		f.FromLedger, f.ToLedger, f.ExpectedCount, f.ActualCount,
		missingIDs, f.Status,
	).Scan(&f.ID, &f.CreatedAt)
	if err != nil {
		return AuditFinding{}, fmt.Errorf("recording audit finding: %w", err)
	}
	return f, nil
}

func (s *SQLite) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	var lastAttempted any
	if !f.LastAttemptedAt.IsZero() {
		lastAttempted = formatTime(f.LastAttemptedAt)
	}
	missingIDs := "[]"
	if len(f.MissingIDs) > 0 {
		b, _ := json.Marshal(f.MissingIDs)
		missingIDs = string(b)
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE audit_findings SET
			status            = ?,
			attempts          = ?,
			last_attempted_at = ?,
			last_error        = ?,
			missing_ids       = ?
		WHERE id = ?`,
		f.Status, f.Attempts, lastAttempted, nullableText(f.LastError), missingIDs, f.ID,
	)
	if err != nil {
		return fmt.Errorf("updating audit finding %d: %w", f.ID, err)
	}
	return nil
}

func (s *SQLite) ListOpenFindingsByRange(ctx context.Context, _ string, fromLedger, toLedger int64) (AuditFinding, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, from_ledger, to_ledger, expected_count, actual_count,
		       missing_ids, status, attempts, last_attempted_at, last_error, created_at
		FROM audit_findings
		WHERE status IN ('open', 'unrecoverable')
		  AND from_ledger <= ?
		  AND to_ledger   >= ?
		ORDER BY id DESC
		LIMIT 1`,
		toLedger, fromLedger,
	)
	return scanAuditFinding(row)
}

func scanAuditFinding(row *sql.Row) (AuditFinding, error) {
	var (
		f             AuditFinding
		lastAttempted *string
		lastError     *string
		missingIDs    string
		createdAt     string
	)
	err := row.Scan(&f.ID, &f.FromLedger, &f.ToLedger, &f.ExpectedCount, &f.ActualCount,
		&missingIDs, &f.Status, &f.Attempts, &lastAttempted, &lastError, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return AuditFinding{}, ErrNotFound
	}
	if err != nil {
		return AuditFinding{}, fmt.Errorf("listing findings: %w", err)
	}
	if lastAttempted != nil {
		f.LastAttemptedAt = parseTime(*lastAttempted)
	}
	if lastError != nil {
		f.LastError = *lastError
	}
	_ = json.Unmarshal([]byte(missingIDs), &f.MissingIDs)
	f.CreatedAt = parseTime(createdAt)
	return f, nil
}

func (s *SQLite) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	filtersJSON, err := json.Marshal(sub.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO subscriptions (url, filters, secret, enabled)
		VALUES (?, ?, ?, ?)
		RETURNING id, failure_count, created_at`,
		sub.URL, filtersJSON, sub.Secret, sub.Enabled,
	).Scan(&sub.ID, &sub.FailureCount, &sub.CreatedAt)
	if err != nil {
		return Subscription{}, fmt.Errorf("creating subscription: %w", err)
	}
	return sub, nil
}

func (s *SQLite) GetSubscription(ctx context.Context, id int64, _ SubscriptionOwner) (Subscription, error) {
	return scanSubscription(s.db.QueryRowContext(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions WHERE id = ?`, id))
}

func (s *SQLite) ListSubscriptions(ctx context.Context, _ SubscriptionOwner) ([]Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptionsSQLite(rows)
}

func (s *SQLite) UpdateSubscription(ctx context.Context, sub Subscription, _ SubscriptionOwner) (Subscription, error) {
	filtersJSON, err := json.Marshal(sub.Filters)
	if err != nil {
		return Subscription{}, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE subscriptions SET
			url    = ?,
			filters = ?,
			secret  = ?,
			enabled = ?
		WHERE id = ?`,
		sub.URL, filtersJSON, sub.Secret, sub.Enabled, sub.ID,
	)
	if err != nil {
		return Subscription{}, fmt.Errorf("updating subscription %d: %w", sub.ID, err)
	}
	return s.GetSubscription(ctx, sub.ID, SubscriptionOwner{})
}

func (s *SQLite) DeleteSubscription(ctx context.Context, id int64, _ SubscriptionOwner) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM subscriptions WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("deleting subscription %d: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLite) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, url, filters, secret, enabled, failure_count, created_at
		FROM subscriptions WHERE enabled = 1 ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing enabled subscriptions: %w", err)
	}
	defer rows.Close()
	return scanSubscriptionsSQLite(rows)
}

func (s *SQLite) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	var newCount int
	var enabled bool
	err := s.db.QueryRowContext(ctx, `
		UPDATE subscriptions SET
			failure_count = failure_count + 1,
			enabled = CASE WHEN failure_count + 1 >= ? THEN 0 ELSE enabled END
		WHERE id = ?
		RETURNING failure_count, enabled`,
		maxFailures, id,
	).Scan(&newCount, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, ErrNotFound
	}
	if err != nil {
		return 0, false, fmt.Errorf("incrementing failures for subscription %d: %w", id, err)
	}
	return newCount, !enabled, nil
}

func (s *SQLite) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE subscriptions SET failure_count = 0 WHERE id = ?`, id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("resetting failures for subscription %d: %w", id, err)
	}
	return nil
}

func (s *SQLite) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO delivery_attempts
			(subscription_id, event_id, status, response_code, duration_ms, error)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id, created_at`,
		a.SubscriptionID, a.EventID, a.Status, a.ResponseCode,
		a.DurationMs, nullableText(a.Error),
	).Scan(&a.ID, &a.CreatedAt)
	if err != nil {
		return DeliveryAttempt{}, fmt.Errorf("recording delivery attempt: %w", err)
	}
	return a, nil
}

func (s *SQLite) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, _ SubscriptionOwner) ([]DeliveryAttempt, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, subscription_id, event_id, status, response_code,
		       duration_ms, error, created_at
		FROM delivery_attempts
		WHERE subscription_id = ?
		ORDER BY created_at DESC
		LIMIT ?`,
		subscriptionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("listing delivery attempts: %w", err)
	}
	defer rows.Close()

	var attempts []DeliveryAttempt
	for rows.Next() {
		var (
			a         DeliveryAttempt
			errStr    *string
			createdAt string
		)
		if err := rows.Scan(&a.ID, &a.SubscriptionID, &a.EventID, &a.Status,
			&a.ResponseCode, &a.DurationMs, &errStr, &createdAt); err != nil {
			return nil, fmt.Errorf("scanning delivery attempt: %w", err)
		}
		if errStr != nil {
			a.Error = *errStr
		}
		a.CreatedAt = parseTime(createdAt)
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading delivery attempts: %w", err)
	}
	return attempts, nil
}

// CountDeliveryAttempts returns the total number of delivery attempts
// recorded for a subscription, ignoring the list's limit. SQLite applies
// no owner filter (mirroring ListDeliveryAttempts) — the multi-tenant
// owner boundary lives in Postgres.
func (s *SQLite) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, _ SubscriptionOwner) (int64, error) {
	var total int64
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM delivery_attempts WHERE subscription_id = ?`,
		subscriptionID,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("counting delivery attempts: %w", err)
	}
	return total, nil
}

func (s *SQLite) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	var specJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT spec_json FROM contract_specs WHERE wasm_hash = ?`, wasmHash,
	).Scan(&specJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("loading contract spec for wasm_hash %s: %w", wasmHash, err)
	}
	return []byte(specJSON), nil
}

func (s *SQLite) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO contract_specs (wasm_hash, contract_id, spec_json, fetched_at)
		VALUES (?, ?, ?, datetime('now'))
		ON CONFLICT (wasm_hash) DO UPDATE SET
			spec_json  = excluded.spec_json,
			fetched_at = excluded.fetched_at`,
		wasmHash, contractID, string(specJSON),
	)
	if err != nil {
		return fmt.Errorf("saving contract spec for %s: %w", wasmHash, err)
	}
	return nil
}

func (s *SQLite) Stats(ctx context.Context, _ Scope) (Stats, error) {
	var st Stats
	err := s.db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM events),
			(SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state),
			(SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state),
			(SELECT coalesce(min(ledger), 0) FROM events),
			(SELECT count(DISTINCT contract_id) FROM events),
			(SELECT count(*) FROM watched_contracts)`,
	).Scan(&st.TotalEvents, &st.LastIngestedLedger, &st.VerifiedThroughLedger,
		&st.OldestStoredLedger, &st.ContractCount, &st.WatchedContracts)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	return st, nil
}

func (s *SQLite) MigrationVersion(ctx context.Context) (version int, dirty bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT version, dirty FROM schema_migrations`,
	).Scan(&version, &dirty)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading migration version: %w", err)
	}
	return version, dirty, nil
}

func (s *SQLite) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// --- helpers ---

func scanEventSQLite(row interface {
	Scan(dest ...any) error
}) (Event, error) {
	var (
		e         Event
		createdAt string
		rawTopics *string
		rawValue  sql.NullString
	)
	err := row.Scan(&e.Network, &e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash,
		&e.TxIndex, &e.OpIndex, &e.InSuccessfulCall, &e.Topics, &e.Value,
		&createdAt, &rawTopics, &rawValue)
	if err != nil {
		return Event{}, err
	}
	e.CreatedAt = parseTime(createdAt)
	if rawTopics != nil {
		_ = json.Unmarshal([]byte(*rawTopics), &e.RawTopicXDR)
	}
	if rawValue.Valid {
		e.RawValueXDR = rawValue.String
	}
	return e, nil
}

func scanSubscription(row interface {
	Scan(dest ...any) error
}) (Subscription, error) {
	var (
		s         Subscription
		filters   []byte
		createdAt string
	)
	err := row.Scan(&s.ID, &s.URL, &filters, &s.Secret, &s.Enabled,
		&s.FailureCount, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Subscription{}, ErrNotFound
		}
		return Subscription{}, fmt.Errorf("scanning subscription: %w", err)
	}
	if err := json.Unmarshal(filters, &s.Filters); err != nil {
		return Subscription{}, fmt.Errorf("unmarshaling subscription filters: %w", err)
	}
	s.CreatedAt = parseTime(createdAt)
	return s, nil
}

func scanSubscriptionsSQLite(rows *sql.Rows) ([]Subscription, error) {
	var subs []Subscription
	for rows.Next() {
		var (
			s         Subscription
			filters   []byte
			createdAt string
		)
		if err := rows.Scan(&s.ID, &s.URL, &filters, &s.Secret, &s.Enabled,
			&s.FailureCount, &createdAt); err != nil {
			return nil, fmt.Errorf("scanning subscription: %w", err)
		}
		if err := json.Unmarshal(filters, &s.Filters); err != nil {
			return nil, fmt.Errorf("unmarshaling subscription filters: %w", err)
		}
		s.CreatedAt = parseTime(createdAt)
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05.999999", s)
			if err != nil {
				return time.Time{}
			}
		}
	}
	return t.UTC()
}

func nullableXDRTopics(s []string) any {
	if len(s) == 0 {
		return nil
	}
	b, _ := json.Marshal(s)
	return string(b)
}

// Contract metadata (token enrichment) is Postgres-only; the SQLite backend
// reports "not found"/empty so the enrichment worker stays a no-op.
func (s *SQLite) ListContractIDs(context.Context) ([]string, error) { return nil, nil }
func (s *SQLite) GetContractMeta(context.Context, string) (ContractMeta, error) {
	return ContractMeta{}, ErrNotFound
}
func (s *SQLite) UpsertContractMeta(context.Context, ContractMeta) error     { return nil }
func (s *SQLite) CountContractEvents(context.Context, string) (int64, error) { return 0, nil }
func (s *SQLite) ListContractsNeedingRefresh(context.Context, time.Time) ([]string, error) {
	return nil, nil
}

// ListContracts is not implemented for the SQLite backend: the contract
// inventory endpoint is Postgres-only.

// GetContractSummary is not implemented for the SQLite backend.
func (s *SQLite) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	return ContractSummary{}, fmt.Errorf("GetContractSummary: not supported by the sqlite backend")
}

// ContractEventTypeCounts is not implemented for the SQLite backend.
func (s *SQLite) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	return nil, fmt.Errorf("ContractEventTypeCounts: not supported by the sqlite backend")
}

func (s *SQLite) ListContracts(context.Context, ContractsFilter) ([]ContractSummary, string, error) {
	return nil, "", fmt.Errorf("ListContracts: not supported by the sqlite backend")
}

// Per-contract cursors are not implemented for the SQLite backend: watched
// ingestion always uses the single global ingestion_state row.
func (s *SQLite) GetContractCursor(context.Context, string) (ContractCursor, error) {
	return ContractCursor{}, ErrNotFound
}
func (s *SQLite) SaveContractCursor(context.Context, ContractCursor) error { return nil }
func (s *SQLite) DeleteContractCursor(context.Context, string) error       { return nil }
func (s *SQLite) ListContractCursors(context.Context) ([]ContractCursor, error) {
	return nil, nil
}
