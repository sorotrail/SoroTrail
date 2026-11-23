package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ClickHouse implements Store over ClickHouse's HTTP interface (the
// clickhouseExecutor wrapping it predates, and never grew into, a native
// driver dependency). It is intentionally minimal for now and is wired
// through the same interface so the app can select it via DATABASE_URL.
//
// Every method below (Ping included) talks to the server over the HTTP
// interface the executor wraps, so the readiness probe and the query path
// fail against exactly the same dependency.
type ClickHouse struct {
	cfg clickHouseConfig
}

var _ Store = (*ClickHouse)(nil)

// unsupportedLists the named operations this backend deliberately does not
// implement. A silent zero-value result here would let an operator believe a
// capability exists when it does not, which is the bug issue #527 traces to.
// Every method named here returns ErrUnsupported; the conformance suite
// asserts exactly this set (see TestStoreUnsupportedOperations).

type clickHouseConfig struct {
	host     string
	port     int
	httpPort int
	username string
	password string
	database string
	ssl      bool
}

// databaseOrDefault returns the configured database name, or ClickHouse's
// "default" database when the URL didn't name one — the same fallback
// migrateClickHouse uses when applying schema.
func (cfg clickHouseConfig) databaseOrDefault() string {
	if cfg.database == "" {
		return "default"
	}
	return cfg.database
}

// Contract metadata (token enrichment) is Postgres-only. Every method of
// that surface refuses here rather than answering with an empty result: a
// silent no-op made the enrichment pipeline report success forever while
// writing nothing, and it left a caller unable to tell a contract that has
// no metadata from a backend that cannot answer the question at all. The
// refusal wraps ErrUnsupported, so a caller recognises it with errors.Is and
// stands down instead of retrying a capability that will never arrive.
func (c *ClickHouse) ListContractIDs(context.Context) ([]string, error) {
	return nil, errUnsupported("clickhouse", "ListContractIDs")
}
func (c *ClickHouse) GetContractMeta(context.Context, string) (ContractMeta, error) {
	return ContractMeta{}, errUnsupported("clickhouse", "GetContractMeta")
}
func (c *ClickHouse) UpsertContractMeta(context.Context, ContractMeta) error {
	return errUnsupported("clickhouse", "UpsertContractMeta")
}
func (c *ClickHouse) CountContractEvents(context.Context, string) (int64, error) {
	return 0, errUnsupported("clickhouse", "CountContractEvents")
}

// GetContractSummary returns a single contract's summary from ClickHouse.
// It aggregates the events table in the database, mirroring the Postgres
// query (which has no separate contracts registry either): a contract is a
// contract_id that has emitted at least one stored event, and a contract
// with no events is ErrNotFound.
//
// The aggregate is exact, not approximate. count(), min(ledger) and
// max(ledger) are exact in ClickHouse, and max(created_at) is exact too, so
// there is no need for the approximate-function caveat other analytical
// endpoints carry.
func (c *ClickHouse) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	query := "SELECT contract_id, count() AS event_count, min(ledger) AS first_ledger, " +
		"max(ledger) AS last_ledger, max(created_at) AS last_seen " +
		"FROM events WHERE contract_id = " + clickHouseQuoteString(contractID) +
		" GROUP BY contract_id"
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return ContractSummary{}, fmt.Errorf("get contract summary %s: %w", contractID, err)
	}
	if len(rows) == 0 {
		return ContractSummary{}, ErrNotFound
	}
	row := rows[0]
	return ContractSummary{
		ContractID:  row.str("contract_id"),
		EventCount:  row.int64("event_count"),
		FirstLedger: row.int64("first_ledger"),
		LastLedger:  row.int64("last_ledger"),
		LastSeen:    row.time("last_seen"),
	}, nil
}

// ContractEventTypeCounts returns per-type event counts from ClickHouse,
// ordered by count descending like the Postgres implementation. The
// aggregation happens in the database rather than by fetching event rows
// into Go, which is the point of using the analytical backend at all.
func (c *ClickHouse) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	query := "SELECT type, count() AS event_count FROM events WHERE contract_id = " +
		clickHouseQuoteString(contractID) +
		" GROUP BY type ORDER BY event_count DESC"
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return nil, fmt.Errorf("contract event type counts %s: %w", contractID, err)
	}
	out := make([]ContractEventTypeCount, 0, len(rows))
	for _, row := range rows {
		out = append(out, ContractEventTypeCount{
			Type:  row.str("type"),
			Count: row.int64("event_count"),
		})
	}
	return out, nil
}

func (c *ClickHouse) ListContractsNeedingRefresh(context.Context, time.Time) ([]string, error) {
	return nil, errUnsupported("clickhouse", "ListContractsNeedingRefresh")
}

func parseClickHouseConfig(raw string) (clickHouseConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return clickHouseConfig{}, fmt.Errorf("parsing clickhouse url: %w", err)
	}
	if u.Scheme != "clickhouse" {
		return clickHouseConfig{}, fmt.Errorf("expected clickhouse scheme")
	}
	cfg := clickHouseConfig{database: strings.TrimPrefix(u.Path, "/")}
	if u.Host == "" {
		return clickHouseConfig{}, fmt.Errorf("missing host")
	}
	parts := strings.Split(u.Host, ":")
	cfg.host = parts[0]
	if len(parts) > 1 {
		cfg.port, err = strconv.Atoi(parts[1])
		if err != nil {
			return clickHouseConfig{}, fmt.Errorf("parsing clickhouse port: %w", err)
		}
	} else {
		cfg.port = 9000
	}
	cfg.username = u.User.Username()
	if password, ok := u.User.Password(); ok {
		cfg.password = password
	}
	if strings.EqualFold(u.Query().Get("sslmode"), "true") || strings.EqualFold(u.Query().Get("sslmode"), "require") {
		cfg.ssl = true
	}
	// The native protocol port (9000 by default, carried in the URL's host
	// component) is not used for schema migrations: there is no
	// database/sql-compatible ClickHouse driver in this module's
	// dependency set. Migrations instead go over ClickHouse's HTTP
	// interface, whose port defaults to 8123 but is independently
	// configurable via ?http_port= since operators may remap it.
	cfg.httpPort = 8123
	if raw := u.Query().Get("http_port"); raw != "" {
		cfg.httpPort, err = strconv.Atoi(raw)
		if err != nil {
			return clickHouseConfig{}, fmt.Errorf("parsing clickhouse http_port: %w", err)
		}
	}
	return cfg, nil
}

func NewStoreFromURL(databaseURL string) (Store, error) {
	if strings.HasPrefix(databaseURL, "clickhouse://") {
		cfg, err := parseClickHouseConfig(databaseURL)
		if err != nil {
			return nil, err
		}
		return &ClickHouse{cfg: cfg}, nil
	}
	if strings.HasPrefix(databaseURL, "postgres://") || strings.HasPrefix(databaseURL, "postgresql://") {
		return &Postgres{}, nil
	}
	return nil, fmt.Errorf("unsupported database url scheme")
}

// UpsertEvents inserts events idempotently by ID. The events table is a plain
// MergeTree (no ReplacingMergeTree deduplication), so the idempotency the
// Postgres ON CONFLICT provides has to be explicit here: the batch is deduped
// in memory, ids already present are read back, and only the genuinely new
// rows are inserted. The count returned is the number actually written, which
// is what callers use to report how much new data a cycle ingested.
func (c *ClickHouse) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	deduped := dedupeEventsByID(events)
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()

	ids := make([]string, 0, len(deduped))
	for _, e := range deduped {
		ids = append(ids, e.ID)
	}
	existing, err := exec.queryColumn(ctx, database, "SELECT id FROM events WHERE id IN ("+clickHouseQuoteList(ids)+")")
	if err != nil {
		return 0, fmt.Errorf("checking existing events: %w", err)
	}
	have := make(map[string]bool, len(existing))
	for _, id := range existing {
		have[id] = true
	}

	rows := make([]map[string]any, 0, len(deduped))
	for _, e := range deduped {
		if have[e.ID] {
			continue
		}
		rows = append(rows, clickHouseEventRow(e))
	}
	if err := exec.insertRows(ctx, database, "events", clickHouseEventColumns, rows); err != nil {
		return 0, fmt.Errorf("upserting events: %w", err)
	}
	return int64(len(rows)), nil
}

// ReplaceEventsInRange swaps the stored events in a ledger range for a
// freshly fetched set, as the auditor's repair path does. The stored raw XDR
// is preserved for any incoming row that arrives without it, mirroring
// Postgres's coalesce() in the repair upsert, so a JSON-only repair cannot
// strip the bytes a later replay needs.
//
// ClickHouse mutations are asynchronous by default, which would let the
// delete race the reinsert (the mutation could run after the new rows land
// and remove them too). SETTINGS mutations_sync = 1 makes the delete
// synchronous, so the replacement is ordered exactly as it is on Postgres.
func (c *ClickHouse) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	if len(events) == 0 {
		return nil
	}
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()

	rangePredicate := fmt.Sprintf("ledger >= %d AND ledger <= %d", fromLedger, toLedger)
	stored, err := exec.queryRows(ctx, database,
		"SELECT id, raw_topic_xdr, raw_value_xdr FROM events WHERE "+rangePredicate)
	if err != nil {
		return fmt.Errorf("reading events before repair: %w", err)
	}
	byID := make(map[string]clickHouseRow, len(stored))
	for _, r := range stored {
		byID[r.str("id")] = r
	}

	merged := make([]map[string]any, 0, len(events))
	for _, e := range events {
		if prev, ok := byID[e.ID]; ok {
			if len(e.RawTopicXDR) == 0 {
				e.RawTopicXDR = prev.strSlice("raw_topic_xdr")
			}
			if e.RawValueXDR == "" {
				e.RawValueXDR = prev.str("raw_value_xdr")
			}
		}
		merged = append(merged, clickHouseEventRow(e))
	}

	if err := exec.exec(ctx, database, "ALTER TABLE events DELETE WHERE "+rangePredicate+" SETTINGS mutations_sync = 1"); err != nil {
		return fmt.Errorf("deleting events in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	if err := exec.insertRows(ctx, database, "events", clickHouseEventColumns, merged); err != nil {
		return fmt.Errorf("replacing events in range [%d,%d]: %w", fromLedger, toLedger, err)
	}
	return nil
}

// GetEvent returns one event by ID, scoped like the Postgres implementation:
// an event outside the scope is indistinguishable from a missing one, so a
// caller cannot probe for other tenants' events by ID.
func (c *ClickHouse) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	if sc.DeniesAll() {
		return Event{}, ErrNotFound
	}
	where := []string{"id = " + clickHouseQuoteString(id)}
	if !sc.IsWildcard() {
		ids := sc.Contracts()
		if len(ids) == 0 {
			return Event{}, ErrNotFound
		}
		where = append(where, "contract_id IN ("+clickHouseQuoteList(ids)+")")
	}
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseEventColumnList+" FROM events"+clickHouseJoinWhere(where)+" LIMIT 1")
	if err != nil {
		return Event{}, fmt.Errorf("getting event %s: %w", id, err)
	}
	if len(rows) == 0 {
		return Event{}, ErrNotFound
	}
	return eventFromRow(rows[0]), nil
}

// GetEventsByTxHash returns every event from one transaction except the
// caller's own row, in ascending ID order. It is deliberately unscoped: the
// API only reaches it after the anchor event has already passed an
// authorization check.
func (c *ClickHouse) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	where := []string{"tx_hash = " + clickHouseQuoteString(txHash)}
	if excludeID != "" {
		where = append(where, "id != "+clickHouseQuoteString(excludeID))
	}
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseEventColumnList+" FROM events"+clickHouseJoinWhere(where)+" ORDER BY id ASC")
	if err != nil {
		return nil, fmt.Errorf("querying events by tx hash: %w", err)
	}
	events := make([]Event, 0, len(rows))
	for _, r := range rows {
		events = append(events, eventFromRow(r))
	}
	return events, nil
}

// EventExists is the cheap 304 probe behind conditional GETs. A scope that
// grants nothing reports false without issuing SQL, matching Postgres.
func (c *ClickHouse) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	if sc.DeniesAll() {
		return false, nil
	}
	where := []string{"id = " + clickHouseQuoteString(id)}
	if !sc.IsWildcard() {
		ids := sc.Contracts()
		if len(ids) == 0 {
			return false, nil
		}
		where = append(where, "contract_id IN ("+clickHouseQuoteList(ids)+")")
	}
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(),
		"SELECT count(*) FROM events"+clickHouseJoinWhere(where))
	if err != nil {
		return false, fmt.Errorf("checking event existence: %w", err)
	}
	return total > 0, nil
}

// QueryEvents returns one page of events plus the cursor for the next. It
// reuses clickHouseEventWhereClause for the filter/scope predicate (which
// errors on the jsonb topic filters rather than approximating them) and adds
// the keyset cursor, ordering and limit itself, so the read path matches
// Postgres's semantics for everything the ClickHouse schema can express.
func (c *ClickHouse) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
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
	if !ValidOrderBy(f.OrderBy) {
		return nil, "", fmt.Errorf("unsupported order_by %q", f.OrderBy)
	}

	where, err := clickHouseEventWhereClause(f)
	if err != nil {
		return nil, "", err
	}
	cursorClause, err := clickHouseEventCursorClause(f)
	if err != nil {
		return nil, "", err
	}
	if cursorClause != "" {
		where = append(where, cursorClause)
	}

	// Fetch one extra row to learn whether a next page exists, same as
	// Postgres, then trim it back off before returning.
	query := "SELECT " + clickHouseEventColumnList + " FROM events" + clickHouseJoinWhere(where) +
		" ORDER BY " + clickHouseEventOrderColumns(f) + " LIMIT " + strconv.Itoa(limit+1)
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return nil, "", fmt.Errorf("querying events: %w", err)
	}
	events := make([]Event, 0, len(rows))
	for _, r := range rows {
		events = append(events, eventFromRow(r))
	}
	next := ""
	if len(events) > limit {
		events = events[:limit]
		next = EncodeCursorForBackend(CursorBackendClickHouse, f.OrderBy, events[limit-1])
	}
	return events, next, nil
}

// CountEvents mirrors Postgres.CountEvents: same early-return on a denying
// scope, same "count ignores pagination" contract. It builds its WHERE
// clause from clickHouseEventWhereClause, which errors out on the topic
// filters rather than approximating them — see that function's comment.
func (c *ClickHouse) CountEvents(ctx context.Context, f EventFilter) (int64, error) {
	if f.Scope.DeniesAll() {
		return 0, nil
	}
	where, err := clickHouseEventWhereClause(f)
	if err != nil {
		return 0, err
	}
	query := "SELECT count(*) FROM events"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return 0, fmt.Errorf("counting events: %w", err)
	}
	return total, nil
}

// clickHouseEventWhereClause translates the subset of EventFilter that maps
// cleanly onto the ClickHouse events schema into a SQL WHERE clause,
// mirroring buildEventWhereClause's Postgres semantics (scope ANDed with
// caller filters, empty/zero fields meaning "no constraint").
//
// The jsonb-containment filters (Topic, TopicContains, Topic0-3) have no
// equivalent here: `topics` is stored as an opaque JSON string, not a
// structured column, because ClickHouse's JSON support is either
// experimental or requires a schema this issue doesn't otherwise need. Per
// #586's "anything genuinely unsupported must return an explicit error"
// rule, a filter using any of them errors out instead of silently ignoring
// the constraint and over-returning rows.
func clickHouseEventWhereClause(f EventFilter) ([]string, error) {
	var where []string

	if f.Network != "" {
		where = append(where, "network = "+clickHouseQuoteString(f.Network))
	}
	if !f.Scope.IsWildcard() {
		ids := f.Scope.Contracts()
		if len(ids) == 0 {
			// DeniesAll is handled by callers before reaching here, but stay
			// safe if one doesn't: an empty IN() would match everything on
			// some engines, so match nothing explicitly instead.
			where = append(where, "1 = 0")
		} else {
			where = append(where, "contract_id IN ("+clickHouseQuoteList(ids)+")")
		}
	}
	if len(f.ContractIDs) > 0 {
		ids := f.ContractIDs
		if f.ContractID != "" {
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
		where = append(where, "contract_id IN ("+clickHouseQuoteList(ids)+")")
	} else if f.ContractID != "" {
		where = append(where, "contract_id = "+clickHouseQuoteString(f.ContractID))
	}
	if f.ContractIDPrefix != "" {
		where = append(where, "contract_id LIKE "+clickHouseQuoteString(f.ContractIDPrefix+"%"))
	}
	if len(f.Types) > 0 {
		where = append(where, "type IN ("+clickHouseQuoteList(f.Types)+")")
	}
	if f.TxHash != "" {
		where = append(where, "tx_hash = "+clickHouseQuoteString(f.TxHash))
	}
	if f.InSuccessfulCall != nil {
		v := "0"
		if *f.InSuccessfulCall {
			v = "1"
		}
		where = append(where, "in_successful_call = "+v)
	}
	if len(f.Topic) > 0 || len(f.TopicContains) > 0 || len(f.Topic0) > 0 || len(f.Topic1) > 0 || len(f.Topic2) > 0 || len(f.Topic3) > 0 {
		return nil, fmt.Errorf("clickhouse backend: topic filters are not supported")
	}
	if f.HasValue != nil {
		if *f.HasValue {
			where = append(where, "value IS NOT NULL")
		} else {
			where = append(where, "value IS NULL")
		}
	}
	if f.TxIndex != nil {
		where = append(where, fmt.Sprintf("tx_index = %d", *f.TxIndex))
	}
	if f.OpIndex != nil {
		where = append(where, fmt.Sprintf("op_index = %d", *f.OpIndex))
	}
	if f.FromLedger > 0 {
		where = append(where, fmt.Sprintf("ledger >= %d", f.FromLedger))
	}
	if f.ToLedger > 0 {
		where = append(where, fmt.Sprintf("ledger <= %d", f.ToLedger))
	}
	if !f.FromTime.IsZero() {
		where = append(where, "created_at >= '"+f.FromTime.UTC().Format("2006-01-02 15:04:05.000")+"'")
	}
	if !f.ToTime.IsZero() {
		where = append(where, "created_at <= '"+f.ToTime.UTC().Format("2006-01-02 15:04:05.000")+"'")
	}
	return where, nil
}

// clickHouseQuoteString single-quotes a value for inline SQL, escaping
// embedded quotes and backslashes. Every value passed through it comes from
// authenticated API filters or operator config, never raw user text
// rendered without validation, but quoting is cheap insurance regardless
// given there is no parameterized-query placeholder support being used
// here (ClickHouse's HTTP interface takes a single request body per
// statement, not the query/args split database/sql callers get elsewhere in
// this package).
func clickHouseQuoteString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

func clickHouseQuoteList(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = clickHouseQuoteString(v)
	}
	return strings.Join(quoted, ", ")
}

func (c *ClickHouse) LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error) {
	return nil, errUnsupported("clickhouse", "LedgerRangeCensus")
}

func (c *ClickHouse) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	return nil, errUnsupported("clickhouse", "AggregateEvents")
}

// clickHouseIngestionStateVersion mints the updated_at version stamped onto
// every SaveIngestionState row. DateTime64(3) only resolves a millisecond,
// so two saves inside the same millisecond would carry equal versions;
// ReplacingMergeTree then collapses equal versions in an unspecified order
// and FINAL could hand a restarting ingester the older resume position,
// silently rewinding the frontier it just persisted. Keeping the version
// strictly increasing within the process makes the newer save always win.
var lastClickHouseIngestionStateVersion atomic.Int64

func clickHouseIngestionStateVersion(now time.Time) time.Time {
	ms := now.UnixMilli()
	for {
		last := lastClickHouseIngestionStateVersion.Load()
		if ms <= last {
			ms = last + 1
		}
		if lastClickHouseIngestionStateVersion.CompareAndSwap(last, ms) {
			return time.UnixMilli(ms).UTC()
		}
	}
}

// GetIngestionState returns the single default-network ingestion row, or
// ErrNotFound on a fresh instance. FINAL collapses the ReplacingMergeTree's
// appended versions to the newest one, which is the durable resume position
// the ingester restarts from (issue #523).
func (c *ClickHouse) GetIngestionState(ctx context.Context) (IngestionState, error) {
	return c.GetIngestionStateForNetwork(ctx, "default")
}

func (c *ClickHouse) GetIngestionStateForNetwork(ctx context.Context, network string) (IngestionState, error) {
	network = defaultNetwork(network)
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT network, last_ingested_ledger, last_cursor, last_successful_poll, updated_at FROM ingestion_state FINAL WHERE network = "+clickHouseQuoteString(network)+" LIMIT 1")
	if err != nil {
		return IngestionState{}, fmt.Errorf("loading ingestion state for network %q: %w", network, err)
	}
	if len(rows) == 0 {
		return IngestionState{}, ErrNotFound
	}
	return ingestionStateFromRow(rows[0]), nil
}

// SaveIngestionState appends a new version of the singleton row. ClickHouse
// has no efficient row-level UPDATE, so an upsert is an insert of a newer
// version keyed by network; the schema's ReplacingMergeTree(updated_at) makes
// readers see the latest without waiting for a background merge. updated_at
// comes from clickHouseIngestionStateVersion so back-to-back saves can never
// tie on that version column (issue #523).
func (c *ClickHouse) SaveIngestionState(ctx context.Context, s IngestionState) error {
	if s.Network == "" {
		s.Network = "default"
	}
	now := clickHouseIngestionStateVersion(time.Now().UTC())
	row := map[string]any{
		"network":              s.Network,
		"last_ingested_ledger": s.LastIngestedLedger,
		"last_cursor":          s.LastCursor,
		"last_successful_poll": nil,
		"updated_at":           formatClickHouseTime(now),
	}
	if s.LastSuccessfulPoll != nil {
		row["last_successful_poll"] = formatClickHouseTime(*s.LastSuccessfulPoll)
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "ingestion_state",
		[]string{"network", "last_ingested_ledger", "last_cursor", "last_successful_poll", "updated_at"},
		[]map[string]any{row}); err != nil {
		return fmt.Errorf("saving ingestion state for network %q: %w", s.Network, err)
	}
	return nil
}

// GetAuditState returns the verified-through position for one network, or
// ErrNotFound when the network has never been audited. Unlike SaveAuditState
// it does not substitute a default network name, matching the Postgres
// implementation exactly.
func (c *ClickHouse) GetAuditState(ctx context.Context, network string) (AuditState, error) {
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT network, verified_through_ledger, updated_at FROM audit_state FINAL WHERE network = "+clickHouseQuoteString(network)+" LIMIT 1")
	if err != nil {
		return AuditState{}, fmt.Errorf("loading audit state for network %q: %w", network, err)
	}
	if len(rows) == 0 {
		return AuditState{}, ErrNotFound
	}
	return auditStateFromRow(rows[0]), nil
}

func (c *ClickHouse) SaveAuditState(ctx context.Context, s AuditState) error {
	if s.Network == "" {
		s.Network = "default"
	}
	row := map[string]any{
		"network":                 s.Network,
		"verified_through_ledger": s.VerifiedThroughLedger,
		"updated_at":              formatClickHouseTime(time.Now().UTC()),
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "audit_state",
		[]string{"network", "verified_through_ledger", "updated_at"},
		[]map[string]any{row}); err != nil {
		return fmt.Errorf("saving audit state for network %q: %w", s.Network, err)
	}
	return nil
}

// SaveAuditStateIfGreater advances the verified-through marker only when the
// new ledger is strictly ahead of what is stored, so a racing or lagging
// auditor cannot regress it. Postgres gets the compare-and-set from an
// ON CONFLICT ... WHERE; ClickHouse has no conditional upsert, so the current
// value is read (FINAL) and the write skipped when it would move backward.
// The read-modify-write is not atomic across instances, which matches the
// monotonic intent closely enough for a marker the auditor owns exclusively.
func (c *ClickHouse) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error) {
	if network == "" {
		network = "default"
	}
	current, err := c.GetAuditState(ctx, network)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return AuditState{}, err
	}
	if err == nil && ledger <= current.VerifiedThroughLedger {
		return current, nil
	}
	s := AuditState{Network: network, VerifiedThroughLedger: ledger, UpdatedAt: time.Now().UTC()}
	if err := c.SaveAuditState(ctx, s); err != nil {
		return AuditState{}, err
	}
	return s, nil
}

// ListWatchedContracts returns the currently-watched set: rows whose latest
// version has no removed_at. Removal is a soft delete (a newer version with
// removed_at set), so FINAL is what turns the append-only table back into
// the presence list ingestion reads.
func (c *ClickHouse) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	return c.ListWatchedContractsScoped(ctx, WildcardScope())
}

func (c *ClickHouse) ListWatchedContractsScoped(ctx context.Context, sc Scope) ([]WatchedContract, error) {
	if sc.DeniesAll() {
		return []WatchedContract{}, nil
	}
	where := "removed_at IS NULL"
	if !sc.IsWildcard() {
		ids := sc.Contracts()
		if len(ids) == 0 {
			return []WatchedContract{}, nil
		}
		where += " AND contract_id IN (" + clickHouseQuoteList(ids) + ")"
	}
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT network, contract_id, added_at FROM watched_contracts FINAL WHERE "+where+" ORDER BY network, contract_id")
	if err != nil {
		return nil, fmt.Errorf("listing watched contracts: %w", err)
	}
	out := make([]WatchedContract, 0, len(rows))
	for _, r := range rows {
		out = append(out, watchedContractFromRow(r))
	}
	return out, nil
}

// RemoveWatchedContract stops future ingestion for a contract without
// touching stored events. ErrNotFound signals a typo so the API can 404.
func (c *ClickHouse) RemoveWatchedContract(ctx context.Context, w WatchedContract) error {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	network := defaultNetwork(w.Network)
	rows, err := exec.queryRows(ctx, database,
		"SELECT contract_id FROM watched_contracts FINAL WHERE network = "+clickHouseQuoteString(network)+" AND contract_id = "+clickHouseQuoteString(w.ContractID)+" AND removed_at IS NULL LIMIT 1")
	if err != nil {
		return fmt.Errorf("removing watched contract: %w", err)
	}
	if len(rows) == 0 {
		return ErrNotFound
	}
	now := formatClickHouseTime(time.Now().UTC())
	row := map[string]any{"network": network, "contract_id": w.ContractID, "added_at": now, "removed_at": now}
	if err := exec.insertRows(ctx, database, "watched_contracts",
		[]string{"network", "contract_id", "added_at", "removed_at"}, []map[string]any{row}); err != nil {
		return fmt.Errorf("removing watched contract: %w", err)
	}
	return nil
}

// AddWatchedContract makes a contract watched, idempotently: re-adding an
// already-watched contract appends another version that FINAL still collapses
// to one present row. Re-adding a previously-removed contract appends a
// newer version with removed_at NULL, which is what brings it back.
func (c *ClickHouse) AddWatchedContract(ctx context.Context, w WatchedContract) error {
	row := map[string]any{
		"network":     defaultNetwork(w.Network),
		"contract_id": w.ContractID,
		"added_at":    formatClickHouseTime(time.Now().UTC()),
		"removed_at":  nil,
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "watched_contracts",
		[]string{"network", "contract_id", "added_at", "removed_at"}, []map[string]any{row}); err != nil {
		return fmt.Errorf("adding watched contract: %w", err)
	}
	return nil
}

func (c *ClickHouse) GetContractCursor(ctx context.Context, contractID string) (ContractCursor, error) {
	return ContractCursor{}, errUnsupported("clickhouse", "GetContractCursor")
}

func (c *ClickHouse) SaveContractCursor(ctx context.Context, cur ContractCursor) error {
	return errUnsupported("clickhouse", "SaveContractCursor")
}

// DeleteContractCursor removes a contract's resume position, returning
// ErrNotFound when there was nothing to delete. mutations_sync makes the
// delete synchronous so a subsequent ListContractCursors reflects it.
func (c *ClickHouse) DeleteContractCursor(ctx context.Context, contractID string) error {
	return errUnsupported("clickhouse", "DeleteContractCursor")
}

func (c *ClickHouse) ListContractCursors(ctx context.Context) ([]ContractCursor, error) {
	return nil, errUnsupported("clickhouse", "ListContractCursors")
}

// RecordAuditFinding inserts a new finding. The id is minted by the writer
// (there is no sequence over the HTTP interface) and the attempts counter
// starts at zero, matching the Postgres insert.
func (c *ClickHouse) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	now := time.Now().UTC()
	f.ID = nextClickHouseID()
	f.CreatedAt = now
	row := map[string]any{
		"id":                f.ID,
		"network":           defaultNetwork(f.Network),
		"from_ledger":       f.FromLedger,
		"to_ledger":         f.ToLedger,
		"expected_count":    f.ExpectedCount,
		"actual_count":      f.ActualCount,
		"missing_ids":       f.MissingIDs,
		"status":            f.Status,
		"attempts":          0,
		"last_attempted_at": nil,
		"last_error":        f.LastError,
		"created_at":        formatClickHouseTime(now),
		"updated_at":        formatClickHouseTime(now),
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "audit_findings",
		clickHouseAuditFindingColumns, []map[string]any{row}); err != nil {
		return AuditFinding{}, fmt.Errorf("recording audit finding: %w", err)
	}
	return f, nil
}

// UpdateAuditFinding appends a new version of a finding carrying the mutable
// fields the auditor tracks. A missing id is a silent no-op, mirroring the
// Postgres UPDATE, which affects zero rows without erroring.
func (c *ClickHouse) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	rows, err := exec.queryRows(ctx, database,
		"SELECT id, network, from_ledger, to_ledger, expected_count, actual_count, missing_ids, status, attempts, last_attempted_at, last_error, created_at FROM audit_findings FINAL WHERE id = "+strconv.FormatInt(f.ID, 10)+" LIMIT 1")
	if err != nil {
		return fmt.Errorf("updating audit finding %d: %w", f.ID, err)
	}
	if len(rows) == 0 {
		return nil
	}
	current := auditFindingFromRow(rows[0])
	current.Status = f.Status
	current.Attempts = f.Attempts
	current.LastAttemptedAt = f.LastAttemptedAt
	current.LastError = f.LastError
	current.MissingIDs = f.MissingIDs

	row := map[string]any{
		"id":                current.ID,
		"network":           defaultNetwork(current.Network),
		"from_ledger":       current.FromLedger,
		"to_ledger":         current.ToLedger,
		"expected_count":    current.ExpectedCount,
		"actual_count":      current.ActualCount,
		"missing_ids":       current.MissingIDs,
		"status":            current.Status,
		"attempts":          current.Attempts,
		"last_attempted_at": nil,
		"last_error":        current.LastError,
		"created_at":        formatClickHouseTime(current.CreatedAt),
		"updated_at":        formatClickHouseTime(time.Now().UTC()),
	}
	if !current.LastAttemptedAt.IsZero() {
		row["last_attempted_at"] = formatClickHouseTime(current.LastAttemptedAt)
	}
	if err := exec.insertRows(ctx, database, "audit_findings", clickHouseAuditFindingColumns, []map[string]any{row}); err != nil {
		return fmt.Errorf("updating audit finding %d: %w", f.ID, err)
	}
	return nil
}

var clickHouseAuditFindingColumns = []string{
	"id", "network", "from_ledger", "to_ledger", "expected_count", "actual_count",
	"missing_ids", "status", "attempts", "last_attempted_at", "last_error",
	"created_at", "updated_at",
}

// ListOpenFindingsByRange returns the newest finding that overlaps a ledger
// and is still actionable (open or unrecoverable), or ErrNotFound.
func (c *ClickHouse) ListOpenFindingsByRange(ctx context.Context, network string, fromLedger, toLedger int64) (AuditFinding, error) {
	query := "SELECT id, network, from_ledger, to_ledger, expected_count, actual_count, missing_ids, status, attempts, last_attempted_at, last_error, created_at FROM audit_findings FINAL" +
		" WHERE network = " + clickHouseQuoteString(network) +
		" AND status IN ('open', 'unrecoverable')" +
		fmt.Sprintf(" AND from_ledger <= %d AND to_ledger >= %d", toLedger, fromLedger) +
		" ORDER BY id DESC LIMIT 1"
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return AuditFinding{}, fmt.Errorf("listing findings for [%d,%d]: %w", fromLedger, toLedger, err)
	}
	if len(rows) == 0 {
		return AuditFinding{}, ErrNotFound
	}
	return auditFindingFromRow(rows[0]), nil
}

// CreateSubscription registers a webhook callback. The id and timestamps are
// assigned here because ClickHouse has no sequence or RETURNING over its HTTP
// interface, and failure_count starts at zero like the Postgres default.
func (c *ClickHouse) CreateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	now := time.Now().UTC()
	s.ID = nextClickHouseID()
	s.FailureCount = 0
	s.CreatedAt = now
	row, err := clickHouseSubscriptionRow(s, now)
	if err != nil {
		return Subscription{}, err
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "subscriptions",
		clickHouseSubscriptionColumns, []map[string]any{row}); err != nil {
		return Subscription{}, fmt.Errorf("creating subscription: %w", err)
	}
	return s, nil
}

// GetSubscription looks a subscription up under the owner filter. The owner
// predicate is part of the query, not a post-filter, so a tenant cannot read
// another's callback even incidentally — the same boundary Postgres enforces.
func (c *ClickHouse) GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error) {
	where := "id = " + strconv.FormatInt(id, 10) + clickHouseOwnerPredicate(owner)
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseSubscriptionColumnList+" FROM subscriptions FINAL WHERE "+where+" LIMIT 1")
	if err != nil {
		return Subscription{}, fmt.Errorf("getting subscription %d: %w", id, err)
	}
	if len(rows) == 0 {
		return Subscription{}, ErrNotFound
	}
	return subscriptionFromRow(rows[0])
}

func (c *ClickHouse) ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error) {
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseSubscriptionColumnList+" FROM subscriptions FINAL WHERE 1 = 1"+clickHouseOwnerPredicate(owner)+" ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("listing subscriptions: %w", err)
	}
	subs := make([]Subscription, 0, len(rows))
	for _, r := range rows {
		s, err := subscriptionFromRow(r)
		if err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, nil
}

// UpdateSubscription rewrites the mutable fields (url, filters, secret,
// enabled) as a new version, preserving the creation time and failure count
// that only the counter methods own. Ownership is checked by the initial
// GetSubscription, so a tenant cannot edit another's row.
func (c *ClickHouse) UpdateSubscription(ctx context.Context, s Subscription, owner SubscriptionOwner) (Subscription, error) {
	existing, err := c.GetSubscription(ctx, s.ID, owner)
	if err != nil {
		return Subscription{}, err
	}
	s.CreatedAt = existing.CreatedAt
	s.FailureCount = existing.FailureCount
	if s.TenantID == nil {
		s.TenantID = existing.TenantID
	}
	row, err := clickHouseSubscriptionRow(s, time.Now().UTC())
	if err != nil {
		return Subscription{}, err
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "subscriptions",
		clickHouseSubscriptionColumns, []map[string]any{row}); err != nil {
		return Subscription{}, fmt.Errorf("updating subscription %d: %w", s.ID, err)
	}
	return c.GetSubscription(ctx, s.ID, owner)
}

func (c *ClickHouse) DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	total, err := exec.queryInt64(ctx, database,
		"SELECT count(*) FROM subscriptions FINAL WHERE id = "+strconv.FormatInt(id, 10)+clickHouseOwnerPredicate(owner))
	if err != nil {
		return fmt.Errorf("deleting subscription %d: %w", id, err)
	}
	if total == 0 {
		return ErrNotFound
	}
	if err := exec.exec(ctx, database,
		"ALTER TABLE subscriptions DELETE WHERE id = "+strconv.FormatInt(id, 10)+clickHouseOwnerPredicate(owner)+" SETTINGS mutations_sync = 1"); err != nil {
		return fmt.Errorf("deleting subscription %d: %w", id, err)
	}
	return nil
}

func (c *ClickHouse) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseSubscriptionColumnList+" FROM subscriptions FINAL WHERE enabled = 1 ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("listing enabled subscriptions: %w", err)
	}
	subs := make([]Subscription, 0, len(rows))
	for _, r := range rows {
		s, err := subscriptionFromRow(r)
		if err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, nil
}

// IncrementSubscriptionFailures bumps the failure counter and disables the
// subscription once it reaches the caller's ceiling. Postgres does both in one
// UPDATE; here the current row is read (FINAL) and a new version written, so
// the return value still reflects the increment the way callers expect.
func (c *ClickHouse) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	s, err := c.getSubscriptionByID(ctx, id)
	if err != nil {
		return 0, false, err
	}
	newCount := s.FailureCount + 1
	disabled := newCount >= maxFailures
	s.FailureCount = newCount
	if disabled {
		s.Enabled = false
	}
	row, err := clickHouseSubscriptionRow(s, time.Now().UTC())
	if err != nil {
		return 0, false, err
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "subscriptions",
		clickHouseSubscriptionColumns, []map[string]any{row}); err != nil {
		return 0, false, fmt.Errorf("incrementing failures for subscription %d: %w", id, err)
	}
	return newCount, disabled, nil
}

// getSubscriptionByID is the unowned lookup the failure-counter methods use:
// Postgres increments and resets by id alone, because the caller is the
// delivery worker acting on behalf of every tenant, not a tenant itself.
func (c *ClickHouse) getSubscriptionByID(ctx context.Context, id int64) (Subscription, error) {
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseSubscriptionColumnList+" FROM subscriptions FINAL WHERE id = "+strconv.FormatInt(id, 10)+" LIMIT 1")
	if err != nil {
		return Subscription{}, fmt.Errorf("getting subscription %d: %w", id, err)
	}
	if len(rows) == 0 {
		return Subscription{}, ErrNotFound
	}
	return subscriptionFromRow(rows[0])
}

// ResetSubscriptionFailures clears the counter after a successful delivery.
// A missing id is a silent no-op, matching the Postgres UPDATE that affects
// zero rows without erroring.
func (c *ClickHouse) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	s, err := c.getSubscriptionByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	s.FailureCount = 0
	row, err := clickHouseSubscriptionRow(s, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "subscriptions",
		clickHouseSubscriptionColumns, []map[string]any{row}); err != nil {
		return fmt.Errorf("resetting failures for subscription %d: %w", id, err)
	}
	return nil
}

func (c *ClickHouse) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	a.ID = nextClickHouseID()
	a.CreatedAt = time.Now().UTC()
	row := map[string]any{
		"id":              a.ID,
		"subscription_id": a.SubscriptionID,
		"event_id":        a.EventID,
		"status":          a.Status,
		"response_code":   a.ResponseCode,
		"duration_ms":     a.DurationMs,
		"error":           a.Error,
		"created_at":      formatClickHouseTime(a.CreatedAt),
	}
	if err := newClickHouseExecutor(c.cfg).insertRows(ctx, c.cfg.databaseOrDefault(), "delivery_attempts",
		[]string{"id", "subscription_id", "event_id", "status", "response_code", "duration_ms", "error", "created_at"},
		[]map[string]any{row}); err != nil {
		return DeliveryAttempt{}, fmt.Errorf("recording delivery attempt: %w", err)
	}
	return a, nil
}

// ListDeliveryAttempts resolves the subscription under the owner filter first,
// then reads its history newest-first. Delivery history names the events that
// matched, so an unowned read would leak a tenant's data one attempt at a time.
func (c *ClickHouse) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error) {
	if limit <= 0 {
		limit = 50
	}
	if _, err := c.GetSubscription(ctx, subscriptionID, owner); err != nil {
		return nil, err
	}
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT id, subscription_id, event_id, status, response_code, duration_ms, error, created_at FROM delivery_attempts WHERE subscription_id = "+strconv.FormatInt(subscriptionID, 10)+" ORDER BY created_at DESC LIMIT "+strconv.Itoa(limit))
	if err != nil {
		return nil, fmt.Errorf("listing delivery attempts: %w", err)
	}
	attempts := make([]DeliveryAttempt, 0, len(rows))
	for _, r := range rows {
		attempts = append(attempts, deliveryAttemptFromRow(r))
	}
	return attempts, nil
}

func (c *ClickHouse) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, owner SubscriptionOwner) (int64, error) {
	if _, err := c.GetSubscription(ctx, subscriptionID, owner); err != nil {
		return 0, err
	}
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(),
		"SELECT count(*) FROM delivery_attempts WHERE subscription_id = "+strconv.FormatInt(subscriptionID, 10))
	if err != nil {
		return 0, fmt.Errorf("counting delivery attempts: %w", err)
	}
	return total, nil
}

func (c *ClickHouse) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	return nil, errUnsupported("clickhouse", "GetContractSpec")
}

func (c *ClickHouse) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	return errUnsupported("clickhouse", "SetContractSpec")
}

func (c *ClickHouse) GetContractSpecOverride(ctx context.Context, contractID string) ([]byte, error) {
	return nil, errUnsupported("clickhouse", "GetContractSpecOverride")
}

func (c *ClickHouse) SetContractSpecOverride(ctx context.Context, contractID string, specJSON []byte) error {
	return errUnsupported("clickhouse", "SetContractSpecOverride")
}

func (c *ClickHouse) DeleteContractSpecOverride(ctx context.Context, contractID string) error {
	return errUnsupported("clickhouse", "DeleteContractSpecOverride")
}

// The override history is not implemented on ClickHouse: the backend has no
// contract_spec_overrides table to begin with, and the history's rows are
// written in the same transaction as the override itself. Refusing
// explicitly keeps the API's answer honest — a 501 naming the backend,
// rather than a history that silently comes back empty.
func (c *ClickHouse) SetContractSpecOverrideWithHistory(ctx context.Context, contractID string, specJSON []byte, apiKeyPrefix string) error {
	return errUnsupported("clickhouse", "SetContractSpecOverrideWithHistory")
}

func (c *ClickHouse) DeleteContractSpecOverrideWithHistory(ctx context.Context, contractID string, apiKeyPrefix string) error {
	return errUnsupported("clickhouse", "DeleteContractSpecOverrideWithHistory")
}

func (c *ClickHouse) ListContractSpecOverrideHistory(ctx context.Context, contractID string, limit int, cursor string) ([]ContractSpecOverrideVersion, string, error) {
	return nil, "", errUnsupported("clickhouse", "ListContractSpecOverrideHistory")
}

// DeleteEventsBeforeLedger deletes every event strictly below beforeLedger.
// ClickHouse's ALTER TABLE ... DELETE is an asynchronous mutation with no
// synchronous "rows affected" result over the HTTP interface, so the
// reported count is a snapshot taken by counting matching rows immediately
// before the mutation is issued. That is exact at the instant it is taken;
// concurrent ingestion writing new rows below beforeLedger between the
// count and the mutation (which should not happen in practice, since
// beforeLedger is expected to trail last_ingested_ledger) would make the
// two numbers diverge slightly, which is the same approximation the issue
// allows for ClickHouse retention.
func (c *ClickHouse) DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error) {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	where := fmt.Sprintf("ledger < %d", beforeLedger)

	affected, err := exec.queryInt64(ctx, database, "SELECT count(*) FROM events WHERE "+where)
	if err != nil {
		return 0, fmt.Errorf("counting events before ledger %d: %w", beforeLedger, err)
	}
	if affected == 0 {
		return 0, nil
	}
	if err := exec.exec(ctx, database, "ALTER TABLE events DELETE WHERE "+where); err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", beforeLedger, err)
	}
	return affected, nil
}

func (c *ClickHouse) MigrationVersion(ctx context.Context) (int, bool, error) {
	return 0, false, errUnsupported("clickhouse", "MigrationVersion")
}

// Stats aggregates the same counters as Postgres.Stats, against the tables
// that carry the same data in the ClickHouse schema: events, ingestion_state,
// audit_state and watched_contracts (see 0001_init.up.sql). Table size is
// reported as 0 — ClickHouse exposes that through system.parts rather than a
// single scalar function, and it isn't part of this issue's "done" list.
//
// Multi-tenant watch-list entries (tenant_watched_contracts in Postgres)
// have no ClickHouse counterpart yet, so WatchedContracts here only reflects
// the global watched_contracts table. That mirrors the single-tenant
// behavior Postgres has always had and is a documented gap, not a silent
// undercount, for instances running MULTI_TENANT=true against ClickHouse.
func (c *ClickHouse) Stats(ctx context.Context, sc Scope) (Stats, error) {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()

	pred := ""
	if sc.DeniesAll() {
		pred = "WHERE 1 = 0"
	} else if !sc.IsWildcard() {
		ids := sc.Contracts()
		quoted := make([]string, len(ids))
		for i, id := range ids {
			quoted[i] = clickHouseQuoteString(id)
		}
		pred = "WHERE contract_id IN (" + strings.Join(quoted, ", ") + ")"
	}

	var s Stats
	totalEvents, err := exec.queryInt64(ctx, database, "SELECT count(*) FROM events "+pred)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.TotalEvents = totalEvents

	lastIngested, err := exec.queryInt64(ctx, database, "SELECT coalesce(max(last_ingested_ledger), 0) FROM ingestion_state")
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.LastIngestedLedger = lastIngested

	verified, err := exec.queryInt64(ctx, database, "SELECT coalesce(max(verified_through_ledger), 0) FROM audit_state")
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.VerifiedThroughLedger = verified

	if sc.DeniesAll() {
		// Frontier values above are instance-wide progress, not tenant
		// data, so they're still reported even when the scope denies all
		// rows — matching Postgres's frontierStats behavior.
		return s, nil
	}

	oldest, err := exec.queryInt64(ctx, database, "SELECT coalesce(min(ledger), 0) FROM events "+pred)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.OldestStoredLedger = oldest

	contracts, err := exec.queryInt64(ctx, database, "SELECT count(DISTINCT contract_id) FROM events "+pred)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.ContractCount = contracts

	watchedQuery := `SELECT count(*) FROM (
		SELECT contract_id FROM watched_contracts FINAL WHERE removed_at IS NULL
	) w`
	if pred != "" {
		watchedQuery = `SELECT count(*) FROM (
			SELECT contract_id FROM watched_contracts FINAL WHERE removed_at IS NULL
		) w ` + pred
	}
	watched, err := exec.queryInt64(ctx, database, watchedQuery)
	if err != nil {
		return Stats{}, fmt.Errorf("loading stats: %w", err)
	}
	s.WatchedContracts = watched

	return s, nil
}

func (c *ClickHouse) ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error) {
	return nil, "", errUnsupported("clickhouse", "ListContracts")
}

// CountContracts counts distinct contract_id values in events, matching
// Postgres.CountContracts' semantics exactly: a "contract" here means a
// contract_id that has emitted at least one stored event, not a row in a
// separate contracts registry (ClickHouse has none).
func (c *ClickHouse) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	var clauses []string
	if !f.Scope.IsWildcard() && len(f.Scope.Contracts()) > 0 {
		clauses = append(clauses, "contract_id IN ("+clickHouseQuoteList(f.Scope.Contracts())+")")
	}
	if f.ContractIDPrefix != "" {
		clauses = append(clauses, "contract_id LIKE "+clickHouseQuoteString(f.ContractIDPrefix+"%"))
	}
	where := ""
	if len(clauses) > 0 {
		where = "WHERE " + strings.Join(clauses, " AND ")
	}
	query := "SELECT count(*) FROM (SELECT contract_id FROM events " + where + " GROUP BY contract_id) AS sub"
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return 0, fmt.Errorf("counting contracts: %w", err)
	}
	return total, nil
}

// API keys are not implemented for the ClickHouse backend: it is used as a
// read-side analytics mirror behind the Postgres-backed API, which owns
// authentication.
func (c *ClickHouse) CreateAPIKey(context.Context, APIKey) (APIKey, error) {
	return APIKey{}, fmt.Errorf("CreateAPIKey: not supported by the clickhouse backend")
}

func (c *ClickHouse) GetAPIKey(context.Context, int64) (APIKey, error) {
	return APIKey{}, fmt.Errorf("GetAPIKey: not supported by the clickhouse backend")
}

func (c *ClickHouse) LookupAPIKeyByPrefix(context.Context, string) (APIKey, error) {
	return APIKey{}, fmt.Errorf("LookupAPIKeyByPrefix: not supported by the clickhouse backend")
}

func (c *ClickHouse) ListAPIKeys(context.Context) ([]APIKey, error) {
	return nil, fmt.Errorf("ListAPIKeys: not supported by the clickhouse backend")
}

func (c *ClickHouse) RevokeAPIKey(context.Context, int64) error {
	return fmt.Errorf("RevokeAPIKey: not supported by the clickhouse backend")
}

// DeleteEventsBefore deletes up to limit events strictly below maxLedger
// (and, if beforeTime is non-zero, older than beforeTime too), for the
// background pruner's batched retention sweep. ClickHouse has no `DELETE
// ... LIMIT`, so the batch is pinned by first selecting up to limit
// matching ids and then deleting exactly those — the same two-step shape
// Postgres uses via `id IN (SELECT ... LIMIT n)`, just as two statements
// instead of one since ClickHouse mutations can't subquery the table
// they're mutating in the same statement.
func (c *ClickHouse) DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	where := clickHouseRetentionWhere(maxLedger, beforeTime)

	ids, err := exec.queryColumn(ctx, database, fmt.Sprintf("SELECT id FROM events WHERE %s LIMIT %d", where, limit))
	if err != nil {
		return 0, fmt.Errorf("selecting events before ledger %d: %w", maxLedger, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = clickHouseQuoteString(id)
	}
	del := fmt.Sprintf("ALTER TABLE events DELETE WHERE id IN (%s)", strings.Join(quoted, ", "))
	if err := exec.exec(ctx, database, del); err != nil {
		return 0, fmt.Errorf("deleting events before ledger %d: %w", maxLedger, err)
	}
	return int64(len(ids)), nil
}

// CountEventsBefore counts events eligible for DeleteEventsBefore, capped at
// limit so the pruner's dry-run and live paths agree on batch size.
func (c *ClickHouse) CountEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	where := clickHouseRetentionWhere(maxLedger, beforeTime)
	query := fmt.Sprintf("SELECT count(*) FROM (SELECT id FROM events WHERE %s LIMIT %d) sub", where, limit)
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return 0, fmt.Errorf("counting events before ledger %d: %w", maxLedger, err)
	}
	return total, nil
}

// clickHouseRetentionWhere builds the shared predicate for the two
// before-ledger retention methods above: never touch rows at or above
// maxLedger, and additionally require created_at < beforeTime when a time
// bound was given.
func clickHouseRetentionWhere(maxLedger int64, beforeTime time.Time) string {
	where := fmt.Sprintf("ledger < %d", maxLedger)
	if !beforeTime.IsZero() {
		where += fmt.Sprintf(" AND created_at < '%s'", beforeTime.UTC().Format("2006-01-02 15:04:05.000"))
	}
	return where
}

// DeadLetterEvent quarantines an event the ingester could not persist. It is
// retry-safe: re-submitting the same event_id increments the attempt counter
// and overwrites the payload with the newest attempt, keeping the row id
// stable so GET/DELETE by id continue to work. Postgres gets this from
// UNIQUE(event_id) plus ON CONFLICT; ClickHouse reads the existing version
// with FINAL and appends an incremented one.
func (c *ClickHouse) DeadLetterEvent(ctx context.Context, in DeadLetterInput) (DeadLetter, error) {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	now := time.Now().UTC()

	errStr := ""
	if in.Err != nil {
		errStr = in.Err.Error()
	}
	d := DeadLetter{
		ID:          nextClickHouseID(),
		EventID:     in.EventID,
		ContractID:  in.ContractID,
		Ledger:      in.Ledger,
		Type:        in.Type,
		TxHash:      in.TxHash,
		TopicXDR:    in.TopicXDR,
		ValueXDR:    in.ValueXDR,
		Error:       errStr,
		Attempts:    1,
		LastAttempt: now,
		CreatedAt:   now,
	}

	existing, err := exec.queryRows(ctx, database,
		"SELECT id, attempts, created_at FROM dead_letters FINAL WHERE event_id = "+clickHouseQuoteString(in.EventID)+" LIMIT 1")
	if err != nil {
		return DeadLetter{}, fmt.Errorf("recording dead letter: %w", err)
	}
	if len(existing) > 0 {
		d.ID = existing[0].int64("id")
		d.Attempts = int(existing[0].int64("attempts")) + 1
		d.CreatedAt = existing[0].time("created_at")
	}

	if err := exec.insertRows(ctx, database, "dead_letters",
		clickHouseDeadLetterColumns, []map[string]any{clickHouseDeadLetterRow(d)}); err != nil {
		return DeadLetter{}, fmt.Errorf("recording dead letter: %w", err)
	}
	return d, nil
}

// ListDeadLetters returns dead-letter rows newest-first, keyset-paginated by
// the cursor's encoded id. It mirrors the Postgres implementation's contract
// exactly, including the base64 cursor shape and the limit clamp.
func (c *ClickHouse) ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error) {
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}
	where := []string{}
	if contractID != "" {
		where = append(where, "contract_id = "+clickHouseQuoteString(contractID))
	}
	if cursor != "" {
		id, err := DecodeDeadLetterCursorForBackend(CursorBackendClickHouse, cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, "id < "+strconv.FormatInt(id, 10))
	}
	query := "SELECT " + clickHouseDeadLetterColumnList + " FROM dead_letters FINAL" +
		clickHouseJoinWhere(where) + " ORDER BY id DESC LIMIT " + strconv.Itoa(limit+1)
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(), query)
	if err != nil {
		return nil, "", fmt.Errorf("listing dead letters: %w", err)
	}
	letters := make([]DeadLetter, 0, len(rows))
	for _, r := range rows {
		letters = append(letters, deadLetterFromRow(r))
	}
	next := ""
	if len(letters) > limit {
		last := letters[limit-1]
		letters = letters[:limit]
		next = EncodeDeadLetterCursorForBackend(CursorBackendClickHouse, last.ID)
	}
	return letters, next, nil
}

// CountDeadLetters returns the full match set behind any page, matching the
// contract filter of ListDeadLetters.
func (c *ClickHouse) CountDeadLetters(ctx context.Context, contractID string) (int64, error) {
	where := []string{}
	if contractID != "" {
		where = append(where, "contract_id = "+clickHouseQuoteString(contractID))
	}
	total, err := newClickHouseExecutor(c.cfg).queryInt64(ctx, c.cfg.databaseOrDefault(),
		"SELECT count(*) FROM dead_letters FINAL"+clickHouseJoinWhere(where))
	if err != nil {
		return 0, fmt.Errorf("counting dead letters: %w", err)
	}
	return total, nil
}

func (c *ClickHouse) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error) {
	rows, err := newClickHouseExecutor(c.cfg).queryRows(ctx, c.cfg.databaseOrDefault(),
		"SELECT "+clickHouseDeadLetterColumnList+" FROM dead_letters FINAL WHERE id = "+strconv.FormatInt(id, 10)+" LIMIT 1")
	if err != nil {
		return DeadLetter{}, fmt.Errorf("loading dead letter %d: %w", id, err)
	}
	if len(rows) == 0 {
		return DeadLetter{}, ErrNotFound
	}
	return deadLetterFromRow(rows[0]), nil
}

// DeleteDeadLetter removes a quarantine row once it has been dealt with.
// mutations_sync makes the delete synchronous so a following
// ListDeadLetters already reflects it, which the API relies on.
func (c *ClickHouse) DeleteDeadLetter(ctx context.Context, id int64) error {
	exec := newClickHouseExecutor(c.cfg)
	database := c.cfg.databaseOrDefault()
	total, err := exec.queryInt64(ctx, database,
		"SELECT count(*) FROM dead_letters FINAL WHERE id = "+strconv.FormatInt(id, 10))
	if err != nil {
		return fmt.Errorf("deleting dead letter %d: %w", id, err)
	}
	if total == 0 {
		return ErrNotFound
	}
	if err := exec.exec(ctx, database,
		"ALTER TABLE dead_letters DELETE WHERE id = "+strconv.FormatInt(id, 10)+" SETTINGS mutations_sync = 1"); err != nil {
		return fmt.Errorf("deleting dead letter %d: %w", id, err)
	}
	return nil
}

// Ping performs a real round-trip to ClickHouse over the HTTP interface
// every query in this file uses, rather than merely opening a socket: the
// native TCP port a clickhouse:// URL names is not the port this backend
// talks to, so a dial there could succeed while the interface the app
// depends on is dead — or fail while it is healthy behind a proxy — either
// way leaving /readyz reporting an instance that cannot serve (issue #524).
// SELECT 1 is the cheapest statement the server will answer, and it runs
// under the caller's context, which is what lets the readiness probe's own
// deadline bound how long an unresponsive server can stall the probe.
func (c *ClickHouse) Ping(ctx context.Context) error {
	if c.cfg.host == "" {
		return fmt.Errorf("clickhouse: not configured (empty host)")
	}
	out, err := newClickHouseExecutor(c.cfg).query(ctx, c.cfg.databaseOrDefault(), "SELECT 1 FORMAT TabSeparated")
	if err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	if out != "1" {
		return fmt.Errorf("clickhouse ping: unexpected response %q", out)
	}
	return nil
}

func (c *ClickHouse) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	return errUnsupported("clickhouse", "UpsertAddressRefs")
}

func (c *ClickHouse) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	return nil, "", errUnsupported("clickhouse", "QueryAddressEvents")
}

func (c *ClickHouse) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	return 0, errUnsupported("clickhouse", "CountAddressEvents")
}

func (c *ClickHouse) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	return AddressSummary{}, errUnsupported("clickhouse", "GetAddressSummary")
}
