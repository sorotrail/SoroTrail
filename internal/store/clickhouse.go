package store

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ClickHouse implements Store with clickhouse-go/v2.
// It is intentionally minimal for now and is wired through the same
// interface so the app can select it via DATABASE_URL.
//
// contributors: Ping performs a real TCP dial to the server so the
// readiness probe cannot report healthy against an unreachable database.
type ClickHouse struct {
	host string
	port int
	cfg  clickHouseConfig
}

var _ Store = (*ClickHouse)(nil)

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

// Contract metadata (token enrichment) is Postgres-only; the ClickHouse
// backend reports "not found"/empty so the enrichment worker stays a no-op.
func (c *ClickHouse) ListContractIDs(context.Context) ([]string, error) { return nil, nil }
func (c *ClickHouse) GetContractMeta(context.Context, string) (ContractMeta, error) {
	return ContractMeta{}, ErrNotFound
}
func (c *ClickHouse) UpsertContractMeta(context.Context, ContractMeta) error     { return nil }
func (c *ClickHouse) CountContractEvents(context.Context, string) (int64, error) { return 0, nil }

// GetContractSummary returns a single contract's summary from ClickHouse.
func (c *ClickHouse) GetContractSummary(ctx context.Context, contractID string) (ContractSummary, error) {
	// TODO: implement ClickHouse-specific query
	return ContractSummary{}, fmt.Errorf("GetContractSummary: not yet implemented for ClickHouse")
}

// ContractEventTypeCounts returns per-type event counts from ClickHouse.
func (c *ClickHouse) ContractEventTypeCounts(ctx context.Context, contractID string) ([]ContractEventTypeCount, error) {
	// TODO: implement ClickHouse-specific query
	return nil, fmt.Errorf("ContractEventTypeCounts: not supported by the clickhouse backend")
}

func (c *ClickHouse) ListContractsNeedingRefresh(context.Context, time.Time) ([]string, error) {
	return nil, nil
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
		return &ClickHouse{host: cfg.host, port: cfg.port, cfg: cfg}, nil
	}
	if strings.HasPrefix(databaseURL, "postgres://") || strings.HasPrefix(databaseURL, "postgresql://") {
		return &Postgres{}, nil
	}
	return nil, fmt.Errorf("unsupported database url scheme")
}

func (c *ClickHouse) UpsertEvents(ctx context.Context, events []Event) (int64, error) {
	return int64(len(events)), nil
}

func (c *ClickHouse) ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error {
	return nil
}

func (c *ClickHouse) GetEvent(ctx context.Context, id string, sc Scope) (Event, error) {
	return Event{}, ErrNotFound
}

func (c *ClickHouse) GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error) {
	return nil, nil
}

func (c *ClickHouse) EventExists(ctx context.Context, id string, sc Scope) (bool, error) {
	return false, nil
}

func (c *ClickHouse) QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error) {
	return nil, "", nil
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
	return nil, nil
}

func (c *ClickHouse) AggregateEvents(ctx context.Context, f EventFilter, bucket string) ([]AggregateBucket, error) {
	return nil, nil
}

func (c *ClickHouse) GetIngestionState(ctx context.Context) (IngestionState, error) {
	return IngestionState{}, nil
}

func (c *ClickHouse) SaveIngestionState(ctx context.Context, s IngestionState) error {
	return nil
}

func (c *ClickHouse) GetAuditState(ctx context.Context, network string) (AuditState, error) {
	return AuditState{}, nil
}

func (c *ClickHouse) SaveAuditState(ctx context.Context, s AuditState) error {
	return nil
}

func (c *ClickHouse) SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error) {
	return AuditState{}, nil
}

func (c *ClickHouse) ListWatchedContracts(ctx context.Context) ([]WatchedContract, error) {
	return nil, nil
}

func (c *ClickHouse) RemoveWatchedContract(ctx context.Context, contractID string) error {
	return nil
}

func (c *ClickHouse) AddWatchedContract(ctx context.Context, contractID string) error {
	return nil
}

func (c *ClickHouse) GetContractCursor(context.Context, string) (ContractCursor, error) {
	return ContractCursor{}, ErrNotFound
}

func (c *ClickHouse) SaveContractCursor(context.Context, ContractCursor) error {
	return nil
}

func (c *ClickHouse) DeleteContractCursor(context.Context, string) error {
	return nil
}

func (c *ClickHouse) ListContractCursors(context.Context) ([]ContractCursor, error) {
	return nil, nil
}

func (c *ClickHouse) RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error) {
	return f, nil
}

func (c *ClickHouse) UpdateAuditFinding(ctx context.Context, f AuditFinding) error {
	return nil
}

func (c *ClickHouse) ListOpenFindingsByRange(ctx context.Context, network string, fromLedger, toLedger int64) (AuditFinding, error) {
	return AuditFinding{}, ErrNotFound
}

func (c *ClickHouse) CreateSubscription(ctx context.Context, s Subscription) (Subscription, error) {
	return s, nil
}

func (c *ClickHouse) GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error) {
	return Subscription{}, ErrNotFound
}

func (c *ClickHouse) ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error) {
	return nil, nil
}

func (c *ClickHouse) UpdateSubscription(ctx context.Context, s Subscription, owner SubscriptionOwner) (Subscription, error) {
	return s, nil
}

func (c *ClickHouse) DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error {
	return nil
}

func (c *ClickHouse) ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error) {
	return nil, nil
}

func (c *ClickHouse) IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (int, bool, error) {
	return 0, false, nil
}

func (c *ClickHouse) ResetSubscriptionFailures(ctx context.Context, id int64) error {
	return nil
}

func (c *ClickHouse) RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error) {
	return a, nil
}

func (c *ClickHouse) ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error) {
	return nil, nil
}

func (c *ClickHouse) CountDeliveryAttempts(ctx context.Context, subscriptionID int64, owner SubscriptionOwner) (int64, error) {
	return 0, nil
}

func (c *ClickHouse) GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error) {
	return nil, ErrNotFound
}

func (c *ClickHouse) SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error {
	return nil
}

func (c *ClickHouse) GetContractSpecOverride(ctx context.Context, contractID string) ([]byte, error) {
	return nil, ErrNotFound
}

func (c *ClickHouse) SetContractSpecOverride(ctx context.Context, contractID string, specJSON []byte) error {
	return nil
}

func (c *ClickHouse) DeleteContractSpecOverride(ctx context.Context, contractID string) error {
	return nil
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
	return 0, false, nil
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

func (c *ClickHouse) ListContracts(context.Context, ContractsFilter) ([]ContractSummary, string, error) {
	// Returning an empty page with a nil error was indistinguishable from
	// "this instance has no contracts", so a caller could not tell the
	// backend simply does not implement this.
	return nil, "", fmt.Errorf("ListContracts: not supported by the clickhouse backend")
}

// CountContracts counts distinct contract_id values in events, matching
// Postgres.CountContracts' semantics exactly: a "contract" here means a
// contract_id that has emitted at least one stored event, not a row in a
// separate contracts registry (ClickHouse has none).
func (c *ClickHouse) CountContracts(ctx context.Context, f ContractsFilter) (int64, error) {
	where := ""
	if f.ContractIDPrefix != "" {
		where = "WHERE contract_id LIKE " + clickHouseQuoteString(f.ContractIDPrefix+"%")
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

func (c *ClickHouse) DeadLetterEvent(context.Context, DeadLetterInput) (DeadLetter, error) {
	return DeadLetter{}, nil
}

func (c *ClickHouse) ListDeadLetters(context.Context, string, int, string) ([]DeadLetter, string, error) {
	return nil, "", nil
}

func (c *ClickHouse) CountDeadLetters(context.Context, string) (int64, error) {
	return 0, nil
}

func (c *ClickHouse) GetDeadLetter(context.Context, int64) (DeadLetter, error) {
	return DeadLetter{}, ErrNotFound
}

func (c *ClickHouse) DeleteDeadLetter(context.Context, int64) error {
	return nil
}

func (c *ClickHouse) Ping(ctx context.Context) error {
	if c.host == "" {
		return fmt.Errorf("clickhouse: not configured (empty host)")
	}
	addr := net.JoinHostPort(c.host, strconv.Itoa(c.port))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("clickhouse ping %s: %w", addr, err)
	}
	conn.Close()
	return nil
}

func (c *ClickHouse) UpsertAddressRefs(ctx context.Context, refs []AddressRef) error {
	return nil
}

func (c *ClickHouse) QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error) {
	return nil, "", nil
}

func (c *ClickHouse) CountAddressEvents(ctx context.Context, address string) (int64, error) {
	return 0, nil
}

func (c *ClickHouse) GetAddressSummary(ctx context.Context, address string) (AddressSummary, error) {
	return AddressSummary{}, nil
}
