// Package store persists contract events and ingestion state in Postgres.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Event is a Soroban contract event as persisted by SoroTrail.
type Event struct {
	// ID is the RPC's TOID-based event identifier. IDs are zero-padded, so
	// their lexicographic order matches chronological order — pagination and
	// cursors rely on this.
	ID               string          `json:"id"`
	ContractID       string          `json:"contract_id"`
	Ledger           int64           `json:"ledger"`
	Type             string          `json:"type"`
	TxHash           string          `json:"tx_hash"`
	TxIndex          int32           `json:"tx_index"`
	OpIndex          int32           `json:"op_index"`
	InSuccessfulCall bool            `json:"in_successful_call"`
	Topics           json.RawMessage `json:"topics"`
	Value            json.RawMessage `json:"value"`
	CreatedAt        time.Time       `json:"created_at"`

	// RawTopicXDR and RawValueXDR keep the base64 XDR the RPC delivered, so
	// an improved decoder can re-derive Topics/Value later without the RPC
	// (see internal/replay). Both are empty when the RPC returned
	// already-decoded JSON, or for rows ingested before raw XDR was stored.
	// They are not part of the API representation.
	RawTopicXDR []string `json:"-"`
	RawValueXDR string   `json:"-"`
}

// EnrichedEvent wraps an Event with decoded field information derived from
// the contract's spec. The original Event is preserved in full; DecodedEvent
// carries the enriched view when decoding succeeded.
type EnrichedEvent struct {
	Event        `json:",inline"`      // embed all Event fields
	DecodedEvent *DecodedEventResponse `json:"decoded_event,omitempty"`
	Decoded      bool                  `json:"decoded"`
}

// DecodedEventResponse is the JSON shape returned when an event is successfully
// enriched with spec-driven field names.
type DecodedEventResponse struct {
	Event  string         `json:"event"`
	Fields map[string]any `json:"fields,omitempty"`
}

// DecodedEvent is one event's replayable payload: the raw XDR inputs plus the
// decoded columns currently stored for it.
type DecodedEvent struct {
	ID          string
	ContractID  string
	Ledger      int64
	RawTopicXDR []string
	RawValueXDR string
	Topics      json.RawMessage
	Value       json.RawMessage
}

// HasRawXDR reports whether the event carries enough raw XDR to be replayed.
// Rows without it are skipped (and counted) rather than treated as errors.
func (d DecodedEvent) HasRawXDR() bool {
	return len(d.RawTopicXDR) > 0 || d.RawValueXDR != ""
}

// ReplayState is the single persisted progress row for the replay tool.
// LastEventID is the last event whose rewrite committed; replay resumes at
// the first event after it.
type ReplayState struct {
	FromLedger  int64
	ToLedger    int64
	LastEventID string
	Processed   int64
	Changed     int64
	Skipped     int64
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
}

// Done reports whether the recorded run finished its whole range.
func (s ReplayState) Done() bool { return s.CompletedAt != nil }

// EventFilter narrows a QueryEvents call. Zero values mean "no constraint".
type EventFilter struct {
	ContractID string
	// Types filters by event type. Multiple values are accepted (ANDed
	// together at the SQL level via type = ANY(...)). An empty or nil
	// slice means "no constraint".
	Types []string
	// Topic matches events whose topics array contains this JSON value at any
	// position (Postgres jsonb containment).
	Topic json.RawMessage
	// TxHash limits results to events from a specific transaction.
	TxHash string
	// InSuccessfulCall limits results to events from successful or failed
	// calls. nil means no constraint — use a non-nil pointer to opt in;
	// the zero-value convention doesn't apply to booleans.
	InSuccessfulCall *bool
	// Topic0-Topic3 match the exact JSON value at that specific topic array
	// position. Unspecified positions are wildcards.
	Topic0 json.RawMessage
	Topic1 json.RawMessage
	Topic2 json.RawMessage
	Topic3 json.RawMessage
	// TopicContains matches events whose topics array jsonb-contains this
	// value (Postgres @> operator). Unlike Topic, the value is passed
	// directly without array-wrapping, so callers can use multi-element
	// arrays: topic_contains=[{"symbol":"transfer"},{"address":"C..."}].
	// Uses the GIN index on events.topics.
	TopicContains json.RawMessage
	// HasValue filters events by whether they carry a value payload.
	// nil means no constraint; true means value IS NOT NULL;
	// false means value IS NULL.
	HasValue *bool
	// TxIndex is an exact-match filter on the transaction index within a
	// ledger. A nil pointer means "no constraint". Use TxIndexToPtr for
	// inline construction of a non-nil pointer from a literal.
	TxIndex *int32 // exact match on tx index, nil = unset
	// OpIndex is an exact-match filter on the operation index within a
	// transaction. A nil pointer means "no constraint".
	OpIndex    *int32    // exact match on op index, nil = unset
	FromLedger int64     // inclusive
	ToLedger   int64     // inclusive, zero = no constraint
	FromTime   time.Time // inclusive, zero = no constraint
	ToTime     time.Time // inclusive, zero = no constraint
	// Cursor is the ID of the last event from the previous page.
	Cursor string
	Limit  int
	// Order is "asc" or "desc", defaults to "asc"
	Order string
	// OrderBy selects the sort column: OrderByID (default), OrderByLedger,
	// or OrderByCreatedAt. Every ordering is made total by appending id as
	// a tiebreaker, so keyset pagination stays stable when the sort column
	// has duplicates.
	OrderBy string

	// Scope is the tenant authorization boundary, ANDed into the generated
	// SQL alongside the user-supplied filters above. Unlike every other
	// field on this struct, its zero value is a constraint and not the
	// absence of one: an unset Scope matches nothing. See the Scope type
	// for why it fails closed rather than open.
	//
	// The API layer populates this from the authenticated request in
	// exactly one place (filterFromQuery), so no handler decides for
	// itself whether a caller is entitled to a row.
	Scope Scope
}

// Sort columns accepted in EventFilter.OrderBy. The zero value means
// OrderByID, which is the historical behavior.
const (
	OrderByID        = "id"
	OrderByLedger    = "ledger"
	OrderByCreatedAt = "created_at"
)

// ValidOrderBy reports whether s names a supported sort column. The empty
// string is valid and means OrderByID.
func ValidOrderBy(s string) bool {
	switch s {
	case "", OrderByID, OrderByLedger, OrderByCreatedAt:
		return true
	default:
		return false
	}
}

// IngestionState tracks how far ingestion has progressed.
type IngestionState struct {
	LastIngestedLedger int64
	LastCursor         string
	UpdatedAt          time.Time
}

// AuditState tracks how far the background auditor has verified stored
// ranges against the RPC. VerifiedThroughLedger is the inclusive highest
// ledger whose stored events have been proven to match a fresh getEvents
// fetch.
type AuditState struct {
	VerifiedThroughLedger int64
	UpdatedAt             time.Time
}

// WatchedContract is one entry of the watch list: a contract ID and the
// time it was added (either by env seeding on startup, or by a runtime
// POST). The API uses this to render the GET response; the ingester reads
// only ContractID for its filter batches.
type WatchedContract struct {
	ContractID string    `json:"contract_id"`
	AddedAt    time.Time `json:"added_at"`
}

// LedgerCensus is one row of a per-ledger census over a contiguous range.
// IDs is the lexicographically sorted list of stored event IDs in the
// ledger — empty in a count-only census, populated for ID-level diffs.
type LedgerCensus struct {
	Ledger int64
	Count  int
	IDs    []string
}

// ContractSummary is one row of the indexed-contract listing: a contract
// ID alongside the aggregate metrics the /contracts endpoint exposes.
// FirstLedger and LastLedger bracket the contract's known activity;
// LastSeen is the wall-clock time of the most recent event ingested
// for it. EventCount is the total number of events for the contract.
type ContractSummary struct {
	ContractID  string    `json:"contract_id"`
	EventCount  int64     `json:"event_count"`
	FirstLedger int64     `json:"first_ledger"`
	LastLedger  int64     `json:"last_ledger"`
	LastSeen    time.Time `json:"last_seen"`
}

// ContractsFilter narrows a ListContracts call.
//
// SortKey selects the column that names activity. Defaults to "count"
// (so the most active contracts come first). Order still controls the
// direction; the comparison pair (SortValue, ContractID) is total
// because ContractID is unique, so keyset pagination stays stable.
//
// ContractIDPrefix, when set, constrains the result to contracts whose
// ID starts with the prefix. Indexed lookups (the contract_id index)
// can serve this directly; no full scan.
type ContractsFilter struct {
	ContractIDPrefix string
	SortKey          string // "" | "count" | "first_ledger" | "last_ledger" | "last_seen"
	Order            string // "asc" | "desc"; "" defaults to "desc"
	Cursor           string
	Limit            int
}

// SortKey constants for ContractsFilter.SortKey. The zero value
// (empty string) is treated as SortByActivity; the API surface
// exists to make a future "by first seen" view trivial to add.
const (
	SortByActivity    = "count"
	SortByFirstLedger = "first_ledger"
	SortByLastLedger  = "last_ledger"
	SortByLastSeen    = "last_seen"
)

// ErrInvalidContractsCursor is returned when the pagination cursor cannot
// be decoded for the requested sort. The API maps it to 400.
var ErrInvalidContractsCursor = errors.New("invalid contracts cursor")

// DeadLetter is one event that the ingester could not persist into the
// events table. It carries enough context (raw XDR + the error) for an
// operator to inspect the row, hand-replay it through a future
// decoder, and DELETE it once it's been dealt with.
//
// The row is intentionally distinct from the events table: events are
// append-only and immutable, while dead letters are a working queue.
// The same event ID can be dead-lettered more than once across runs,
// so the row's primary key is a fresh bigserial ID rather than the
// TOID-based event ID.
type DeadLetter struct {
	ID          int64     `json:"id"`
	EventID     string    `json:"event_id"`
	ContractID  string    `json:"contract_id"`
	Ledger      int64     `json:"ledger"`
	Type        string    `json:"type"`
	TxHash      string    `json:"tx_hash"`
	TopicXDR    []string  `json:"topic_xdr,omitempty"`
	ValueXDR    string    `json:"value_xdr,omitempty"`
	Error       string    `json:"error"`
	Attempts    int       `json:"attempts"`
	LastAttempt time.Time `json:"last_attempt"`
	CreatedAt   time.Time `json:"created_at"`
}

// Finding statuses the auditor records in audit_findings.
const (
	FindingOpen          = "open"
	FindingRepaired      = "repaired"
	FindingUnverifiable  = "unverifiable"
	FindingUnrecoverable = "unrecoverable"
)

// AuditFinding is one outstanding mismatch the auditor found between the
// store and the RPC. The range [FromLedger, ToLedger] is closed (both
// inclusive) and is bounded by audit_finding_max_ledgers so a single
// finding is a tractable repair target.
type AuditFinding struct {
	ID              int64
	FromLedger      int64
	ToLedger        int64
	ExpectedCount   int // RPC event count for [FromLedger, ToLedger]
	ActualCount     int // Stored event count before repair
	MissingIDs      []string
	Status          string
	Attempts        int
	LastAttemptedAt time.Time
	LastError       string
	CreatedAt       time.Time
}

// SubscriptionFilter is a JSON-serializable filter that subscription
// callbacks use to select which events to deliver. Same semantics as
// GET /events query parameters. An empty (zero-value) filter matches
// every event.
type SubscriptionFilter struct {
	ContractID    string          `json:"contract_id,omitempty"`
	Type          string          `json:"type,omitempty"`
	Topic         json.RawMessage `json:"topic,omitempty"`
	TopicContains json.RawMessage `json:"topic_contains,omitempty"`
	FromLedger    int64           `json:"from_ledger,omitempty"`
	ToLedger      int64           `json:"to_ledger,omitempty"`
}

// MatchesEvent reports whether an event passes this filter. Zero fields
// mean "no constraint" — the filter is applied per-field, so an empty
// filter matches everything.
func (f SubscriptionFilter) MatchesEvent(e Event) bool {
	if f.ContractID != "" && e.ContractID != f.ContractID {
		return false
	}
	if f.Type != "" && e.Type != f.Type {
		return false
	}
	if f.FromLedger > 0 && e.Ledger < f.FromLedger {
		return false
	}
	if f.ToLedger > 0 && e.Ledger > f.ToLedger {
		return false
	}
	if len(f.Topic) > 0 && len(e.Topics) > 0 {
		// Match if any event topic equals the filter topic.
		var topics []json.RawMessage
		if err := json.Unmarshal(e.Topics, &topics); err != nil {
			return false
		}
		matched := false
		for _, t := range topics {
			if string(t) == string(f.Topic) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(f.TopicContains) > 0 && len(e.Topics) > 0 {
		var topics []json.RawMessage
		if err := json.Unmarshal(e.Topics, &topics); err != nil {
			return false
		}
		// Unwrap a single-element array so topic_contains=[{...}] works
		// the same way in-memory as it does in Postgres @> containment.
		needle := f.TopicContains
		var arr []json.RawMessage
		if err := json.Unmarshal(f.TopicContains, &arr); err == nil && len(arr) == 1 {
			needle = arr[0]
		}
		matched := false
		for _, t := range topics {
			if jsonbContains(t, needle) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// jsonbContains reports whether the container jsonb-contains the contained
// value. For objects it checks that every key in contained exists with the
// same raw JSON value in container; for scalars/arrays it falls back to
// direct byte comparison (JSON string equality). This mirrors the Postgres
// @> operator's semantics for the topic-matching use case.
func jsonbContains(container, contained json.RawMessage) bool {
	// If both are objects, check key-value subset.
	var cMap, dMap map[string]json.RawMessage
	if json.Unmarshal(container, &cMap) == nil && json.Unmarshal(contained, &dMap) == nil {
		for k, v := range dMap {
			cv, ok := cMap[k]
			if !ok || string(cv) != string(v) {
				return false
			}
		}
		return true
	}
	// Fallback: exact JSON string match (handles strings, numbers, and
	// cases where unmarshalling into map failed — e.g. arrays).
	return string(container) == string(contained)
}

// Subscription is one registered webhook callback.
type Subscription struct {
	ID           int64              `json:"id"`
	URL          string             `json:"url"`
	Filters      SubscriptionFilter `json:"filters"`
	Secret       string             `json:"secret"`
	Enabled      bool               `json:"enabled"`
	FailureCount int                `json:"failure_count"`
	CreatedAt    time.Time          `json:"created_at"`
	// TenantID is the owning tenant, or nil for an operator-owned
	// subscription — which is what every subscription created before
	// multi-tenancy, or in single-tenant mode, is.
	TenantID *int64 `json:"tenant_id,omitempty"`
}

// SubscriptionOwner scopes subscription CRUD to one tenant's rows.
//
// A subscription delivers event data to an arbitrary external URL, which
// makes it the most valuable thing in the API to an attacker: subscribing to
// a contract you cannot read would exfiltrate it to a server you control.
// Ownership is therefore enforced in the query, exactly like Scope, and for
// the same reason.
//
// Like Scope, its zero value denies: it matches tenant_id = 0, and the
// column is a bigserial reference that never takes that value.
type SubscriptionOwner struct {
	tenantID int64
	all      bool
}

// AllSubscriptions matches every subscription regardless of owner. Used in
// single-tenant mode, by admin tenants, and by the delivery worker, which
// serves all tenants at once.
func AllSubscriptions() SubscriptionOwner { return SubscriptionOwner{all: true} }

// OwnedBy matches only the given tenant's subscriptions.
func OwnedBy(tenantID int64) SubscriptionOwner {
	return SubscriptionOwner{tenantID: tenantID}
}

// IsAll reports whether the owner filter is unrestricted.
func (o SubscriptionOwner) IsAll() bool { return o.all }

// TenantID returns the tenant this owner is restricted to; 0 when
// unrestricted or unset.
func (o SubscriptionOwner) TenantID() int64 { return o.tenantID }

// DeliveryAttempt records one attempt to POST an event to a subscriber's
// callback URL.
type DeliveryAttempt struct {
	ID             int64     `json:"id"`
	SubscriptionID int64     `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	Status         string    `json:"status"`
	ResponseCode   int       `json:"response_code"`
	DurationMs     int       `json:"duration_ms"`
	Error          string    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// AddressRef records one address→event mapping. Populated during ingestion
// from decoded event topics and value JSON.
type AddressRef struct {
	Address string `json:"address"`
	EventID string `json:"event_id"`
	Role    string `json:"role"`
}

// AddressSummary is the aggregate view returned by GetAddressSummary.
type AddressSummary struct {
	Address           string   `json:"address"`
	FirstSeenLedger   int64    `json:"first_seen_ledger"`
	LastSeenLedger    int64    `json:"last_seen_ledger"`
	EventCount        int64    `json:"event_count"`
	DistinctContracts []string `json:"distinct_contracts"`
}

// Stats summarizes what the indexer has stored so far. VerifiedThroughLedger
// is the inclusive highest ledger whose stored events have been confirmed
// to match a fresh RPC fetch; 0 means no ledger has been verified yet.
// TableSizeBytes is the approximate on-disk size of the events table
// (including its partitions, indexes, and TOAST) reported by PostgreSQL's
// pg_total_relation_size; it is 0 on backends that don't report it.
// Auditor counters are filled in by the API layer when an auditor is wired.
type Stats struct {
	TotalEvents           int64  `json:"total_events"`
	LastIngestedLedger    int64  `json:"last_ingested_ledger"`
	VerifiedThroughLedger int64  `json:"verified_through_ledger"`
	OldestStoredLedger    int64  `json:"oldest_stored_ledger"`
	ChainHeadLedger       *int64 `json:"chain_head_ledger"`
	IngestLagLedgers      *int64 `json:"ingest_lag_ledgers"`
	ContractCount         int64  `json:"contract_count"`
	WatchedContracts      int64  `json:"watched_contracts"`
	// TableSizeBytes is the approximate on-disk size of the events table
	// (including partitions, indexes, and TOAST). 0 when the backend does
	// not report it.
	TableSizeBytes int64 `json:"table_size_bytes"`
	// QueryErrors is the number of store queries that have returned an
	// error (timeout, connection failure, etc.) since the process started.
	// Set by the guarded store wrapper; zero when the store is used
	// directly or when no errors have occurred.
	QueryErrors uint64 `json:"query_errors"`
	// PanicsRecovered is the number of panics the HTTP middleware has
	// recovered since process start. Set by the API handler.
	PanicsRecovered uint64 `json:"panics_recovered"`
	// RPCErrors counts RPC call failures by method name since the process
	// started. Populated by the CountingClient wrapper; zero-valued when
	// the wrapper is not in use.
	RPCErrors RPCErrorStats `json:"rpc_errors,omitempty"`
	// Auditor counters are populated only when the audit package is
	// active; omitted from JSON when the auditor is nil.
	Auditor AuditStats `json:"auditor,omitempty"`
	// Pruner counters are populated only when retention is configured;
	// omitted from JSON when the pruner is a no-op.
	Pruner PrunerStats `json:"pruner,omitempty"`
}

// PrunerStats is a JSON-friendly view of pruner.Metrics.
type PrunerStats struct {
	RunsCompleted   uint64 `json:"runs_completed"`
	TotalRowsPurged int64  `json:"total_rows_purged"`
}

// RPCErrorStats is a JSON-friendly snapshot of per-method RPC error counts.
type RPCErrorStats struct {
	GetEvents        uint64 `json:"getEvents,omitempty"`
	GetLatestLedger  uint64 `json:"getLatestLedger,omitempty"`
	GetHealth        uint64 `json:"getHealth,omitempty"`
	GetLedgerEntries uint64 `json:"getLedgerEntries,omitempty"`
}

// AuditStats is a JSON-friendly view of audit.Metrics. Defined here so
// json.Marshal sees concrete field tags (avoiding mapstructure work in
// the API layer).
type AuditStats struct {
	PassesRun             uint64 `json:"passes_run"`
	LedgersChecked        uint64 `json:"ledgers_checked"`
	FindingsOpened        uint64 `json:"findings_opened"`
	FindingsRepaired      uint64 `json:"findings_repaired"`
	FindingsUnverifiable  uint64 `json:"findings_unverifiable"`
	FindingsUnrecoverable uint64 `json:"findings_unrecoverable"`
	RPCRequests           uint64 `json:"rpc_requests"`
}

// ReplayBatch is one transactional unit of replay work: the rewritten
// decoded columns for a batch of events, every dependent table re-derived
// from them, and the progress marker to advance.
//
// contributors: derivation order is part of this type's contract. Dependent
// tables are derived from the *new* Events decoding, never from what is
// currently in the database, so a replay is a pure function of the raw XDR.
// When a dependent table lands (e.g. token_events from a SEP-41 decoder) add
// it as a field here and write it in CommitReplayBatch after Events — that
// keeps the order in one place instead of spread across call sites.
type ReplayBatch struct {
	// Events are the events-table rewrites, applied first because every
	// dependent table reads from them.
	Events []EventDecoding
	// State is the progress marker, written last and in the same
	// transaction, so committed progress can never run ahead of committed
	// rewrites.
	State ReplayState
}

// EventDecoding is a freshly decoded events row, keyed by event ID.
type EventDecoding struct {
	ID     string
	Topics json.RawMessage
	Value  json.RawMessage
}

// Store is the persistence boundary. The ingester, auditor, and API depend
// on this interface, never on Postgres directly, so alternative backends can
// be contributed by implementing it.
//
// Replay-specific persistence is deliberately not part of this interface —
// it lives on *Postgres and is consumed through the narrower interface in
// internal/replay, so backends that don't need a maintenance replay tool
// aren't forced to implement one.
type Store interface {
	// UpsertEvents inserts events idempotently (duplicates by ID are ignored)
	// and returns the number of newly inserted rows.
	UpsertEvents(ctx context.Context, events []Event) (int64, error)
	// ReplaceEventsInRange atomically makes [fromLedger, toLedger] in the
	// store exactly match `events`: orphans (rows in that ledger range that
	// no longer appear in `events`) are deleted, missing events are
	// inserted, and same-ID rows are updated (so topic/value mutations on
	// the RPC side are corrected). Designed for targeted repair from the
	// auditor; ingest uses UpsertEvents instead. Raw XDR already stored for
	// a surviving event is preserved when the incoming event carries none,
	// so a repair never costs a row its replayability.
	ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error
	// GetEvent returns the event with the given ID, or ErrNotFound.
	//
	// An event outside sc is reported as ErrNotFound, not as a permission
	// error. Event IDs are dense and guessable (they are TOIDs), so
	// distinguishing "exists but forbidden" from "does not exist" would let
	// a caller enumerate the existence of other tenants' events one probe
	// at a time. Named-contract endpoints answer 403 instead, because there
	// the caller already supplied the contract ID and learns nothing from
	// being told they lack access to it.
	GetEvent(ctx context.Context, id string, sc Scope) (Event, error)
	// GetEventsByTxHash returns all events emitted by the transaction
	// identified by txHash, excluding the event with id excludeID (when
	// non-empty). Returns an empty slice when no other events exist.
	GetEventsByTxHash(ctx context.Context, txHash, excludeID string) ([]Event, error)
	// EventExists reports whether an event with the given ID is in the
	// store. It is the cheap 304 path used by the API when a conditional
	// GET carries an If-None-Match whose validator matches the request
	// URL: we want to confirm "still here" without re-serializing the
	// full row, so retention/pruning (when it lands, see #8) can't leave
	// cached clients believing a deleted event is still available.
	//
	// Scoped for the same reason as GetEvent, and more urgently: this is a
	// pure existence oracle, so an unscoped version would be the cheapest
	// possible cross-tenant enumeration primitive.
	EventExists(ctx context.Context, id string, sc Scope) (bool, error)
	// QueryEvents returns a page of events in ascending ID order, plus a
	// cursor for the next page ("" when there are no more results).
	// Default order is ascending (oldest-first) for backward compatibility.
	QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error)
	// CountEvents returns the total number of events matching the filter
	// (ignoring pagination: cursor, order, and limit are not applied).
	CountEvents(ctx context.Context, f EventFilter) (int64, error)
	// LedgerRangeCensus returns one LedgerCensus row per ledger in the
	// inclusive [fromLedger, toLedger] range that contains at least one
	// event, in ascending ledger order. idsOnly=true populates LedgerCensus.IDs
	// (sorted lexicographically); idsOnly=false returns counts only and is
	// the cheap path used for the common "all good" verify sweep.
	LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error)

	// ListContracts returns one ContractSummary per indexed contract
	// matching f, plus a cursor for the next page ("" when there are no
	// more results). Pagination is keyset over (SortValue, ContractID);
	// the cursor encodes both halves so pages land at stable boundaries.
	ListContracts(ctx context.Context, f ContractsFilter) ([]ContractSummary, string, error)
	// CountContracts returns the total number of indexed contracts
	// matching f (ignoring pagination: cursor, order, and limit).
	CountContracts(ctx context.Context, f ContractsFilter) (int64, error)

	// DeadLetterEvent records a single event the ingester could not
	// persist (decode failure, constraint violation, etc.) along with
	// the original RPC payload and the error that dropped it. Retry-safe:
	// re-submitting the same rpc.Event with a different err text
	// increments `attempts` and updates last_attempt + error.
	DeadLetterEvent(ctx context.Context, ev DeadLetterInput) (DeadLetter, error)
	// ListDeadLetters returns one DeadLetter per row, newest first,
	// filtered by contractID ("" means all). Pagination is keyset: the
	// returned cursor encodes the last row's id so a follow-up call
	// resumes cleanly.
	ListDeadLetters(ctx context.Context, contractID string, limit int, cursor string) ([]DeadLetter, string, error)
	// GetDeadLetter returns a single row by id, or ErrNotFound.
	GetDeadLetter(ctx context.Context, id int64) (DeadLetter, error)
	// DeleteDeadLetter removes a row (call after the row has been
	// inspected and replayed manually). ErrNotFound when no row matches.
	DeleteDeadLetter(ctx context.Context, id int64) error

	// Ping(ctx context.Context) error

	GetIngestionState(ctx context.Context) (IngestionState, error)
	SaveIngestionState(ctx context.Context, s IngestionState) error

	GetAuditState(ctx context.Context) (AuditState, error)
	SaveAuditState(ctx context.Context, s AuditState) error
	// SaveAuditStateIfGreater atomically sets verified_through_ledger
	// only when it is strictly greater than the stored value. Returns the
	// post-write AuditState (whether or not it was modified). It's an
	// UPDATE ... WHERE clause, so concurrent auditors can't regress the
	// HWM even if they race.
	SaveAuditStateIfGreater(ctx context.Context, ledger int64) (AuditState, error)

	// ListWatchedContracts returns the union of the operator-configured
	// watch list and every tenant's watch list, which is what ingestion
	// must fetch. It is intentionally not scoped: ingestion serves all
	// tenants at once, and a contract two tenants both want is fetched
	// once. Removal is implicit — a contract disappears from the union
	// when the last row naming it is gone.
	ListWatchedContracts(ctx context.Context) ([]WatchedContract, error)
	AddWatchedContract(ctx context.Context, contractID string) error
	// RemoveWatchedContract stops future ingestion for the given contract
	// by removing its row from watched_contracts. It does NOT delete any
	// (event) rows already in storage — removal is "stop watching", not
	// "drop history", so a removed contract's events stay queryable.
	// ErrNotFound is returned when no row matches the given ID, so the
	// API can surface 404 for typos.
	RemoveWatchedContract(ctx context.Context, contractID string) error

	// RecordAuditFinding persists a new finding (status "open") and
	// returns it with its assigned ID populated.
	RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error)
	// UpdateAuditFinding overwrites the mutable fields (status, attempts,
	// last_*) of an existing finding.
	UpdateAuditFinding(ctx context.Context, f AuditFinding) error
	// ListOpenFindingsByRange returns the most recent open finding whose
	// range contains a single ledger, or ErrNotFound if none. The auditor
	// uses this to keep working while a finding is being repaired.
	ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error)

	// Subscription CRUD. Every read and mutation is filtered by owner, so a
	// tenant cannot enumerate, read, modify or delete another's callbacks —
	// and, critically, cannot register one that would deliver events it is
	// not entitled to read.
	CreateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	GetSubscription(ctx context.Context, id int64, owner SubscriptionOwner) (Subscription, error)
	ListSubscriptions(ctx context.Context, owner SubscriptionOwner) ([]Subscription, error)
	UpdateSubscription(ctx context.Context, s Subscription, owner SubscriptionOwner) (Subscription, error)
	DeleteSubscription(ctx context.Context, id int64, owner SubscriptionOwner) error

	// ListEnabledSubscriptions returns all subscriptions with enabled=true.
	ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error)

	// IncrementSubscriptionFailures atomically increments failure_count
	// and returns the new value. When newCount >= maxFailures the
	// subscription is also disabled.
	IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (newCount int, disabled bool, err error)
	// ResetSubscriptionFailures sets failure_count to 0.
	ResetSubscriptionFailures(ctx context.Context, id int64) error

	// RecordDeliveryAttempt persists one delivery attempt.
	RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error)
	// ListDeliveryAttempts returns delivery attempts for a subscription,
	// newest first. Owner-filtered: delivery history reveals which events
	// matched, so it is as sensitive as the subscription itself.
	ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int, owner SubscriptionOwner) ([]DeliveryAttempt, error)

	// GetContractSpec returns the JSON-serialized spec for a wasm_hash,
	// or ErrNotFound when no spec is cached for that hash.
	GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error)
	// SetContractSpec persists a JSON-serialized spec keyed by wasm_hash
	// and contract_id so subsequent lookups avoid an RPC round trip.
	SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error

	// DeleteEventsBeforeLedger deletes all events with a ledger strictly less than
	// the given ledger number. It returns the number of rows deleted.
	// This is an admin operation and should be auth-gated at the API layer.
	DeleteEventsBeforeLedger(ctx context.Context, beforeLedger int64) (int64, error)
	// DeleteEventsBefore deletes up to limit events strictly below maxLedger
	// and (when beforeTime is non-zero) older than beforeTime. The limit
	// keeps a single DELETE from holding a long lock; the pruner loops.
	DeleteEventsBefore(ctx context.Context, maxLedger int64, beforeTime time.Time, limit int) (int64, error)

	// UpsertAddressRefs inserts address→event index rows idempotently.
	// Duplicate (address, event_id, role) combinations are silently ignored.
	UpsertAddressRefs(ctx context.Context, refs []AddressRef) error
	// QueryAddressEvents returns events involving the given address, in
	// chronological order (by event_id), cursor-paginated.
	QueryAddressEvents(ctx context.Context, address string, f EventFilter) ([]Event, string, error)
	// CountAddressEvents returns the total number of events involving the
	// given address (ignoring pagination).
	CountAddressEvents(ctx context.Context, address string) (int64, error)
	// GetAddressSummary returns aggregate information about an address's
	// event history: first/last seen ledger, total event count, and
	// distinct contracts interacted with.
	GetAddressSummary(ctx context.Context, address string) (AddressSummary, error)

	// MigrationVersion returns the currently applied migration version and
	// whether the schema_migrations table reports a dirty state. When the
	// migration table does not exist or returns no rows, it returns (0, false, nil).
	MigrationVersion(ctx context.Context) (version int, dirty bool, err error)

	// Stats summarizes the store within sc. Aggregates are scoped because
	// counts are an information leak in their own right: an unscoped
	// total_events or contract_count tells a tenant how much data exists
	// outside its grants, and watching those numbers move reveals other
	// tenants' ingestion activity.
	Stats(ctx context.Context, sc Scope) (Stats, error)
	Ping(ctx context.Context) error
}

// DeadLetterInput is the payload handed to Store.DeadLetterEvent. The
// RPC event is captured at the moment of failure (raw XDR if the RPC
// delivered it, JSON shapes) so a replay from this row reproduces the
// exact bytes the ingester saw. Err is the failure message; the row's
// error column always reflects the most recent attempt.
type DeadLetterInput struct {
	EventID    string
	ContractID string
	Ledger     int64
	Type       string
	TxHash     string
	TopicXDR   []string
	ValueXDR   string
	Err        error
}
