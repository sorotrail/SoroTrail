// Package store persists contract events and ingestion state in Postgres.
package store

import (
	"context"
	"encoding/json"
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
}

// EventFilter narrows a QueryEvents call. Zero values mean "no constraint".
type EventFilter struct {
	ContractID string
	Type       string
	// Topic matches events whose topics array contains this JSON value at any
	// position (Postgres jsonb containment).
	Topic      json.RawMessage
	FromLedger int64 // inclusive
	ToLedger   int64 // inclusive
	// Cursor is the ID of the last event from the previous page.
	Cursor string
	Limit  int
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

// LedgerCensus is one row of a per-ledger census over a contiguous range.
// IDs is the lexicographically sorted list of stored event IDs in the
// ledger — empty in a count-only census, populated for ID-level diffs.
type LedgerCensus struct {
	Ledger int64
	Count  int
	IDs    []string
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

// Stats summarizes what the indexer has stored so far. VerifiedThroughLedger
// is the inclusive highest ledger whose stored events have been confirmed
// to match a fresh RPC fetch; 0 means no ledger has been verified yet.
// Auditor counters are filled in by the API layer when an auditor is wired.
type Stats struct {
	TotalEvents           int64 `json:"total_events"`
	LastIngestedLedger    int64 `json:"last_ingested_ledger"`
	VerifiedThroughLedger int64 `json:"verified_through_ledger"`
	ContractCount         int64 `json:"contract_count"`
	WatchedContracts      int64 `json:"watched_contracts"`
	// Auditor counters are populated only when the audit package is
	// active; omitted from JSON when the auditor is nil.
	Auditor AuditStats `json:"auditor,omitempty"`
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

// Store is the persistence boundary. The ingester, auditor, and API depend
// on this interface, never on Postgres directly, so alternative backends can
// be contributed by implementing it.
type Store interface {
	// UpsertEvents inserts events idempotently (duplicates by ID are ignored)
	// and returns the number of newly inserted rows.
	UpsertEvents(ctx context.Context, events []Event) (int64, error)
	// ReplaceEventsInRange atomically makes [fromLedger, toLedger] in the
	// store exactly match `events`: orphans (rows in that ledger range that
	// no longer appear in `events`) are deleted, missing events are
	// inserted, and same-ID rows are updated (so topic/value mutations on
	// the RPC side are corrected). Designed for targeted repair from the
	// auditor; ingest uses UpsertEvents instead.
	ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error
	// GetEvent returns the event with the given ID, or ErrNotFound.
	GetEvent(ctx context.Context, id string) (Event, error)
	// QueryEvents returns a page of events in ascending ID order, plus a
	// cursor for the next page ("" when there are no more results).
	QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error)
	// LedgerRangeCensus returns one LedgerCensus row per ledger in the
	// inclusive [fromLedger, toLedger] range that contains at least one
	// event, in ascending ledger order. idsOnly=true populates LedgerCensus.IDs
	// (sorted lexicographically); idsOnly=false returns counts only and is
	// the cheap path used for the common "all good" verify sweep.
	LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error)

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

	ListWatchedContracts(ctx context.Context) ([]string, error)
	AddWatchedContract(ctx context.Context, contractID string) error

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

	Stats(ctx context.Context) (Stats, error)
	Ping(ctx context.Context) error
}
