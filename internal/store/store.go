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
	ContractIDs []string
	ContractID  string
	Types       []string
	Type        string
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

// Stats summarizes what the indexer has stored so far.
type Stats struct {
	TotalEvents        int64 `json:"total_events"`
	LastIngestedLedger int64 `json:"last_ingested_ledger"`
	ContractCount      int64 `json:"contract_count"`
	WatchedContracts   int64 `json:"watched_contracts"`
}

// Store is the persistence boundary. The ingester and API depend on this
// interface, never on Postgres directly, so alternative backends can be
// contributed by implementing it.
type Store interface {
	// UpsertEvents inserts events idempotently (duplicates by ID are ignored)
	// and returns the number of newly inserted rows.
	UpsertEvents(ctx context.Context, events []Event) (int64, error)
	// GetEvent returns the event with the given ID, or ErrNotFound.
	GetEvent(ctx context.Context, id string) (Event, error)
	// QueryEvents returns a page of events in ascending ID order, plus a
	// cursor for the next page ("" when there are no more results).
	QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error)

	GetIngestionState(ctx context.Context) (IngestionState, error)
	SaveIngestionState(ctx context.Context, s IngestionState) error

	ListWatchedContracts(ctx context.Context) ([]string, error)
	AddWatchedContract(ctx context.Context, contractID string) error

	Stats(ctx context.Context) (Stats, error)
	Ping(ctx context.Context) error
}
