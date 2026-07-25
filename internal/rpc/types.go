package rpc

import "encoding/json"

// XDRFormatJSON asks the RPC to return ScVals as readable JSON
// (topicJson/valueJson) instead of base64-encoded XDR. Supported by Soroban
// RPC protocol 21+.
const XDRFormatJSON = "json"

// EventFilter is one entry of the getEvents "filters" param. The RPC matches
// an event when it matches ANY filter; within a filter, contractIds and
// topics are ANDed.
type EventFilter struct {
	Type        string     `json:"type,omitempty"` // contract | system | diagnostic
	ContractIDs []string   `json:"contractIds,omitempty"`
	Topics      [][]string `json:"topics,omitempty"`
}

// RPC caps enforced by Soroban RPC servers.
const (
	// MaxFiltersPerRequest is the hard cap on filters per getEvents call.
	MaxFiltersPerRequest = 5
	// MaxContractIDsPerFilter is the cap on contractIds within one filter.
	MaxContractIDsPerFilter = 5
	// MaxEventsPerRequest is the largest pagination limit the RPC accepts.
	MaxEventsPerRequest = 10000
)

// Pagination controls getEvents paging. When Cursor is set, StartLedger must
// be omitted from the request — the client enforces this.
type Pagination struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

// GetEventsRequest are the params for the getEvents method.
type GetEventsRequest struct {
	StartLedger uint32        `json:"startLedger,omitempty"`
	EndLedger   uint32        `json:"endLedger,omitempty"`
	Filters     []EventFilter `json:"filters,omitempty"`
	Pagination  *Pagination   `json:"pagination,omitempty"`
	XDRFormat   string        `json:"xdrFormat,omitempty"`
}

// Event is one event as returned by getEvents. Depending on the requested
// xdrFormat, either Topic/Value (base64 XDR) or TopicJSON/ValueJSON are set.
type Event struct {
	ID                       string `json:"id"`
	PagingToken              string `json:"pagingToken"` // pre-protocol-22 per-event cursor
	Type                     string `json:"type"`
	Ledger                   uint32 `json:"ledger"`
	LedgerClosedAt           string `json:"ledgerClosedAt"`
	ContractID               string `json:"contractId"`
	TxHash                   string `json:"txHash"`
	TransactionIndex         int32  `json:"transactionIndex"`
	OperationIndex           int32  `json:"operationIndex"`
	InSuccessfulContractCall bool   `json:"inSuccessfulContractCall"`

	Topic     []string          `json:"topic,omitempty"`
	Value     string            `json:"value,omitempty"`
	TopicJSON []json.RawMessage `json:"topicJson,omitempty"`
	ValueJSON json.RawMessage   `json:"valueJson,omitempty"`
}

// Cursor returns the value to resume pagination after this event, preferring
// the legacy per-event pagingToken and falling back to the event ID (they are
// the same TOID-derived string on modern servers).
func (e Event) CursorValue() string {
	if e.PagingToken != "" {
		return e.PagingToken
	}
	return e.ID
}

// GetEventsResponse is the result of getEvents.
type GetEventsResponse struct {
	Events       []Event `json:"events"`
	LatestLedger uint32  `json:"latestLedger"`
	OldestLedger uint32  `json:"oldestLedger"`
	// Cursor is the top-level resume cursor (protocol 22+). Empty on older
	// servers, which instead expose per-event pagingToken values.
	Cursor string `json:"cursor"`
}

// LatestLedger is the result of getLatestLedger.
type LatestLedger struct {
	ID              string `json:"id"`
	ProtocolVersion uint32 `json:"protocolVersion"`
	Sequence        uint32 `json:"sequence"`
}

// Health is the result of getHealth.
type Health struct {
	Status                string `json:"status"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
}

// GetLedgerEntriesRequest is the params for getLedgerEntries.
type GetLedgerEntriesRequest struct {
	Keys []string `json:"keys"` // base64-encoded LedgerKey XDR
}

// GetLedgerEntriesResponse is the result of getLedgerEntries.
type GetLedgerEntriesResponse struct {
	Entries []LedgerEntryResult `json:"entries"`
}

// LedgerEntryResult is one entry returned by getLedgerEntries.
type LedgerEntryResult struct {
	Key                   string `json:"key"` // base64-encoded LedgerKey XDR
	XDR                   string `json:"xdr"` // base64-encoded LedgerEntry XDR
	LastModifiedLedgerSeq uint32 `json:"lastModifiedLedgerSeq"`
	// LiveUntilLedgerSeq is set for entries with a time-to-live (e.g. temporary entries).
	LiveUntilLedgerSeq *uint32 `json:"liveUntilLedgerSeq,omitempty"`
}
