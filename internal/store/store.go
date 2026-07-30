// Package store persists contract events and ingestion state in Postgres.
package store

import (
	"context"
	"encoding/json"
	"time"
)

// Event is a Soroban contract event as persisted by SoroTrail.
type Event struct {
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
	Network          string          `json:"network"`

	RawTopicXDR []string `json:"-"`
	RawValueXDR string   `json:"-"`
}

// EnrichedEvent wraps an Event with decoded field information.
type EnrichedEvent struct {
	Event        `json:",inline"`
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
	Network     string
	RawTopicXDR []string
	RawValueXDR string
	Topics      json.RawMessage
	Value       json.RawMessage
}

// HasRawXDR reports whether the event carries enough raw XDR to be replayed.
func (d DecodedEvent) HasRawXDR() bool {
	return len(d.RawTopicXDR) > 0 || d.RawValueXDR != ""
}

// ReplayState is the single persisted progress row for the replay tool.
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
	Type       string
	Topic      json.RawMessage
	Topic0     json.RawMessage
	Topic1     json.RawMessage
	Topic2     json.RawMessage
	Topic3     json.RawMessage
	FromLedger int64
	ToLedger   int64
	FromTime   time.Time
	ToTime     time.Time
	Cursor     string
	Limit      int
	Order      string
	Network    string
}

// IngestionState tracks how far ingestion has progressed for one network.
type IngestionState struct {
	Network            string
	LastIngestedLedger int64
	LastCursor         string
	UpdatedAt          time.Time
}

// AuditState tracks how far the background auditor has verified stored
// ranges against the RPC for one network.
type AuditState struct {
	Network               string
	VerifiedThroughLedger int64
	UpdatedAt             time.Time
}

// LedgerCensus is one row of a per-ledger census over a contiguous range.
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
// store and the RPC.
type AuditFinding struct {
	ID              int64
	FromLedger      int64
	ToLedger        int64
	ExpectedCount   int
	ActualCount     int
	MissingIDs      []string
	Status          string
	Attempts        int
	LastAttemptedAt time.Time
	LastError       string
	CreatedAt       time.Time
}

// SubscriptionFilter is a JSON-serializable filter that subscription
// callbacks use to select which events to deliver.
type SubscriptionFilter struct {
	ContractID string          `json:"contract_id,omitempty"`
	Type       string          `json:"type,omitempty"`
	Topic      json.RawMessage `json:"topic,omitempty"`
	FromLedger int64           `json:"from_ledger,omitempty"`
	ToLedger   int64           `json:"to_ledger,omitempty"`
	Network    string          `json:"network,omitempty"`
}

// MatchesEvent reports whether an event passes this filter.
func (f SubscriptionFilter) MatchesEvent(e Event) bool {
	if f.Network != "" && e.Network != f.Network {
		return false
	}
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
	return true
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
}

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

// PerNetworkStats holds stats for a single network.
type PerNetworkStats struct {
	TotalEvents           int64 `json:"total_events"`
	LastIngestedLedger    int64 `json:"last_ingested_ledger"`
	VerifiedThroughLedger int64 `json:"verified_through_ledger"`
}

// Stats summarizes what the indexer has stored so far.
type Stats struct {
	TotalEvents           int64                      `json:"total_events"`
	LastIngestedLedger    int64                      `json:"last_ingested_ledger"`
	VerifiedThroughLedger int64                      `json:"verified_through_ledger"`
	ContractCount         int64                      `json:"contract_count"`
	WatchedContracts      int64                      `json:"watched_contracts"`
	PerNetwork            map[string]PerNetworkStats `json:"per_network,omitempty"`
	Auditor               AuditStats                 `json:"auditor,omitempty"`
}

// AuditStats is a JSON-friendly view of audit.Metrics.
type AuditStats struct {
	PassesRun             uint64 `json:"passes_run"`
	LedgersChecked        uint64 `json:"ledgers_checked"`
	FindingsOpened        uint64 `json:"findings_opened"`
	FindingsRepaired      uint64 `json:"findings_repaired"`
	FindingsUnverifiable  uint64 `json:"findings_unverifiable"`
	FindingsUnrecoverable uint64 `json:"findings_unrecoverable"`
	RPCRequests           uint64 `json:"rpc_requests"`
}

// ReplayBatch is one transactional unit of replay work.
type ReplayBatch struct {
	Events []EventDecoding
	State  ReplayState
}

// EventDecoding is a freshly decoded events row, keyed by event ID.
type EventDecoding struct {
	ID     string
	Topics json.RawMessage
	Value  json.RawMessage
}

// Store is the persistence boundary.
type Store interface {
	UpsertEvents(ctx context.Context, events []Event) (int64, error)
	ReplaceEventsInRange(ctx context.Context, events []Event, fromLedger, toLedger int64) error
	GetEvent(ctx context.Context, id string) (Event, error)
	EventExists(ctx context.Context, id string) (bool, error)
	QueryEvents(ctx context.Context, f EventFilter) ([]Event, string, error)
	LedgerRangeCensus(ctx context.Context, fromLedger, toLedger int64, idsOnly bool) ([]LedgerCensus, error)

	GetIngestionState(ctx context.Context, network string) (IngestionState, error)
	SaveIngestionState(ctx context.Context, s IngestionState) error

	GetAuditState(ctx context.Context, network string) (AuditState, error)
	SaveAuditState(ctx context.Context, s AuditState) error
	SaveAuditStateIfGreater(ctx context.Context, network string, ledger int64) (AuditState, error)

	ListWatchedContracts(ctx context.Context) ([]string, error)
	AddWatchedContract(ctx context.Context, contractID string) error

	RecordAuditFinding(ctx context.Context, f AuditFinding) (AuditFinding, error)
	UpdateAuditFinding(ctx context.Context, f AuditFinding) error
	ListOpenFindingsByRange(ctx context.Context, fromLedger, toLedger int64) (AuditFinding, error)

	CreateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	GetSubscription(ctx context.Context, id int64) (Subscription, error)
	ListSubscriptions(ctx context.Context) ([]Subscription, error)
	UpdateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	DeleteSubscription(ctx context.Context, id int64) error
	ListEnabledSubscriptions(ctx context.Context) ([]Subscription, error)
	IncrementSubscriptionFailures(ctx context.Context, id int64, maxFailures int) (newCount int, disabled bool, err error)
	ResetSubscriptionFailures(ctx context.Context, id int64) error
	RecordDeliveryAttempt(ctx context.Context, a DeliveryAttempt) (DeliveryAttempt, error)
	ListDeliveryAttempts(ctx context.Context, subscriptionID int64, limit int) ([]DeliveryAttempt, error)
	GetContractSpec(ctx context.Context, wasmHash string) ([]byte, error)
	SetContractSpec(ctx context.Context, wasmHash, contractID string, specJSON []byte) error

	Stats(ctx context.Context) (Stats, error)
	Ping(ctx context.Context) error
}
