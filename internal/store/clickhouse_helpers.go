package store

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Shared column lists and row builders for the ClickHouse backend. Keeping
// the column order in one place is what lets a read scan by name
// (JSONEachRow) while a write names the same columns for its INSERT, so the
// two can't drift apart one method at a time.

const clickHouseEventColumnList = "id, network, contract_id, ledger, type, tx_hash, tx_index, op_index, in_successful_call, topics, value, raw_topic_xdr, raw_value_xdr, created_at"

// clickHouseEventColumns is the same column list as a slice, for INSERT.
var clickHouseEventColumns = []string{
	"id", "network", "contract_id", "ledger", "type", "tx_hash", "tx_index",
	"op_index", "in_successful_call", "topics", "value", "raw_topic_xdr",
	"raw_value_xdr", "created_at",
}

var clickHouseSubscriptionColumns = []string{
	"id", "url", "filters", "secret", "enabled", "failure_count",
	"tenant_id", "created_at", "updated_at",
}

const clickHouseSubscriptionColumnList = "id, url, filters, secret, enabled, failure_count, tenant_id, created_at, updated_at"

var clickHouseDeadLetterColumns = []string{
	"id", "event_id", "contract_id", "ledger", "type", "tx_hash",
	"topic_xdr", "value_xdr", "error", "attempts", "last_attempt", "created_at",
}

const clickHouseDeadLetterColumnList = "id, event_id, contract_id, ledger, type, tx_hash, topic_xdr, value_xdr, error, attempts, last_attempt, created_at"

func clickHouseBoolUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// clickHouseEventRow projects an Event into the column-keyed map the shared
// insertRows helper writes. A zero CreatedAt is replaced with now: ClickHouse
// DateTime64 cannot represent Go's zero year (its range starts in 1900), and
// an event's ingestion time is the sensible value when the caller left it
// unset (the Postgres column defaults to now() for the same reason).
func clickHouseEventRow(e Event) map[string]any {
	created := e.CreatedAt
	if created.IsZero() {
		created = time.Now().UTC()
	}
	row := map[string]any{
		"id":                 e.ID,
		"network":            defaultNetwork(e.Network),
		"contract_id":        e.ContractID,
		"ledger":             e.Ledger,
		"type":               e.Type,
		"tx_hash":            e.TxHash,
		"tx_index":           e.TxIndex,
		"op_index":           e.OpIndex,
		"in_successful_call": clickHouseBoolUint8(e.InSuccessfulCall),
		"topics":             clickHouseTopicsJSON(e.Topics),
		"raw_topic_xdr":      e.RawTopicXDR,
		"raw_value_xdr":      e.RawValueXDR,
		"created_at":         formatClickHouseTime(created),
	}
	if len(e.Value) > 0 {
		row["value"] = string(e.Value)
	} else {
		row["value"] = nil
	}
	return row
}

// clickHouseTopicsJSON keeps the topics column a valid JSON string even when
// the caller supplied none, matching the schema's DEFAULT '[]'.
func clickHouseTopicsJSON(topics json.RawMessage) string {
	if len(topics) == 0 {
		return "[]"
	}
	return string(topics)
}

// dedupeEventsByID keeps the first occurrence of each event ID. A single
// batch can legitimately contain the same ID twice (a retry), and inserting
// both would double-count it on a backend with no unique constraint.
func dedupeEventsByID(events []Event) []Event {
	seen := make(map[string]bool, len(events))
	out := make([]Event, 0, len(events))
	for _, e := range events {
		if seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		out = append(out, e)
	}
	return out
}

// clickHouseOwnerPredicate renders a SubscriptionOwner as a trailing
// " AND tenant_id = N" fragment. An unrestricted owner contributes nothing;
// any other — including the zero value — restricts to that tenant, and the
// zero value's 0 matches no row because ids are minted from the wall clock
// and never start at zero. This mirrors ownerPredicate in the Postgres
// implementation so the two enforce the same boundary.
func clickHouseOwnerPredicate(o SubscriptionOwner) string {
	if o.IsAll() {
		return ""
	}
	return " AND tenant_id = " + strconv.FormatInt(o.TenantID(), 10)
}

func clickHouseSubscriptionRow(s Subscription, now time.Time) (map[string]any, error) {
	filters, err := json.Marshal(s.Filters)
	if err != nil {
		return nil, fmt.Errorf("marshaling subscription filters: %w", err)
	}
	return map[string]any{
		"id":            s.ID,
		"url":           s.URL,
		"filters":       string(filters),
		"secret":        s.Secret,
		"enabled":       clickHouseBoolUint8(s.Enabled),
		"failure_count": s.FailureCount,
		"tenant_id":     s.TenantID,
		"created_at":    formatClickHouseTime(s.CreatedAt),
		"updated_at":    formatClickHouseTime(now),
	}, nil
}

func clickHouseDeadLetterRow(d DeadLetter) map[string]any {
	return map[string]any{
		"id":           d.ID,
		"event_id":     d.EventID,
		"contract_id":  d.ContractID,
		"ledger":       d.Ledger,
		"type":         d.Type,
		"tx_hash":      d.TxHash,
		"topic_xdr":    d.TopicXDR,
		"value_xdr":    d.ValueXDR,
		"error":        d.Error,
		"attempts":     d.Attempts,
		"last_attempt": formatClickHouseTime(d.LastAttempt),
		"created_at":   formatClickHouseTime(d.CreatedAt),
	}
}

// clickHouseEventCursorClause translates an EventFilter's cursor into the
// keyset predicate for the requested ordering. It mirrors the Postgres
// builder: ordering by id compares ids directly, while ledger and created_at
// carry a composite cursor (the sort value and the id tiebreaker) that needs
// a row-value comparison so a page boundary inside a run of equal sort
// values neither skips nor repeats rows.
func clickHouseEventCursorClause(f EventFilter) (string, error) {
	if f.Cursor == "" {
		return "", nil
	}
	op := ">"
	if f.Order == "desc" {
		op = "<"
	}
	switch f.OrderBy {
	case "", OrderByID:
		cursorID, err := DecodeEventIDCursorForBackend(CursorBackendClickHouse, f.Cursor)
		if err != nil {
			return "", err
		}
		return "id " + op + " " + clickHouseQuoteString(cursorID), nil
	case OrderByLedger:
		sortValue, id, err := decodeCompositeCursorForBackend(CursorBackendClickHouse, f.Cursor)
		if err != nil {
			return "", err
		}
		if _, err := strconv.ParseInt(sortValue, 10, 64); err != nil {
			return "", fmt.Errorf("%w: %w: ledger cursor", ErrInvalidCursor, ErrStaleCursor)
		}
		return fmt.Sprintf("(ledger, id) %s (%s, %s)", op, sortValue, clickHouseQuoteString(id)), nil
	case OrderByCreatedAt:
		sortValue, id, err := decodeCompositeCursorForBackend(CursorBackendClickHouse, f.Cursor)
		if err != nil {
			return "", err
		}
		t, err := parseClickHouseTime(sortValue)
		if err != nil {
			return "", fmt.Errorf("%w: %w: created_at cursor", ErrInvalidCursor, ErrStaleCursor)
		}
		return fmt.Sprintf("(created_at, id) %s ('%s', %s)", op, formatClickHouseTime(t), clickHouseQuoteString(id)), nil
	default:
		return "", fmt.Errorf("unsupported order_by %q", f.OrderBy)
	}
}

// clickHouseEventOrderColumns returns the ORDER BY list for a page. Every
// ordering ends in id so the sort is total, exactly as in Postgres.
func clickHouseEventOrderColumns(f EventFilter) string {
	dir := "ASC"
	if f.Order == "desc" {
		dir = "DESC"
	}
	if f.OrderBy == "" || f.OrderBy == OrderByID {
		return "id " + dir
	}
	return f.OrderBy + " " + dir + ", id " + dir
}

// clickHouseJoinWhere renders a WHERE clause from a list of predicates.
func clickHouseJoinWhere(where []string) string {
	if len(where) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(where, " AND ")
}
