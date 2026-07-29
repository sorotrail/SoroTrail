// Package queries provides pure query-builder helpers shared by the REST
// handlers in internal/api and the GraphQL resolvers in
// internal/api/graphql. The point of this package is to keep filter
// validation, type checking, and pagination arg normalization in ONE
// place; both transports consume it instead of duplicating the rules.
//
// Everything in this package is pure: no http.Request, no Resolver
// context, no logging. Callers translate their input into EventFilterArgs
// (REST: from *http.Request.URL.Query; GraphQL: from resolver arguments)
// and call BuildEventFilter to get a store.EventFilter ready for
// store.QueryEvents / store.CountEvents.
package queries

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// EventFilterArgs is the wire-agnostic shape both REST and GraphQL build
// before calling BuildEventFilter. Pointer fields distinguish "absent"
// from "explicitly set to the zero value" (REST treats absent as absent;
// GraphQL has nil semantics naturally).
type EventFilterArgs struct {
	ContractID    string
	Types         []string
	Topic         json.RawMessage
	T0            json.RawMessage
	T1            json.RawMessage
	T2            json.RawMessage
	T3            json.RawMessage
	TopicContains json.RawMessage
	TxHash        string
	FromLedger    int64
	ToLedger      int64
	FromTime      time.Time
	ToTime        time.Time
	Order         string
	OrderBy       string
	Cursor        string
	Limit         int
}

// PageArgs is the wire-agnostic pagination descriptor both REST and
// GraphQL use. The relay-spec "first/after" pair is forward pagination;
// "last/before" is backward (not yet supported — the store layer only
// keyset-paginates forward).
type PageArgs struct {
	// First + After is the forward pagination pair.
	First int
	After string
	// Last + Before is the backward pagination pair; reserved for
	// future compatibility. The store layer cannot service this today.
	Last    int
	Before  string
	Order   string
	OrderBy string
}

// PageSize constants. Same semantics as the REST `?limit` cap (200).
const (
	DefaultPageSize = store.DefaultQueryLimit
	MaxPageSize     = store.MaxQueryLimit
)

// CursorProbe describes a decoded Relay cursor. The store cursor is
// opaque to clients; this struct is the internal decoder the GraphQL
// pagination layer reads, while the REST handlers keep using the bare
// store-cursor string and rely on QueryEvents / CountEvents for
// pagination semantics.
type CursorProbe struct {
	LastID  string
	OrderBy string
	Order   string
}

// BuildEventFilter runs the same validation the REST `filterFromQuery`
// applies — topic / topic-position conflict check, type whitelist,
// contract-ID shape, time range sanity — and returns a store.EventFilter
// ready for the store layer. Validation errors are typed to enable
// callers to translate them into 400 BadRequest (REST) or GraphQL
// `errors[].extensions.code` (GraphQL).
//
// On the cursor argument: anything non-empty is forwarded verbatim into
// the store, which is responsible for cursoring.
//
// On the limit argument: 0 selects DefaultPageSize; values outside
// [1, MaxPageSize] produce a validation error.
func BuildEventFilter(args EventFilterArgs) (store.EventFilter, error) {
	f := store.EventFilter{
		ContractID: args.ContractID,
		Types:      args.Types,
		Topic:      args.Topic,
		Topic0:     args.T0,
		Topic1:     args.T1,
		Topic2:     args.T2,
		Topic3:     args.T3,
		TopicContains: args.TopicContains,
		TxHash:     args.TxHash,
		FromLedger: args.FromLedger,
		ToLedger:   args.ToLedger,
		FromTime:   args.FromTime,
		ToTime:     args.ToTime,
		Order:      args.Order,
		OrderBy:    args.OrderBy,
		Cursor:     args.Cursor,
		Limit:      args.Limit,
	}

	if f.ContractID != "" && !config.ValidContractID(f.ContractID) {
		return f, fmt.Errorf("invalid contract_id %q", f.ContractID)
	}
	if f.Cursor != "" && !config.ValidCursor(f.Cursor) {
		return f, fmt.Errorf("invalid cursor %q", f.Cursor)
	}

	for _, t := range f.Types {
		switch t {
		case "contract", "system", "diagnostic":
		default:
			return f, fmt.Errorf("invalid type %q (want contract|system|diagnostic)", t)
		}
	}

	if f.Order != "" && f.Order != "asc" && f.Order != "desc" {
		return f, fmt.Errorf("invalid order %q (want asc or desc)", f.Order)
	}

	if !store.ValidOrderBy(f.OrderBy) {
		return f, fmt.Errorf("invalid order_by %q (want %s, %s or %s)",
			f.OrderBy, store.OrderByID, store.OrderByLedger, store.OrderByCreatedAt)
	}

	if len(f.Topic) > 0 &&
		(len(f.Topic0) > 0 || len(f.Topic1) > 0 || len(f.Topic2) > 0 || len(f.Topic3) > 0) {
		return f, errors.New("topic and topic0..topic3 filters cannot be combined")
	}

	if f.FromLedger > 0 && f.ToLedger > 0 && f.FromLedger > f.ToLedger {
		return f, fmt.Errorf("from_ledger %d is after to_ledger %d", f.FromLedger, f.ToLedger)
	}
	if !f.FromTime.IsZero() && !f.ToTime.IsZero() && f.FromTime.After(f.ToTime) {
		return f, fmt.Errorf("from_time %s is after to_time %s",
			f.FromTime.Format(time.RFC3339), f.ToTime.Format(time.RFC3339))
	}

	if f.Limit == 0 {
		f.Limit = DefaultPageSize
	} else if f.Limit < 1 || f.Limit > MaxPageSize {
		return f, fmt.Errorf("limit must be an integer in [1,%d]", MaxPageSize)
	}

	return f, nil
}

// ResolvePage merges pagination args into an EventFilterArgs. GraphQL
// resolvers use it; the REST analog lives in the handler's
// ?limit/?recent parsing.
//
// Semantics:
//   - Limit zero => apply DefaultPageSize.
//   - first + after: take from PageArgs (forward pagination).
//   - last + before: not supported today (returns an error).
func ResolvePage(args PageArgs) (limit int, cursor string, order, orderBy string, err error) {
	if args.Last > 0 || args.Before != "" {
		return 0, "", "", "", errors.New("backward pagination (last/before) is not supported")
	}

	if args.First < 0 || args.First > MaxPageSize {
		return 0, "", "", "", fmt.Errorf("first must be in [1,%d] (got %d)", MaxPageSize, args.First)
	}
	limit = args.First
	if limit == 0 {
		limit = DefaultPageSize
	}

	cursor = args.After
	order = args.Order
	switch order {
	case "", "asc", "desc":
		// accepted; "" => asc by default in the store
	default:
		return 0, "", "", "", fmt.Errorf("invalid order %q (want asc|desc)", order)
	}
	orderBy = args.OrderBy
	switch orderBy {
	case "", store.OrderByID, store.OrderByLedger, store.OrderByCreatedAt:
		// accepted
	default:
		return 0, "", "", "", fmt.Errorf("invalid order_by %q", orderBy)
	}
	return
}

// ParseTypes splits and validates a comma-separated event-type string.
// Mirrors the REST `?type=contract,system` shape; callers also use it
// for the GraphQL `types: ["contract", "system"]` shape after splitting.
func ParseTypes(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	var out []string
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		switch t {
		case "contract", "system", "diagnostic":
		default:
			return nil, fmt.Errorf("invalid type %q (want contract|system|diagnostic)", t)
		}
		out = append(out, t)
	}
	return out, nil
}

// ParseTopic converts a raw query-string topic value (which can be
// either a JSON value or a bare string) into a json.RawMessage.
// Mirrors the REST `?topic=…` rules: bare words become quoted strings,
// JSON values pass through. The error message does NOT include a
// parameter name; callers prefix with the specific name (e.g. "topic",
// "topic0") so the user sees which input was bad.
func ParseTopic(raw string) (json.RawMessage, error) {
	if raw == "" {
		return nil, nil
	}
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw), nil
	}
	quoted, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	return quoted, nil
}

// ParseTopicContains requires a JSON value (no auto-quoting). Used by
// REST `?topic_contains=…` and by the GraphQL `topicContains` filter.
func ParseTopicContains(raw string) (json.RawMessage, error) {
	if raw == "" {
		return nil, nil
	}
	if !json.Valid([]byte(raw)) {
		return nil, errors.New("topic_contains must be valid JSON")
	}
	return json.RawMessage(raw), nil
}

// ParseLedgerParam returns 0 when raw is empty (no constraint), else
// validates raw is a positive integer. The error message does NOT
// mention a parameter name; callers prefix with the specific name
// (e.g. "from_ledger") so the user sees which input was bad.
func ParseLedgerParam(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("must be a positive integer")
	}
	return n, nil
}

// ParseTimeParam returns the zero time.Time (no constraint) on empty
// input; otherwise requires RFC3339 with second precision. The error
// message does NOT mention a parameter name; callers prefix with the
// specific name (e.g. "from_time") so the user sees which input was
// bad.
func ParseTimeParam(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("must be an RFC 3339 timestamp (e.g. 2026-07-21T00:00:00Z)")
	}
	if t.Nanosecond() != 0 {
		return time.Time{}, fmt.Errorf("sub-second precision is not supported")
	}
	return t, nil
}
