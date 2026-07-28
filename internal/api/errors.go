package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// Standard error codes returned in the structured error envelope.
const (
	ErrorCodeBadRequest     = "bad_request"
	ErrorCodeNotFound       = "not_found"
	ErrorCodeRateLimited    = "rate_limited"
	ErrorCodeInternalError  = "internal_error"
	ErrorCodeNotImplemented = "not_implemented"
)

// APIError is the structured error body sent in every error response.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errorResponse is the top-level JSON envelope for errors.
type errorResponse struct {
	Error APIError `json:"error"`
}

// errorCodeForStatus maps an HTTP status code to the canonical error code.
func errorCodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return ErrorCodeBadRequest
	case http.StatusNotFound:
		return ErrorCodeNotFound
	case http.StatusTooManyRequests:
		return ErrorCodeRateLimited
	case http.StatusNotImplemented:
		return ErrorCodeNotImplemented
	case http.StatusInternalServerError:
		return ErrorCodeInternalError
	default:
		return ErrorCodeInternalError
	}
}

// writeError sends a structured JSON error response with the correct
// HTTP status code and Cache-Control: no-store.
func writeError(w http.ResponseWriter, status int, err error) {
	writeCacheHeaders(w, cacheNoStore, 0, "")
	writeJSON(w, status, errorResponse{
		Error: APIError{
			Code:    errorCodeForStatus(status),
			Message: err.Error(),
		},
	})
}

// writeErrorString sends a structured JSON error response directly from a
// string message, avoiding the need to create an error first.
func writeErrorString(w http.ResponseWriter, status int, msg string) {
	writeError(w, status, fmt.Errorf("%s", msg))
}

// --- Validators ---

// allowedQueryParams is the set of recognised query-string keys for event
// endpoints. Any key outside this set triggers a 400 rejection.
var allowedQueryParams = []string{
	"contract_id",
	"type",
	"topic",
	"topic0",
	"topic1",
	"topic2",
	"topic3",
	"from_ledger",
	"to_ledger",
	"from_time",
	"to_time",
	"cursor",
	"limit",
	"order",
	"decoded",
}

// knownQueryParam is a set for O(1) lookup during unknown-param rejection.
var knownQueryParam = func() map[string]bool {
	m := make(map[string]bool, len(allowedQueryParams))
	for _, k := range allowedQueryParams {
		m[k] = true
	}
	return m
}()

// rejectUnknownParams checks that every key in the query string is one of the
// recognised keys. Returns the first unknown key as an error.
func rejectUnknownParams(q map[string][]string) error {
	for key := range q {
		if !knownQueryParam[key] {
			return fmt.Errorf("unknown query parameter %q (allowed: %s)",
				key, strings.Join(allowedQueryParams, ", "))
		}
	}
	return nil
}

// ValidateContractID reports whether s is a valid Soroban contract strkey
// and returns a user-facing error when it is not.
func ValidateContractID(s string) error {
	if !config.ValidContractID(s) {
		return fmt.Errorf("invalid contract ID %q (want a C... strkey, 56 characters)", s)
	}
	return nil
}

// validateEventType checks the event type parameter is one of the allowed
// values. An empty string (unset) is allowed.
func validateEventType(t string) error {
	switch t {
	case "", "contract", "system", "diagnostic":
		return nil
	default:
		return fmt.Errorf("invalid type %q (allowed: contract, system, diagnostic)", t)
	}
}

// validateOrder checks the order parameter is valid.
func validateOrder(o string) error {
	switch o {
	case "", "asc", "desc":
		return nil
	default:
		return fmt.Errorf("invalid order %q (allowed: asc, desc)", o)
	}
}

// validateLedgerRange checks that from_ledger <= to_ledger when both are set.
func validateLedgerRange(from, to int64) error {
	if from > 0 && to > 0 && from > to {
		return fmt.Errorf("from_ledger %d is after to_ledger %d", from, to)
	}
	return nil
}

// validateTimeRange checks that from_time <= to_time when both are set.
func validateTimeRange(from, to time.Time) error {
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return fmt.Errorf("from_time %s is after to_time %s",
			from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	return nil
}

// parseLedgerParam parses a ledger query parameter.
func parseLedgerParam(raw, name string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

// parseTimeParam parses an RFC 3339 timestamp query parameter.
// Sub-second precision and missing timezone offset are rejected.
func parseTimeParam(raw, name string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be an RFC 3339 timestamp (e.g. 2026-07-21T00:00:00Z)", name)
	}
	if t.Nanosecond() != 0 {
		return time.Time{}, fmt.Errorf("%s sub-second precision is not supported", name)
	}
	return t, nil
}

// parseTopicParam parses a topic query parameter. It accepts valid JSON
// directly, and bare words are wrapped as JSON strings.
func parseTopicParam(name, raw string) (json.RawMessage, error) {
	if raw == "" {
		return nil, nil
	}
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw), nil
	}
	quoted, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	return quoted, nil
}

// validateCursor checks that a cursor value has the expected event-ID shape.
// Cursors are opaque to clients but internally are TOID event IDs.
func validateCursor(cursor string) error {
	if cursor == "" {
		return nil
	}
	// TOID format: \d+-\d+
	parts := strings.SplitN(cursor, "-", 3)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid cursor %q (want TOID format, e.g. \"0001099511627776-0000000001\")", cursor)
	}
	if _, err := strconv.ParseInt(parts[0], 10, 64); err != nil {
		return fmt.Errorf("invalid cursor %q", cursor)
	}
	if _, err := strconv.ParseInt(parts[1], 10, 64); err != nil {
		return fmt.Errorf("invalid cursor %q", cursor)
	}
	return nil
}

// parseSubscriptionID parses and validates a subscription ID from a raw
// URL parameter string.
func parseSubscriptionID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("subscription id must be a positive integer, got %q", raw)
	}
	return id, nil
}
// --- Filter building ---

// filterFromQuery parses the shared event-filter query params:
// contract_id, type, topic, topic0..topic3, from_ledger, to_ledger,
// from_time, to_time, cursor, limit, order.
func filterFromQuery(r *http.Request) (store.EventFilter, error) {
	q := r.URL.Query()

	// Reject unknown query parameters before processing anything else.
	if err := rejectUnknownParams(q); err != nil {
		return store.EventFilter{}, err
	}

	f := store.EventFilter{
		ContractID: q.Get("contract_id"),
		Cursor:     q.Get("cursor"),
	}

	if f.ContractID != "" {
		if err := ValidateContractID(f.ContractID); err != nil {
			return f, err
		}
	}

	if err := validateEventType(q.Get("type")); err != nil {
		return f, err
	}
	f.Type = q.Get("type")

	if err := validateOrder(q.Get("order")); err != nil {
		return f, err
	}
	f.Order = q.Get("order")

	var err error
	if topic := q.Get("topic"); topic != "" {
		parsed, err := parseTopicParam("topic", topic)
		if err != nil {
			return f, err
		}
		f.Topic = parsed
	}

	if f.Topic0, err = parseTopicParam("topic0", q.Get("topic0")); err != nil {
		return f, err
	}
	if f.Topic1, err = parseTopicParam("topic1", q.Get("topic1")); err != nil {
		return f, err
	}
	if f.Topic2, err = parseTopicParam("topic2", q.Get("topic2")); err != nil {
		return f, err
	}
	if f.Topic3, err = parseTopicParam("topic3", q.Get("topic3")); err != nil {
		return f, err
	}

	if len(f.Topic) > 0 && (len(f.Topic0) > 0 || len(f.Topic1) > 0 || len(f.Topic2) > 0 || len(f.Topic3) > 0) {
		return f, fmt.Errorf("topic and topic0..topic3 filters cannot be combined")
	}

	if f.FromLedger, err = parseLedgerParam(q.Get("from_ledger"), "from_ledger"); err != nil {
		return f, err
	}
	if f.ToLedger, err = parseLedgerParam(q.Get("to_ledger"), "to_ledger"); err != nil {
		return f, err
	}
	if err := validateLedgerRange(f.FromLedger, f.ToLedger); err != nil {
		return f, err
	}

	if f.FromTime, err = parseTimeParam(q.Get("from_time"), "from_time"); err != nil {
		return f, err
	}
	if f.ToTime, err = parseTimeParam(q.Get("to_time"), "to_time"); err != nil {
		return f, err
	}
	if err := validateTimeRange(f.FromTime, f.ToTime); err != nil {
		return f, err
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > store.MaxQueryLimit {
			return f, fmt.Errorf("limit must be an integer in [1,%d]", store.MaxQueryLimit)
		}
		f.Limit = limit
	}

	if err := validateCursor(f.Cursor); err != nil {
		return f, err
	}

	return f, nil
}
