package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/sorotrail/sorotrail/internal/store"
)

// ErrorCode is a stable, machine-readable identifier returned in the "code"
// property of every JSON ErrorResponse envelope. Clients should branch on
// ErrorCode rather than matching human-readable error strings.
type ErrorCode string

const (
	// CodeInvalidParameter indicates a query parameter, path parameter, or
	// semantic request field failed validation (HTTP 400).
	CodeInvalidParameter ErrorCode = "INVALID_PARAMETER"

	// CodeInvalidCursor indicates an opaque pagination cursor is malformed or
	// was replayed against a different sort order (HTTP 400).
	CodeInvalidCursor ErrorCode = "INVALID_CURSOR"

	// CodeRangeTooWide indicates the requested ledger span exceeds the
	// configured maximum range such as EXPORT_MAX_RANGE (HTTP 400).
	CodeRangeTooWide ErrorCode = "RANGE_TOO_WIDE"

	// CodeLimitExceeded indicates a page-size (limit) or window (days)
	// parameter is outside the allowed [1, max] bounds (HTTP 400).
	CodeLimitExceeded ErrorCode = "LIMIT_EXCEEDED"

	// CodeInvalidBody indicates the HTTP request body is empty, malformed
	// JSON, contains unknown fields, or fails structural decoding (HTTP 400).
	CodeInvalidBody ErrorCode = "INVALID_BODY"

	// CodeConfirmationRequired indicates a watch-list mutation that switches
	// global ingestion mode was called without ?confirm=true (HTTP 400).
	CodeConfirmationRequired ErrorCode = "CONFIRMATION_REQUIRED"

	// CodeUnauthorized indicates missing, malformed, unknown, or revoked
	// authentication credentials (HTTP 401).
	CodeUnauthorized ErrorCode = "UNAUTHORIZED"

	// CodeForbidden indicates the authenticated principal is suspended or
	// lacks access to the target contract or admin surface (HTTP 403).
	CodeForbidden ErrorCode = "FORBIDDEN"

	// CodeNotFound indicates the requested resource does not exist or is
	// invisible to the caller's scope (HTTP 404).
	CodeNotFound ErrorCode = "NOT_FOUND"

	// CodeConflict indicates the request conflicts with current state, such as
	// a duplicate tenant name or self-deletion (HTTP 409).
	CodeConflict ErrorCode = "CONFLICT"

	// CodePayloadTooLarge indicates the request body exceeds
	// HTTP_REQUEST_BODY_LIMIT (HTTP 413).
	CodePayloadTooLarge ErrorCode = "PAYLOAD_TOO_LARGE"

	// CodeQuotaExceeded indicates the tenant's watched-contract quota would be
	// exceeded (HTTP 429).
	CodeQuotaExceeded ErrorCode = "QUOTA_EXCEEDED"

	// CodeRateLimited indicates the caller exceeded its request rate, burst,
	// or periodic request allowance (HTTP 429).
	CodeRateLimited ErrorCode = "RATE_LIMITED"

	// CodeInternalError indicates an unexpected store or server failure
	// (HTTP 500). Internal driver details are never exposed to the client.
	CodeInternalError ErrorCode = "INTERNAL_ERROR"

	// CodeUnsupportedOperation indicates the configured store backend or
	// server instance does not support the operation (HTTP 501).
	CodeUnsupportedOperation ErrorCode = "UNSUPPORTED_OPERATION"

	// CodeServiceUnavailable indicates a required subsystem is not configured
	// or unavailable (HTTP 503).
	CodeServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
)

// errorCodeSpec describes one ErrorCode for OpenAPI verification and
// documentation generation.
type errorCodeSpec struct {
	Code        ErrorCode
	Status      int
	Description string
}

// allErrorCodeSpecs lists every defined ErrorCode in canonical order.
var allErrorCodeSpecs = []errorCodeSpec{
	{
		Code:        CodeInvalidParameter,
		Status:      http.StatusBadRequest,
		Description: "A query parameter, path parameter, or request field failed validation.",
	},
	{
		Code:        CodeInvalidCursor,
		Status:      http.StatusBadRequest,
		Description: "The pagination cursor is malformed or was replayed against a different sort order (`store.ErrInvalidCursor`, `store.ErrInvalidContractsCursor`).",
	},
	{
		Code:        CodeRangeTooWide,
		Status:      http.StatusBadRequest,
		Description: "The requested ledger span exceeds the operator's configured maximum (`EXPORT_MAX_RANGE`).",
	},
	{
		Code:        CodeLimitExceeded,
		Status:      http.StatusBadRequest,
		Description: "The requested page size (`limit`) or usage window (`days`) is outside the allowed `[1, max]` bounds.",
	},
	{
		Code:        CodeInvalidBody,
		Status:      http.StatusBadRequest,
		Description: "The request body is empty, malformed JSON, contains unknown fields, or fails structural schema decoding.",
	},
	{
		Code:        CodeConfirmationRequired,
		Status:      http.StatusBadRequest,
		Description: "Adding the first or removing the last watched contract requires `?confirm=true` to acknowledge switching global ingestion mode.",
	},
	{
		Code:        CodeUnauthorized,
		Status:      http.StatusUnauthorized,
		Description: "The request is missing credentials or presents a malformed, unknown, or revoked API key.",
	},
	{
		Code:        CodeForbidden,
		Status:      http.StatusForbidden,
		Description: "The authenticated tenant is disabled, lacks administrative privileges, or requested a contract outside its grants.",
	},
	{
		Code:        CodeNotFound,
		Status:      http.StatusNotFound,
		Description: "The requested resource does not exist or is invisible to the caller's tenant scope (`store.ErrNotFound`).",
	},
	{
		Code:        CodeConflict,
		Status:      http.StatusConflict,
		Description: "The mutation conflicts with existing state, such as a duplicate tenant name (`store.ErrDuplicate`) or tenant self-deletion.",
	},
	{
		Code:        CodePayloadTooLarge,
		Status:      http.StatusRequestEntityTooLarge,
		Description: "The request body exceeds `HTTP_REQUEST_BODY_LIMIT`.",
	},
	{
		Code:        CodeQuotaExceeded,
		Status:      http.StatusTooManyRequests,
		Description: "The tenant's `max_watched_contracts` cap would be exceeded (`store.ErrQuotaExceeded`).",
	},
	{
		Code:        CodeRateLimited,
		Status:      http.StatusTooManyRequests,
		Description: "The caller exceeded its request rate, burst, or hourly/daily request quota.",
	},
	{
		Code:        CodeInternalError,
		Status:      http.StatusInternalServerError,
		Description: "An unexpected internal or store error occurred. Specifics are logged against `X-Request-ID` and never leaked in the response.",
	},
	{
		Code:        CodeUnsupportedOperation,
		Status:      http.StatusNotImplemented,
		Description: "The configured store backend or server instance does not implement the requested capability (`store.ErrUnsupported`).",
	},
	{
		Code:        CodeServiceUnavailable,
		Status:      http.StatusServiceUnavailable,
		Description: "A required subsystem (such as `API_KEY` configuration or the metrics collector) is not enabled.",
	},
}

// classifyError maps an HTTP status and error value to its stable ErrorCode,
// optional offending field/parameter name, and safe client-facing message.
func classifyError(status int, err error) (ErrorCode, string, string) {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	if msg == "" {
		msg = http.StatusText(status)
	}

	// 1. Sentinel error classification takes precedence when status matches.
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidCursor), errors.Is(err, store.ErrInvalidContractsCursor):
			return CodeInvalidCursor, "cursor", msg
		case errors.Is(err, errBodyTooLarge) || status == http.StatusRequestEntityTooLarge:
			return CodePayloadTooLarge, "", msg
		case errors.Is(err, store.ErrQuotaExceeded):
			return CodeQuotaExceeded, "", msg
		case errors.Is(err, store.ErrDuplicate) && status == http.StatusConflict:
			return CodeConflict, "name", msg
		case errors.Is(err, store.ErrIdempotencyKeyMismatch) && status == http.StatusConflict:
			return CodeConflict, IdempotencyKeyHeader, msg
		case errors.Is(err, store.ErrUnsupported) && status == http.StatusNotImplemented:
			return CodeUnsupportedOperation, "", msg
		case errors.Is(err, store.ErrNotFound) && status == http.StatusNotFound:
			return CodeNotFound, "", msg
		}
	}

	// 2. Status-driven classification.
	switch {
	case status == http.StatusUnauthorized:
		return CodeUnauthorized, "", msg
	case status == http.StatusForbidden:
		return CodeForbidden, "", msg
	case status == http.StatusNotFound:
		return CodeNotFound, "", msg
	case status == http.StatusConflict:
		return CodeConflict, "", msg
	case status == http.StatusRequestEntityTooLarge:
		return CodePayloadTooLarge, "", msg
	case status == http.StatusTooManyRequests:
		if strings.Contains(msg, "quota exceeded") {
			return CodeQuotaExceeded, "", msg
		}
		return CodeRateLimited, "", msg
	case status == http.StatusNotImplemented:
		return CodeUnsupportedOperation, "", msg
	case status == http.StatusServiceUnavailable:
		return CodeServiceUnavailable, "", msg
	case status >= http.StatusInternalServerError:
		return CodeInternalError, "", sanitizeInternalMessage(msg)
	case status == http.StatusBadRequest:
		return classifyBadRequest(msg)
	default:
		return CodeInternalError, "", sanitizeInternalMessage(msg)
	}
}

func classifyBadRequest(msg string) (ErrorCode, string, string) {
	switch {
	case strings.Contains(msg, "confirm=true"):
		return CodeConfirmationRequired, "confirm", msg

	case msg == "request body is empty",
		strings.HasPrefix(msg, "invalid JSON"),
		strings.HasPrefix(msg, "reading request body:"),
		strings.HasPrefix(msg, "request body must contain"),
		strings.HasPrefix(msg, "parsing override spec JSON:"):
		return CodeInvalidBody, extractBodyField(msg), msg

	case strings.Contains(msg, "cursor"):
		return CodeInvalidCursor, "cursor", msg

	case strings.HasPrefix(msg, "ledger range ["),
		strings.Contains(msg, "exceeding EXPORT_MAX_RANGE"):
		return CodeRangeTooWide, "to_ledger", msg

	case strings.HasPrefix(msg, "limit must be an integer in [1,"):
		return CodeLimitExceeded, "limit", msg

	case strings.HasPrefix(msg, "days must be an integer in [1,"):
		return CodeLimitExceeded, "days", msg

	default:
		return CodeInvalidParameter, extractParamField(msg), msg
	}
}

func extractBodyField(msg string) string {
	const unknownPrefix = `json: unknown field "`
	if i := strings.Index(msg, unknownPrefix); i >= 0 {
		rest := msg[i+len(unknownPrefix):]
		if j := strings.IndexByte(rest, '"'); j > 0 {
			return rest[:j]
		}
	}
	if strings.HasPrefix(msg, "request body must contain a \"spec\" object") ||
		strings.HasPrefix(msg, "parsing override spec JSON:") {
		return "spec"
	}
	return ""
}

func extractParamField(msg string) string {
	switch {
	case strings.HasPrefix(msg, "override spec:"):
		return "spec"
	case strings.HasPrefix(msg, "invalid contract_id"),
		strings.HasPrefix(msg, "contract_id "),
		strings.HasPrefix(msg, "filters.contract_id"):
		return "contract_id"
	case strings.HasPrefix(msg, "invalid contract id"),
		strings.HasPrefix(msg, "invalid contract ID"):
		return "id"
	case strings.HasPrefix(msg, "invalid address"):
		return "address"
	case strings.HasPrefix(msg, "API key id"),
		strings.HasPrefix(msg, "dead-letter id"),
		strings.HasPrefix(msg, "subscription id"),
		strings.HasPrefix(msg, "invalid tenant id"):
		return "id"
	case strings.HasPrefix(msg, "invalid key_id"),
		strings.HasPrefix(msg, "invalid key id"):
		return "key_id"
	case strings.HasPrefix(msg, "from_ledger"):
		return "from_ledger"
	case strings.HasPrefix(msg, "to_ledger"):
		return "to_ledger"
	case strings.HasPrefix(msg, "before_ledger"):
		return "before_ledger"
	case strings.HasPrefix(msg, "from_time"):
		return "from_time"
	case strings.HasPrefix(msg, "to_time"):
		return "to_time"
	case strings.HasPrefix(msg, "invalid format"):
		return "format"
	case strings.HasPrefix(msg, "invalid terminator"):
		return "terminator"
	case strings.HasPrefix(msg, "bucket "),
		strings.HasPrefix(msg, "invalid bucket"):
		return "bucket"
	case strings.HasPrefix(msg, "unknown field "):
		return "fields"
	case strings.HasPrefix(msg, "invalid sort"):
		return "sort"
	case strings.HasPrefix(msg, "invalid order_by"),
		strings.Contains(msg, "order_by="):
		return "order_by"
	case strings.HasPrefix(msg, "invalid order"):
		return "order"
	case strings.HasPrefix(msg, "invalid type"):
		return "type"
	case strings.HasPrefix(msg, "invalid tx_index"):
		return "tx_index"
	case strings.HasPrefix(msg, "invalid op_index"):
		return "op_index"
	case strings.HasPrefix(msg, "topic_contains"):
		return "topic_contains"
	case strings.HasPrefix(msg, "topic0:"):
		return "topic0"
	case strings.HasPrefix(msg, "topic1:"):
		return "topic1"
	case strings.HasPrefix(msg, "topic2:"):
		return "topic2"
	case strings.HasPrefix(msg, "topic3:"):
		return "topic3"
	case strings.HasPrefix(msg, "topic"):
		return "topic"
	case strings.HasPrefix(msg, "version "):
		return "version"
	case strings.HasPrefix(msg, "secret "):
		return "secret"
	case strings.HasPrefix(msg, "url "):
		return "url"
	case strings.HasPrefix(msg, "name "):
		return "name"
	case strings.HasPrefix(msg, "rate_limit_rps"):
		return "rate_limit_rps"
	case strings.HasPrefix(msg, "rate_limit_burst"):
		return "rate_limit_burst"
	case strings.HasPrefix(msg, "max_watched_contracts"):
		return "max_watched_contracts"
	case strings.HasPrefix(msg, "recent "),
		strings.HasPrefix(msg, "invalid recent"):
		return "recent"
	case strings.HasPrefix(msg, "limit "),
		strings.HasPrefix(msg, "invalid limit"):
		return "limit"
	case strings.HasPrefix(msg, "days "),
		strings.HasPrefix(msg, "invalid days"):
		return "days"
	case strings.HasPrefix(msg, "Idempotency-Key"):
		return IdempotencyKeyHeader
	default:
		return ""
	}
}

// sanitizeInternalMessage ensures 5xx responses never leak internal database,
// driver, network, or stack details even if an unmapped error reaches writeError.
func sanitizeInternalMessage(msg string) string {
	lower := strings.ToLower(msg)
	for _, marker := range []string{
		"sql:",
		"pq:",
		"pgx",
		"sqlite",
		"syntax error",
		"connection refused",
		"dial tcp",
		"goroutine ",
		"panic:",
		"/users/",
		"/home/",
		"/var/",
		"select ",
		"insert into",
		"update ",
		"delete from",
	} {
		if strings.Contains(lower, marker) {
			return http.StatusText(http.StatusInternalServerError)
		}
	}
	if strings.Contains(msg, "\n") {
		return http.StatusText(http.StatusInternalServerError)
	}
	return msg
}
