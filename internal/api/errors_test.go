package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/store"
)

func TestClassifyError_AllCodesAndSentinels(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		err       error
		wantCode  ErrorCode
		wantField string
		wantMsg   string
	}{
		{
			name:      "INVALID_PARAMETER on query validation",
			status:    http.StatusBadRequest,
			err:       errors.New(`invalid type "bogus" (want contract|system|diagnostic)`),
			wantCode:  CodeInvalidParameter,
			wantField: "type",
			wantMsg:   `invalid type "bogus" (want contract|system|diagnostic)`,
		},
		{
			name:      "INVALID_CURSOR via store.ErrInvalidCursor sentinel",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("events page: %w", store.ErrInvalidCursor),
			wantCode:  CodeInvalidCursor,
			wantField: "cursor",
			wantMsg:   "events page: " + store.ErrInvalidCursor.Error(),
		},
		{
			name:      "INVALID_CURSOR via store.ErrInvalidContractsCursor sentinel",
			status:    http.StatusBadRequest,
			err:       fmt.Errorf("contracts page: %w", store.ErrInvalidContractsCursor),
			wantCode:  CodeInvalidCursor,
			wantField: "cursor",
			wantMsg:   "contracts page: " + store.ErrInvalidContractsCursor.Error(),
		},
		{
			name:      "RANGE_TOO_WIDE on export span",
			status:    http.StatusBadRequest,
			err:       errors.New("ledger range [1,200000] spans 200000 ledgers, exceeding EXPORT_MAX_RANGE=100000"),
			wantCode:  CodeRangeTooWide,
			wantField: "to_ledger",
			wantMsg:   "ledger range [1,200000] spans 200000 ledgers, exceeding EXPORT_MAX_RANGE=100000",
		},
		{
			name:      "LIMIT_EXCEEDED on limit parameter",
			status:    http.StatusBadRequest,
			err:       errors.New("limit must be an integer in [1,1000]"),
			wantCode:  CodeLimitExceeded,
			wantField: "limit",
			wantMsg:   "limit must be an integer in [1,1000]",
		},
		{
			name:      "LIMIT_EXCEEDED on days parameter",
			status:    http.StatusBadRequest,
			err:       errors.New("days must be an integer in [1,365]"),
			wantCode:  CodeLimitExceeded,
			wantField: "days",
			wantMsg:   "days must be an integer in [1,365]",
		},
		{
			name:      "INVALID_BODY on unknown JSON field",
			status:    http.StatusBadRequest,
			err:       errors.New(`invalid JSON body: json: unknown field "extra"`),
			wantCode:  CodeInvalidBody,
			wantField: "extra",
			wantMsg:   `invalid JSON body: json: unknown field "extra"`,
		},
		{
			name:      "INVALID_BODY on missing spec wrapper",
			status:    http.StatusBadRequest,
			err:       errors.New(`request body must contain a "spec" object`),
			wantCode:  CodeInvalidBody,
			wantField: "spec",
			wantMsg:   `request body must contain a "spec" object`,
		},
		{
			name:      "CONFIRMATION_REQUIRED on watch list mode transition",
			status:    http.StatusBadRequest,
			err:       errors.New("adding the first watched contract would switch ingestion from 'all contract events' to a specific list — pass ?confirm=true to acknowledge"),
			wantCode:  CodeConfirmationRequired,
			wantField: "confirm",
			wantMsg:   "adding the first watched contract would switch ingestion from 'all contract events' to a specific list — pass ?confirm=true to acknowledge",
		},
		{
			name:      "UNAUTHORIZED on 401",
			status:    http.StatusUnauthorized,
			err:       errors.New("missing or invalid API key"),
			wantCode:  CodeUnauthorized,
			wantField: "",
			wantMsg:   "missing or invalid API key",
		},
		{
			name:      "FORBIDDEN on 403",
			status:    http.StatusForbidden,
			err:       errors.New("admin privileges required"),
			wantCode:  CodeForbidden,
			wantField: "",
			wantMsg:   "admin privileges required",
		},
		{
			name:      "NOT_FOUND via store.ErrNotFound sentinel",
			status:    http.StatusNotFound,
			err:       fmt.Errorf("event \"0001\": %w", store.ErrNotFound),
			wantCode:  CodeNotFound,
			wantField: "",
			wantMsg:   `event "0001": ` + store.ErrNotFound.Error(),
		},
		{
			name:      "CONFLICT via store.ErrDuplicate sentinel sets name field",
			status:    http.StatusConflict,
			err:       store.ErrDuplicate,
			wantCode:  CodeConflict,
			wantField: "name",
			wantMsg:   store.ErrDuplicate.Error(),
		},
		{
			name:      "CONFLICT on tenant self-delete",
			status:    http.StatusConflict,
			err:       errors.New("a tenant cannot delete itself"),
			wantCode:  CodeConflict,
			wantField: "",
			wantMsg:   "a tenant cannot delete itself",
		},
		{
			name:      "PAYLOAD_TOO_LARGE via errBodyTooLarge",
			status:    http.StatusRequestEntityTooLarge,
			err:       fmt.Errorf("%w: exceeds 1048576 bytes", errBodyTooLarge),
			wantCode:  CodePayloadTooLarge,
			wantField: "",
			wantMsg:   errBodyTooLarge.Error() + ": exceeds 1048576 bytes",
		},
		{
			name:      "QUOTA_EXCEEDED via store.ErrQuotaExceeded",
			status:    http.StatusTooManyRequests,
			err:       store.ErrQuotaExceeded,
			wantCode:  CodeQuotaExceeded,
			wantField: "",
			wantMsg:   store.ErrQuotaExceeded.Error(),
		},
		{
			name:      "RATE_LIMITED on 429 rate limiter",
			status:    http.StatusTooManyRequests,
			err:       errors.New("rate limit exceeded; retry later"),
			wantCode:  CodeRateLimited,
			wantField: "",
			wantMsg:   "rate limit exceeded; retry later",
		},
		{
			name:      "INTERNAL_ERROR on 500 with safe message",
			status:    http.StatusInternalServerError,
			err:       errors.New("querying events failed"),
			wantCode:  CodeInternalError,
			wantField: "",
			wantMsg:   "querying events failed",
		},
		{
			name:      "UNSUPPORTED_OPERATION via store.ErrUnsupported",
			status:    http.StatusNotImplemented,
			err:       store.ErrUnsupported,
			wantCode:  CodeUnsupportedOperation,
			wantField: "",
			wantMsg:   store.ErrUnsupported.Error(),
		},
		{
			name:      "SERVICE_UNAVAILABLE on 503",
			status:    http.StatusServiceUnavailable,
			err:       errors.New("metrics not enabled"),
			wantCode:  CodeServiceUnavailable,
			wantField: "",
			wantMsg:   "metrics not enabled",
		},
	}

	seenCodes := make(map[ErrorCode]bool, len(allErrorCodeSpecs))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeError(rec, tc.status, tc.err)
			require.Equal(t, tc.status, rec.Code)

			var env errorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
			assert.Equal(t, tc.wantCode, env.Code)
			assert.Equal(t, tc.wantField, env.Field)
			assert.Equal(t, tc.wantMsg, env.Error)
			seenCodes[env.Code] = true
		})
	}

	for _, spec := range allErrorCodeSpecs {
		assert.Truef(t, seenCodes[spec.Code], "ErrorCode %s was not exercised in TestClassifyError_AllCodesAndSentinels", spec.Code)
	}
}

func TestSanitizeInternalMessage_NeverLeaksDriverOrSQLDetails(t *testing.T) {
	leaks := []string{
		"pq: syntax error at or near \"SELECT\"",
		"sql: database is closed",
		"pgx: connection refused",
		"sqlite3: database disk image is malformed",
		"dial tcp 10.0.0.12:5432: connection refused",
		"SELECT * FROM events WHERE contract_id = $1",
		"goroutine 42 [running]:\ninternal/api/handlers.go:100",
		"open /Users/operator/secret/db.sqlite: permission denied",
	}
	for _, raw := range leaks {
		rec := httptest.NewRecorder()
		writeError(rec, http.StatusInternalServerError, errors.New(raw))
		require.Equal(t, http.StatusInternalServerError, rec.Code)

		var env errorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
		assert.Equal(t, CodeInternalError, env.Code)
		assert.Equal(t, "Internal Server Error", env.Error, "raw internal error %q must be scrubbed", raw)
		assert.Empty(t, env.Field)
	}
}

func TestExtractParamField_Coverage(t *testing.T) {
	pairs := map[string]string{
		`override spec: "events" array is required`:   "spec",
		`invalid contract_id "abc"`:                   "contract_id",
		`filters.contract_id is required`:             "contract_id",
		`invalid contract id "abc"`:                   "id",
		`invalid contract ID "abc"`:                   "id",
		`invalid address "abc"`:                       "address",
		`API key id must be a positive integer`:       "id",
		`dead-letter id must be a positive integer`:   "id",
		`subscription id must be a positive integer`:  "id",
		`invalid tenant id "abc"`:                     "id",
		`invalid key_id "abc"`:                        "key_id",
		`from_ledger must be a positive integer`:      "from_ledger",
		`to_ledger must be a positive integer`:        "to_ledger",
		`before_ledger query parameter is required`:   "before_ledger",
		`from_time must be an RFC 3339 timestamp`:     "from_time",
		`to_time must be an RFC 3339 timestamp`:       "to_time",
		`invalid format "xml"`:                        "format",
		`invalid terminator (want true or false)`:     "terminator",
		`bucket parameter is required`:                "bucket",
		`unknown field "foo"`:                         "fields",
		`invalid sort "foo"`:                          "sort",
		`invalid order_by "foo"`:                      "order_by",
		`address events only support order_by=id`:     "order_by",
		`invalid order "sideways"`:                    "order",
		`invalid type "foo"`:                          "type",
		`invalid tx_index -1`:                         "tx_index",
		`invalid op_index -1`:                         "op_index",
		`topic_contains must be valid JSON`:           "topic_contains",
		`topic0: invalid JSON`:                        "topic0",
		`topic1: invalid JSON`:                        "topic1",
		`topic2: invalid JSON`:                        "topic2",
		`topic3: invalid JSON`:                        "topic3",
		`topic and topic0..topic3 filters cannot mix`: "topic",
		`version must be a positive integer`:          "version",
		`secret is required`:                          "secret",
		`url is required`:                             "url",
		`name is required`:                            "name",
		`rate_limit_rps must be non-negative`:         "rate_limit_rps",
		`rate_limit_burst must be non-negative`:       "rate_limit_burst",
		`max_watched_contracts must be non-negative`:  "max_watched_contracts",
		`invalid recent "foo"`:                        "recent",
		`invalid limit "foo"`:                         "limit",
		`invalid days "foo"`:                          "days",
		`unauthenticated`:                             "",
	}
	for msg, want := range pairs {
		assert.Equalf(t, want, extractParamField(msg), "extractParamField(%q)", msg)
	}
}
