// Package client is the versioned Go client for the SoroTrail HTTP API.
//
// The typed surface — request/response types and one method per
// operation — is generated from api/openapi.yaml by cmd/clientgen
// (`make client`), and pkg/client/drift_test.go fails the build if the
// committed client drifts from the spec. This file holds the hand-written
// core the generated code calls: the HTTP transport, error mapping, and
// the small helpers the generated methods are compiled against.
//
// The client is versioned in lockstep with the spec: SpecVersion (in
// client.gen.go) carries the api/openapi.yaml info.version it was
// generated from. See RELEASING.md for how releases publish it.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultTimeout bounds every request; callers with long-running queries
// (large exports, slow pagination) can supply their own http.Client via
// WithHTTPClient.
const defaultTimeout = 30 * time.Second

const (
	// IdempotencyKeyHeader is the HTTP request header sent by callers to
	// deduplicate write retries within the server's retention window.
	IdempotencyKeyHeader = "Idempotency-Key"
	// IdempotencyReplayedHeader is the HTTP response header set to "true"
	// when a response was served from the idempotency store.
	IdempotencyReplayedHeader = "Idempotency-Replayed"
)

type idempotencyKeyCtxKey struct{}

// WithIdempotencyKey attaches an Idempotency-Key header value to ctx so the
// next write call made with ctx sends `Idempotency-Key: <key>`.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, idempotencyKeyCtxKey{}, key)
}

// IdempotencyKeyFromContext returns the idempotency key attached to ctx, or "".
func IdempotencyKeyFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(idempotencyKeyCtxKey{}).(string)
	return v
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the tenant API key. It is sent as
// `Authorization: Bearer <key>` on every request, matching the spec's
// TenantAuth scheme. Single-tenant instances serve anonymous requests,
// so the option is optional.
func WithAPIKey(key string) Option {
	return func(c *Client) {
		c.apiKey = key
	}
}

// WithHTTPClient replaces the default HTTP client (30s timeout). Use it
// to control transport, timeouts, or retries.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		c.http = hc
	}
}

// Client talks to one SoroTrail API instance. Construct with New; all
// generated methods are on this type.
type Client struct {
	baseURL        string
	http           *http.Client
	apiKey         string
	idempotencyKey string
}

// WithIdempotencyKey returns a shallow copy of c that sends `Idempotency-Key: <key>`
// on requests unless overridden by a context-scoped WithIdempotencyKey(ctx, ...).
func (c *Client) WithIdempotencyKey(key string) *Client {
	cp := *c
	cp.idempotencyKey = key
	return &cp
}

// New returns a Client for the given base URL (e.g.
// "https://sorotrail.example.com"). The URL is used verbatim; no path
// is appended, so point it at the API root.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http:    &http.Client{Timeout: defaultTimeout},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ErrorCode is a stable, machine-readable error code returned in the "code"
// property of every SoroTrail JSON error envelope.
type ErrorCode string

const (
	CodeInvalidParameter     ErrorCode = "INVALID_PARAMETER"
	CodeInvalidCursor        ErrorCode = "INVALID_CURSOR"
	CodeRangeTooWide         ErrorCode = "RANGE_TOO_WIDE"
	CodeLimitExceeded        ErrorCode = "LIMIT_EXCEEDED"
	CodeInvalidBody          ErrorCode = "INVALID_BODY"
	CodeConfirmationRequired ErrorCode = "CONFIRMATION_REQUIRED"
	CodeUnauthorized         ErrorCode = "UNAUTHORIZED"
	CodeForbidden            ErrorCode = "FORBIDDEN"
	CodeNotFound             ErrorCode = "NOT_FOUND"
	CodeConflict             ErrorCode = "CONFLICT"
	CodePayloadTooLarge      ErrorCode = "PAYLOAD_TOO_LARGE"
	CodeQuotaExceeded        ErrorCode = "QUOTA_EXCEEDED"
	CodeRateLimited          ErrorCode = "RATE_LIMITED"
	CodeInternalError        ErrorCode = "INTERNAL_ERROR"
	CodeUnsupportedOperation ErrorCode = "UNSUPPORTED_OPERATION"
	CodeServiceUnavailable   ErrorCode = "SERVICE_UNAVAILABLE"
)

// ErrStaleCursor is matched via errors.Is(err, client.ErrStaleCursor) or
// client.IsStaleCursor(err) when the server rejects a pagination cursor with
// HTTP 400 / code INVALID_CURSOR (for example after a major-version upgrade,
// a storage-backend migration, or a sort-order change). Callers that observe
// ErrStaleCursor should restart pagination from an explicit anchor such as
// FromLedger (or from an empty Cursor).
var ErrStaleCursor = errors.New("sorotrail: stale or invalid pagination cursor")

// APIError is returned for every non-2xx response. Message, Code, and Field
// are populated from the server's standard error envelope when the body
// carried one.
type APIError struct {
	StatusCode int
	Code       ErrorCode
	Message    string
	Field      string
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("sorotrail: %s (HTTP %d)", e.Message, e.StatusCode)
	}
	return fmt.Sprintf("sorotrail: HTTP %d", e.StatusCode)
}

// IsStaleCursor reports whether e represents a rejected pagination cursor
// (HTTP 400 with code INVALID_CURSOR, field "cursor", or a cursor-rejection
// error message), indicating that the caller should restart pagination from a
// fresh anchor (such as FromLedger) instead of retrying the same cursor.
func (e *APIError) IsStaleCursor() bool {
	if e == nil {
		return false
	}
	if e.Code == CodeInvalidCursor {
		return true
	}
	if e.StatusCode != http.StatusBadRequest {
		return false
	}
	if strings.EqualFold(e.Field, "cursor") {
		return true
	}
	msg := strings.ToLower(e.Message)
	return strings.Contains(msg, "invalid cursor") || strings.Contains(msg, "stale cursor")
}

// Is allows errors.Is(err, client.ErrStaleCursor) to match cursor-rejection
// APIErrors directly.
func (e *APIError) Is(target error) bool {
	if target == ErrStaleCursor {
		return e.IsStaleCursor()
	}
	return false
}

// IsStaleCursor reports whether err (or any wrapped *APIError) indicates a
// stale or invalid pagination cursor.
func IsStaleCursor(err error) bool {
	return errors.Is(err, ErrStaleCursor)
}

// do is the typed helper the generated object-returning methods call.
// It decodes the success body into a fresh T and returns a pointer to it.
func do[T any](c *Client, ctx context.Context, method, path string, query url.Values, body any) (*T, error) {
	var out T
	if err := c.roundTrip(ctx, method, path, query, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// doSlice is the typed helper for operations whose success body is a
// JSON array (e.g. listSubscriptions). It returns the slice by value.
func doSlice[T any](c *Client, ctx context.Context, method, path string, query url.Values, body any) ([]T, error) {
	var out []T
	if err := c.roundTrip(ctx, method, path, query, body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// doRaw is the helper for operations whose success body is not JSON
// (the CSV export, raw XDR). The raw bytes are returned untouched.
func doRaw(c *Client, ctx context.Context, method, path string, query url.Values, body any) ([]byte, error) {
	resp, err := c.send(ctx, method, path, query, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading %s %s response: %w", method, path, err)
	}
	return data, nil
}

// doNoContent is the helper for operations with no success body (204
// or empty success responses): only the error is interesting.
func doNoContent(c *Client, ctx context.Context, method, path string, query url.Values, body any) error {
	return c.roundTrip(ctx, method, path, query, body, nil)
}

// roundTrip sends the request and JSON-decodes the success body into
// out (when out is non-nil).
func (c *Client) roundTrip(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	resp, err := c.send(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding %s %s response: %w", method, path, err)
	}
	return nil
}

// send builds and executes one request, returning a non-nil error for
// transport failures and an *APIError for non-2xx responses. The caller
// owns the returned response body.
func (c *Client) send(ctx context.Context, method, path string, query url.Values, body any) (*http.Response, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding %s %s request body: %w", method, path, err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return nil, fmt.Errorf("building %s %s request: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if key := IdempotencyKeyFromContext(ctx); key != "" {
		req.Header.Set(IdempotencyKeyHeader, key)
	} else if c.idempotencyKey != "" {
		req.Header.Set(IdempotencyKeyHeader, c.idempotencyKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sorotrail: %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var errResp struct {
			Error string    `json:"error"`
			Code  ErrorCode `json:"code"`
			Field string    `json:"field"`
		}
		// The envelope is small; limit the read so a misbehaving server
		// cannot balloon the error path.
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&errResp)
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Code:       errResp.Code,
			Message:    errResp.Error,
			Field:      errResp.Field,
		}
	}
	return resp, nil
}

// urlEscapePath substitutes the {param} placeholders of a template path
// (in declaration order) with percent-escaped values. The generated
// methods pass path arguments positionally, mirroring the spec's
// parameter order.
func urlEscapePath(tmpl string, values ...string) string {
	for _, v := range values {
		start := strings.IndexByte(tmpl, '{')
		end := strings.IndexByte(tmpl, '}')
		if start < 0 || end < 0 || end < start {
			break
		}
		tmpl = tmpl[:start] + url.PathEscape(v) + tmpl[end+1:]
	}
	return tmpl
}
