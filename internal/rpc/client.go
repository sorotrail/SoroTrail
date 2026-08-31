// Package rpc is a minimal JSON-RPC 2.0 client for the Stellar RPC (Soroban)
// methods SoroTrail needs: getEvents, getLatestLedger, getHealth.
//
// The entry point is [NewHTTPClient], which returns an [*HTTPClient]
// implementing [Client]. The ingester and API depend on the [Client]
// interface, not on the concrete type, so tests can substitute a mock.
//
// Non-obvious contracts:
//   - Requests are rate-limited by default (≥100ms apart, ~10 req/s)
//     via [WithMinRequestInterval]. Set to 0 to disable.
//   - [HTTPClient] auto-detects whether the server supports
//     xdrFormat: "json". If the server rejects it, the client flips
//     a flag and falls back to returning raw XDR for callers to
//     decode locally.
//   - [IsLedgerOutOfRange] should be checked after GetEvents to detect
//     when the resume point has aged out of the RPC's retention window.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sorotrail/sorotrail/internal/metrics"
)

// Client is the RPC boundary. The ingester and API depend on this interface

// so tests can substitute a mock.
type Client interface {
	GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error)
	GetLatestLedger(ctx context.Context) (LatestLedger, error)
	GetHealth(ctx context.Context) (Health, error)
	// GetLedgerEntries returns the current state of one or more ledger entries.
	// Keys are base64-encoded LedgerKey XDR, returned entries include the
	// base64-encoded LedgerEntry XDR.
	GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error)
	// SimulateTransaction simulates a transaction (typically a contract
	// invocation) against the current ledger state. Used by the contract
	// metadata worker to call SEP-41 token interface functions (name,
	// symbol, decimals) without submitting a real transaction.
	SimulateTransaction(ctx context.Context, req SimulateTransactionRequest) (SimulateTransactionResponse, error)
}

// RequestObserver is called after each RPC call completes so callers can
// instrument request counts by method and outcome without the rpc package
// importing a metrics library.
type RequestObserver interface {
	ObserveRPCRequest(method string, err error)
}

// Error is a JSON-RPC 2.0 error object returned by the server.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// RateLimitedError is returned when the RPC endpoint answers with HTTP 429.
// It carries the parsed Retry-After hint so the retry layer can wait
// exactly as long as the provider asked instead of guessing with blind
// exponential backoff. RetryAfter is 0 when the header is absent or
// unparseable, in which case the caller falls back to computed backoff.
type RateLimitedError struct {
	// StatusCode is the HTTP status (always 429 today).
	StatusCode int
	// RetryAfter is the wait requested by the provider, parsed from a
	// delta-seconds or HTTP-date Retry-After header. Zero when absent.
	RetryAfter time.Duration
	// Body is a truncated copy of the response body for diagnostics.
	Body string
}

func (e *RateLimitedError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("RPC endpoint returned HTTP %d (rate limited, retry-after %s): %s", e.StatusCode, e.RetryAfter, e.Body)
	}
	return fmt.Sprintf("RPC endpoint returned HTTP %d (rate limited): %s", e.StatusCode, e.Body)
}

// parseRetryAfter decodes a Retry-After header value (RFC 7231 §7.1.3):
// either delta-seconds ("30") or an HTTP-date ("Wed, 21 Oct 2026 07:28:00 GMT").
// It returns 0 for absent, negative (already elapsed), or malformed values —
// callers treat 0 as "no hint".
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if secs, err := strconv.Atoi(header); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(header); err == nil {
		d := time.Until(t)
		if d <= 0 {
			return 0
		}
		return d
	}
	return 0
}

// IsLedgerOutOfRange reports whether err indicates the requested startLedger
// has fallen outside the RPC's retention window, so the caller should
// re-clamp to the oldest retained ledger.
func IsLedgerOutOfRange(err error) bool {
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	msg := strings.ToLower(rpcErr.Message + " " + rpcErr.Data)
	return strings.Contains(msg, "ledger range") ||
		strings.Contains(msg, "outside of retention window") ||
		strings.Contains(msg, "must be within")
}

// HTTPClient talks JSON-RPC 2.0 over HTTP POST, with a request-rate cap for

// public endpoints and automatic fallback for servers that don't support

// xdrFormat: "json".

type HTTPClient struct {
	url        string
	httpClient *http.Client
	limiter    *intervalLimiter
	reqID      atomic.Int64

	// xdrJSONUnsupported flips to true once the server rejects the xdrFormat
	// param, so we stop sending it and callers decode raw XDR instead.
	xdrJSONUnsupported atomic.Bool

	// requestObserver, when non-nil, is called after every call() completes.
	requestObserver RequestObserver
}

var _ Client = (*HTTPClient)(nil)

// Option customizes an HTTPClient.
type Option func(*HTTPClient)

// WithHTTPClient replaces the underlying HTTP client (e.g. for tests).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *HTTPClient) { c.httpClient = hc }
}

// WithMinRequestInterval sets the minimum spacing between requests.
// Zero disables rate limiting.
func WithMinRequestInterval(d time.Duration) Option {
	return func(c *HTTPClient) { c.limiter = newIntervalLimiter(d) }
}

// WithRateLimitRPS caps the request rate at rps requests/second.
// Values ≤ 0 keep the client's default spacing, so callers can pass a
// config value straight through without pre-validating it (config.Load
// rejects non-positive values anyway).
func WithRateLimitRPS(rps float64) Option {
	return func(c *HTTPClient) {
		if rps <= 0 {
			return
		}
		c.limiter = newIntervalLimiter(time.Duration(float64(time.Second) / rps))
	}
}

// WithHTTPTimeout sets the timeout on the underlying HTTP client used for
// RPC requests. Values ≤ 0 keep the client's default (30s).
func WithHTTPTimeout(d time.Duration) Option {
	return func(c *HTTPClient) {
		if d <= 0 {
			return
		}
		c.httpClient.Timeout = d
	}
}

// WithRequestObserver sets an observer that is called after every RPC call
// with the JSON-RPC method name and any error that occurred.
func WithRequestObserver(obs RequestObserver) Option {
	return func(c *HTTPClient) { c.requestObserver = obs }
}

// NewHTTPClient creates a client for the RPC server at url. By default
// requests are spaced ≥100ms apart (~10 req/s, the public endpoint limit).
func NewHTTPClient(url string, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		url:        url,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		limiter:    newIntervalLimiter(100 * time.Millisecond),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HTTPClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	// The RPC rejects requests that set both a pagination cursor and a
	// ledger range — the cursor alone defines the position.
	if req.Pagination != nil && req.Pagination.Cursor != "" {
		req.StartLedger = 0
		req.EndLedger = 0
	}
	if !c.xdrJSONUnsupported.Load() {
		req.XDRFormat = XDRFormatJSON
	} else {
		req.XDRFormat = ""
	}

	var resp GetEventsResponse
	err := c.call(ctx, "getEvents", req, &resp)
	if err != nil && isXDRFormatRejected(err) {
		// Older server: remember and retry once without the param. The
		// retried call observes its own latency via call() — no manual
		// observation here.
		c.xdrJSONUnsupported.Store(true)
		req.XDRFormat = ""
		err = c.call(ctx, "getEvents", req, &resp)
	}
	return resp, err
}

func (c *HTTPClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	var resp LatestLedger
	err := c.call(ctx, "getLatestLedger", nil, &resp)
	return resp, err
}

func (c *HTTPClient) GetHealth(ctx context.Context) (Health, error) {
	var resp Health
	err := c.call(ctx, "getHealth", nil, &resp)
	return resp, err
}

func (c *HTTPClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	var resp GetLedgerEntriesResponse
	err := c.call(ctx, "getLedgerEntries", req, &resp)
	return resp, err
}

func (c *HTTPClient) SimulateTransaction(ctx context.Context, req SimulateTransactionRequest) (SimulateTransactionResponse, error) {
	var resp SimulateTransactionResponse
	err := c.call(ctx, "simulateTransaction", req, &resp)
	return resp, err
}

func isXDRFormatRejected(err error) bool {
	var rpcErr *Error
	if !errors.As(err, &rpcErr) {
		return false
	}
	return strings.Contains(strings.ToLower(rpcErr.Message+" "+rpcErr.Data), "xdrformat")
}

type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *Error          `json:"error"`
}

// call is the single choke point every JSON-RPC request flows through, so
// it is also the single place RPC latency is observed. Each call records
// its duration once, labelled by method and outcome (success | error); the
// per-method wrappers never observe on their own, so a call is counted
// exactly once even when GetEvents retries internally after an XDR-format
// rejection.
func (c *HTTPClient) call(ctx context.Context, method string, params, result any) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		metrics.RPCCallLatency.WithLabelValues(method, outcome).Observe(time.Since(start).Seconds())
	}()

	if err := c.limiter.Wait(ctx); err != nil {
		return err
	}

	body, err := json.Marshal(request{
		JSONRPC: "2.0",
		ID:      c.reqID.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("marshaling %s request: %w", method, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building %s request: %w", method, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("calling %s: %w", method, err)
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(httpResp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("reading %s response: %w", method, err)
	}
	if httpResp.StatusCode != http.StatusOK {
		// Rate limiting gets a typed error carrying the provider's
		// Retry-After hint, so the retry layer can honor it instead of
		// blind exponential backoff (issue #58).
		if httpResp.StatusCode == http.StatusTooManyRequests {
			return &RateLimitedError{
				StatusCode: httpResp.StatusCode,
				RetryAfter: parseRetryAfter(httpResp.Header.Get("Retry-After")),
				Body:       truncate(respBody, 200),
			}
		}
		return fmt.Errorf("%s returned HTTP %d: %s", method, httpResp.StatusCode, truncate(respBody, 200))
	}

	var rpcResp response
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return fmt.Errorf("decoding %s response: %w", method, err)
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("%s: %w", method, rpcResp.Error)
	}
	if result != nil {
		if err := json.Unmarshal(rpcResp.Result, result); err != nil {
			return fmt.Errorf("decoding %s result: %w", method, err)
		}
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// intervalLimiter enforces a minimum time between requests. A nil-duration
// limiter never blocks.
type intervalLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newIntervalLimiter(interval time.Duration) *intervalLimiter {
	return &intervalLimiter{interval: interval}
}

func (l *intervalLimiter) Wait(ctx context.Context) error {
	if l == nil || l.interval <= 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	wait := l.next.Sub(now)
	if wait < 0 {
		wait = 0
	}
	l.next = now.Add(wait + l.interval)
	l.mu.Unlock()

	if wait == 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
