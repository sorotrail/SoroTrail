// Package rpc is a minimal JSON-RPC 2.0 client for the Stellar RPC (Soroban)
// methods SoroTrail needs: getEvents, getLatestLedger, getHealth.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
		// Older server: remember and retry once without the param.
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

func (c *HTTPClient) call(ctx context.Context, method string, params, result any) error {
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
