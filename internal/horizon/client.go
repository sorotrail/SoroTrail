// Horizon REST client. Mirrors the small "minimal HTTP wrapper for one
// Stellar API" pattern in internal/rpc/client.go: explicit rate limiter,
// simple JSON decoding, no SDK.
//
// Producing and consuming both happen here; the XDR meta extraction
// (the part that turns a Tx result_meta_xdr into store.Event rows) lives
// in extract.go so this file stays a thin HTTP boundary.
package horizon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client is the Horizon boundary. Tests can substitute a fake by
// implementing the same interface.
type Client interface {
	// ListContractTransactions walks every transaction that touches
	// contractID, in ascending ledger/transaction order, with Horizon
	// cursor pagination. The cursor argument is opaque — pass "" for the
	// first page, then the value returned in TransactionsResponse.
	//
	// Callers should pass limit in 1..200; 200 matches Horizon's cap.
	// includeFailed=true matches the live ingester's behavior: a
	// transaction whose contract call succeeded but that other ops
	// failed still carries events from the call we care about.
	ListContractTransactions(ctx context.Context, contractID, cursor string, limit int, includeFailed bool) (TransactionsResponse, error)
}

// ErrRateLimited is returned when Horizon replied 429. Callers should
// back off and retry.
var ErrRateLimited = errors.New("horizon rate limited")

// ErrNotFound is returned for 404 — typically a malformed URL or the
// account not existing on this Horizon instance.
var ErrNotFound = errors.New("horizon not found")

// HTTPClient implements Client over plain HTTP.
type HTTPClient struct {
	baseURL string
	http    *http.Client
	limiter *intervalLimiter
}

// NewHTTPClient configures a client targeting baseURL. minInterval is the
// minimum gap between requests (a simple spacing token bucket that matches
// the internal/rpc/client.go pattern so we don't have two rate-limit
// implementations in the tree). 100ms ≈ 10 req/s, the public Horizon cap.
func NewHTTPClient(baseURL string, minInterval time.Duration) *HTTPClient {
	baseURL = strings.TrimRight(baseURL, "/")
	if minInterval <= 0 {
		minInterval = 100 * time.Millisecond
	}
	return &HTTPClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 60 * time.Second},
		limiter: newIntervalLimiter(minInterval),
	}
}

// WithHTTPClient swaps the underlying transport (testing seam).
func (c *HTTPClient) WithHTTPClient(hc *http.Client) *HTTPClient {
	c.http = hc
	return c
}

func (c *HTTPClient) ListContractTransactions(ctx context.Context, contractID, cursor string, limit int, includeFailed bool) (TransactionsResponse, error) {
	if contractID == "" {
		return TransactionsResponse{}, errors.New("horizon: contractID is required")
	}
	if limit <= 0 || limit > 200 {
		limit = 200
	}

	q := urlValues(cursor, limit, includeFailed)
	// Horizon's contract-id-as-account endpoint accepts contract IDs in
	// the path the same way it accepts classic account IDs (G...).
	url := c.baseURL + "/accounts/" + contractID + "/transactions?" + q

	if err := c.limiter.Wait(ctx); err != nil {
		return TransactionsResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return TransactionsResponse{}, fmt.Errorf("building horizon request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return TransactionsResponse{}, fmt.Errorf("calling horizon: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return TransactionsResponse{}, fmt.Errorf("reading horizon response: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		// fall through
	case http.StatusTooManyRequests:
		return TransactionsResponse{}, ErrRateLimited
	case http.StatusNotFound:
		return TransactionsResponse{}, ErrNotFound
	default:
		return TransactionsResponse{}, fmt.Errorf("horizon returned HTTP %d: %s", resp.StatusCode, truncate(body, 200))
	}

	var out TransactionsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return TransactionsResponse{}, fmt.Errorf("decoding horizon response: %w", err)
	}
	return out, nil
}

// urlValues assembles the query string. Kept in a helper to keep
// ListContractTransactions scannable.
func urlValues(cursor string, limit int, includeFailed bool) string {
	var b strings.Builder
	b.WriteString("limit=")
	b.WriteString(itoa(limit))
	b.WriteString("&order=asc")
	if includeFailed {
		b.WriteString("&include_failed=true")
	} else {
		b.WriteString("&include_failed=false")
	}
	if cursor != "" {
		b.WriteString("&cursor=")
		b.WriteString(cursor)
	}
	return b.String()
}

// itoa avoids pulling strconv into a hot path; limit is always small.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// intervalLimiter enforces a minimum spacing between requests. Created
// once per HTTPClient so concurrent backfill goroutines serialize their
// Horizon calls behind a single token bucket.
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
