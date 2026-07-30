package rpc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// RetryConfig controls the retry/backoff behaviour applied to every RPC call
// by RetryClient. Zero values are safe because they have defaults.
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts (including the first
	// attempt). Must be ≥1; values <1 are treated as 1.
	MaxAttempts int
	// BaseBackoff is the initial backoff duration. Each retry doubles it.
	BaseBackoff time.Duration
	// MaxBackoff caps the backoff duration. Each retry doubles BaseBackoff
	// but never exceeds MaxBackoff.
	MaxBackoff time.Duration
	// Jitter, when true, randomises each backoff to [0.5×backoff, 1.5×backoff)
	// so concurrent retries don't thundering-herd the endpoint.
	Jitter bool
}

// RetryClient wraps any Client and retries calls on transient errors with
// exponential backoff. Non-retryable errors (context cancellation, invalid
// arguments) are returned immediately.
type RetryClient struct {
	inner  Client
	config RetryConfig
}

var _ Client = (*RetryClient)(nil)

// NewRetryClient wraps inner with retry/backoff using the supplied config.
// When cfg has zero-valued fields, safe defaults are applied.
func NewRetryClient(inner Client, cfg RetryConfig) *RetryClient {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	return &RetryClient{inner: inner, config: cfg}
}

// doWithRetry runs fn, retrying on transient errors with exponential backoff.
// fn should return a retryable error or a non-retryable error that should
// surface immediately. The context passed to fn carries the overall deadline,
// and backoff sleep respects ctx cancellation.
func (c *RetryClient) doWithRetry(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error
	backoff := c.config.BaseBackoff
	for attempt := 1; attempt <= c.config.MaxAttempts; attempt++ {
		// Propagate context cancellation immediately on every attempt.
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
		if attempt < c.config.MaxAttempts {
			// Exponential backoff with optional jitter.
			d := backoff
			if c.config.Jitter {
				// jitter in [0.5×d, 1.5×d)
				half := d / 2
				d = half + rand.N(d)
			}
			if !sleepCtx(ctx, d) {
				return ctx.Err()
			}
			backoff *= 2
			if backoff > c.config.MaxBackoff {
				backoff = c.config.MaxBackoff
			}
		}
	}
	return fmt.Errorf("exhausted %d retries: %w", c.config.MaxAttempts, lastErr)
}

// isRetryable reports whether err is worth retrying. Context cancellation,
// invalid arguments, and non-retryable RPC errors are not retried; transient
// HTTP-level errors and server-side JSON-RPC errors (code ≥ -32000) are.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation is never retryable.
	if isContextErr(err) {
		return false
	}
	// JSON-RPC error objects: codes ≥ -32000 are server errors.
	var rpcErr *Error
	if rpcErr = rpcErrFromErr(err); rpcErr != nil {
		// -32600 is Invalid Request, -32601 is Method Not Found — not
		// retryable.
		if rpcErr.Code == -32600 || rpcErr.Code == -32601 {
			return false
		}
		// -32000 and above are server errors (e.g. ledger out of range) —
		// treat as retryable.
		if rpcErr.Code <= -32000 {
			return true
		}
		// Other specific codes: 0 means an internal error (Stellar RPC can
		// return code 0 for transient failures).
		return rpcErr.Code == 0
	}
	// HTTP transport errors (connection refused, timeout, EOF) are retryable.
	return isTransientHTTP(err)
}

// rpcErrFromErr unwraps an *Error from the error chain.
func rpcErrFromErr(err error) *Error {
	if err == nil {
		return nil
	}
	type causer interface{ Unwrap() error }
	for e := err; e != nil; {
		if rpcErr, ok := e.(*Error); ok {
			return rpcErr
		}
		u, ok := e.(causer)
		if !ok {
			break
		}
		e = u.Unwrap()
	}
	return nil
}

func isContextErr(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func isTransientHTTP(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, frag := range []string{
		"connection reset",
		"connection refused",
		"EOF",
		"timeout",
		"temporary failure",
		"no such host",
	} {
		if containsStr(s, frag) {
			return true
		}
	}
	return false
}

func containsStr(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// --- Client interface methods ---

func (c *RetryClient) GetEvents(ctx context.Context, req GetEventsRequest) (GetEventsResponse, error) {
	var resp GetEventsResponse
	err := c.doWithRetry(ctx, func(ctx context.Context) error {
		var innerErr error
		resp, innerErr = c.inner.GetEvents(ctx, req)
		return innerErr
	})
	return resp, err
}

func (c *RetryClient) GetLatestLedger(ctx context.Context) (LatestLedger, error) {
	var resp LatestLedger
	err := c.doWithRetry(ctx, func(ctx context.Context) error {
		var innerErr error
		resp, innerErr = c.inner.GetLatestLedger(ctx)
		return innerErr
	})
	return resp, err
}

func (c *RetryClient) GetHealth(ctx context.Context) (Health, error) {
	var resp Health
	err := c.doWithRetry(ctx, func(ctx context.Context) error {
		var innerErr error
		resp, innerErr = c.inner.GetHealth(ctx)
		return innerErr
	})
	return resp, err
}

func (c *RetryClient) GetLedgerEntries(ctx context.Context, req GetLedgerEntriesRequest) (GetLedgerEntriesResponse, error) {
	var resp GetLedgerEntriesResponse
	err := c.doWithRetry(ctx, func(ctx context.Context) error {
		var innerErr error
		resp, innerErr = c.inner.GetLedgerEntries(ctx, req)
		return innerErr
	})
	return resp, err
}
