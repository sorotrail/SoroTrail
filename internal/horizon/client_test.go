package horizon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPClient_Success: a 200 response with `_embedded.records`
// parses into a struct that hands back the same records.
func TestHTTPClient_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request shape.
		assert.Equal(t, "/accounts/CABC/transactions", r.URL.Path)
		assert.Equal(t, "asc", r.URL.Query().Get("order"))
		assert.Equal(t, "false", r.URL.Query().Get("include_failed"))
		assert.Equal(t, "50", r.URL.Query().Get("limit"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"_embedded": map[string]any{
				"records": []Transaction{
					{ID: "r1", Hash: "h1", Ledger: 100, ResultMetaXDR: "", ResultCode: "txSuccess"},
				},
			},
		})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	resp, err := c.ListContractTransactions(context.Background(), "CABC", "", 50, false)
	require.NoError(t, err)
	require.Len(t, resp.Embedded.Records, 1)
	assert.Equal(t, "h1", resp.Embedded.Records[0].Hash)
}

// TestHTTPClient_RateLimited: 429 surfaces as ErrRateLimited so
// cmd/sorotrail/backfill.go's retry loop can back off.
func TestHTTPClient_RateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	_, err := c.ListContractTransactions(context.Background(), "CABC", "", 200, false)
	assert.ErrorIs(t, err, ErrRateLimited)
}

// TestHTTPClient_NotFound: 404 surfaces as ErrNotFound; we use a
// non-existent contract on a real (non-existent) account.
func TestHTTPClient_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"status":404,"title":"Resource Missing"}`)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	_, err := c.ListContractTransactions(context.Background(), "CABC", "", 200, false)
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestHTTPClient_HTTPError: any other status wraps the body so the
// caller has a diagnostic.
func TestHTTPClient_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "kaboom")
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	_, err := c.ListContractTransactions(context.Background(), "CABC", "", 200, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kaboom")
	assert.Contains(t, err.Error(), "500")
}

// TestHTTPClient_CursorIsForwarded: paging_token goes through the URL
// query verbatim so a backfill page boundary resumes mid-cursor.
func TestHTTPClient_CursorIsForwarded(t *testing.T) {
	var captured url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.URL.Query()
		_, _ = io.WriteString(w, `{"_embedded":{"records":[]}}`)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	_, err := c.ListContractTransactions(context.Background(), "CABC", "abc123", 200, false)
	require.NoError(t, err)
	assert.Equal(t, "abc123", captured.Get("cursor"))
}

// TestHTTPClient_LimitClampedAbove200: limit > 200 falls back to 200
// to match Horizon's cap without producing a query Horizon rejects.
func TestHTTPClient_LimitClampedAbove200(t *testing.T) {
	var captured url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.URL.Query()
		_, _ = io.WriteString(w, `{"_embedded":{"records":[]}}`)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 0)
	_, err := c.ListContractTransactions(context.Background(), "CABC", "", 999, false)
	require.NoError(t, err)
	assert.Equal(t, "200", captured.Get("limit"))
}

// TestHTTPClient_MinIntervalEnforced: with a 50ms minimum spacing two
// sequential calls take ≥50ms more than a single one.
func TestHTTPClient_MinIntervalEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond instantly; the limiter, not the server, throttles.
		_, _ = io.WriteString(w, `{"_embedded":{"records":[]}}`)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 50*time.Millisecond)
	ctx := context.Background()
	start := time.Now()
	_, err := c.ListContractTransactions(ctx, "CABC", "", 200, false)
	require.NoError(t, err)
	_, err = c.ListContractTransactions(ctx, "CABC", "", 200, false)
	require.NoError(t, err)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond)
}
