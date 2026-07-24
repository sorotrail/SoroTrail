package rpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonRPCServer decodes incoming JSON-RPC requests and lets a test handler
// produce the result or error per call.
func jsonRPCServer(t *testing.T, handle func(method string, params json.RawMessage) (any, *Error)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.Number     `json:"id"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		require.NoError(t, json.Unmarshal(body, &req))
		assert.Equal(t, "2.0", req.JSONRPC)

		result, rpcErr := handle(req.Method, req.Params)
		out := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if rpcErr != nil {
			out["error"] = rpcErr
		} else {
			out["result"] = result
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(out))
	}))
}

func TestGetEvents_SendsXDRFormatJSON(t *testing.T) {
	var gotParams GetEventsRequest
	srv := jsonRPCServer(t, func(method string, params json.RawMessage) (any, *Error) {
		require.Equal(t, "getEvents", method)
		require.NoError(t, json.Unmarshal(params, &gotParams))
		return GetEventsResponse{LatestLedger: 123}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 100})
	require.NoError(t, err)
	assert.Equal(t, uint32(123), resp.LatestLedger)
	assert.Equal(t, XDRFormatJSON, gotParams.XDRFormat)
	assert.Equal(t, uint32(100), gotParams.StartLedger)
}

func TestGetEvents_CursorClearsLedgerRange(t *testing.T) {
	var gotParams GetEventsRequest
	srv := jsonRPCServer(t, func(_ string, params json.RawMessage) (any, *Error) {
		require.NoError(t, json.Unmarshal(params, &gotParams))
		return GetEventsResponse{}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	_, err := c.GetEvents(context.Background(), GetEventsRequest{
		StartLedger: 100,
		EndLedger:   200,
		Pagination:  &Pagination{Cursor: "abc", Limit: 10},
	})
	require.NoError(t, err)
	assert.Zero(t, gotParams.StartLedger)
	assert.Zero(t, gotParams.EndLedger)
	require.NotNil(t, gotParams.Pagination)
	assert.Equal(t, "abc", gotParams.Pagination.Cursor)
}

func TestGetEvents_FallsBackWhenXDRFormatUnsupported(t *testing.T) {
	calls := 0
	srv := jsonRPCServer(t, func(_ string, params json.RawMessage) (any, *Error) {
		calls++
		var req GetEventsRequest
		require.NoError(t, json.Unmarshal(params, &req))
		if req.XDRFormat != "" {
			return nil, &Error{Code: -32602, Message: `unknown field "xdrFormat"`}
		}
		return GetEventsResponse{LatestLedger: 55}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, uint32(55), resp.LatestLedger)
	assert.Equal(t, 2, calls, "first call rejected, retried without xdrFormat")

	// The rejection is remembered: no second round-trip next time.
	_, err = c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRPCErrorSurfaced(t *testing.T) {
	srv := jsonRPCServer(t, func(string, json.RawMessage) (any, *Error) {
		return nil, &Error{Code: -32600, Message: "startLedger must be within the ledger range: 100 - 200"}
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	_, err := c.GetEvents(context.Background(), GetEventsRequest{StartLedger: 5})
	require.Error(t, err)
	assert.True(t, IsLedgerOutOfRange(err))
}

func TestSimulateTransaction(t *testing.T) {
	srv := jsonRPCServer(t, func(method string, params json.RawMessage) (any, *Error) {
		require.Equal(t, "simulateTransaction", method)
		return SimulateTransactionResponse{
			TransactionData: "AAAAAg==",
			Cost: SimulationCost{
				CPUInstructions: 1000,
				MemoryBytes:     4096,
			},
		}, nil
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	resp, err := c.SimulateTransaction(context.Background(), SimulateTransactionRequest{
		Transaction: "AAAAAg...",
	})
	require.NoError(t, err)
	assert.Equal(t, "AAAAAg==", resp.TransactionData)
	assert.Equal(t, uint64(1000), resp.Cost.CPUInstructions)
}

func TestGetHealthAndLatestLedger(t *testing.T) {
	srv := jsonRPCServer(t, func(method string, _ json.RawMessage) (any, *Error) {
		switch method {
		case "getHealth":
			return Health{Status: "healthy", LatestLedger: 500, OldestLedger: 10}, nil
		case "getLatestLedger":
			return LatestLedger{Sequence: 500}, nil
		}
		return nil, &Error{Code: -32601, Message: "method not found"}
	})
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithMinRequestInterval(0))
	h, err := c.GetHealth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", h.Status)
	assert.Equal(t, uint32(10), h.OldestLedger)

	l, err := c.GetLatestLedger(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(500), l.Sequence)
}
