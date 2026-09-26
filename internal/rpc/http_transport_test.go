package rpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wireEnvelope is one JSON-RPC 2.0 request exactly as HTTPClient puts it on
// the wire. Params stays raw so a test can assert the field is absent rather
// than present-but-null — encoding/json omits an empty interface, and the
// RPC rejects some methods that receive an explicit null params.
type wireEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// recordingServer answers every request with a canned body while keeping
// the requests it saw, so tests can assert what the client sent as well as
// how it parsed what came back.
type recordingServer struct {
	srv       *httptest.Server
	envelopes []wireEnvelope
	headers   []http.Header
	verb      []string
	path      []string
	status    int
	body      string
}

func newRecordingServer(t *testing.T, status int, body string) *recordingServer {
	t.Helper()
	rs := &recordingServer{status: status, body: body}
	rs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
		var env wireEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Errorf("request body is not a JSON-RPC envelope: %v (%s)", err, raw)
		}
		rs.envelopes = append(rs.envelopes, env)
		rs.headers = append(rs.headers, r.Header.Clone())
		rs.verb = append(rs.verb, r.Method)
		rs.path = append(rs.path, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rs.status)
		_, _ = io.WriteString(w, rs.body)
	}))
	t.Cleanup(rs.srv.Close)
	return rs
}

// resultServer serves `result` as the JSON-RPC success payload for any
// method, letting a test focus on how the client parses one response shape.
func resultServer(t *testing.T, result string) *recordingServer {
	return newRecordingServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":`+result+`}`)
}

func (rs *recordingServer) client() *HTTPClient {
	return NewHTTPClient(rs.srv.URL, WithMinRequestInterval(0))
}

func (rs *recordingServer) last(t *testing.T) wireEnvelope {
	t.Helper()
	require.NotEmpty(t, rs.envelopes, "the client sent no request")
	return rs.envelopes[len(rs.envelopes)-1]
}

// TestHTTPClient_RequestConstruction pins the wire format every RPC method
// shares: an HTTP POST carrying a JSON-RPC 2.0 envelope with an
// application/json content type. The id increments per call so a server (or
// a log reader) can correlate a response with its request.
func TestHTTPClient_RequestConstruction(t *testing.T) {
	rs := resultServer(t, `{"status":"healthy","latestLedger":7,"oldestLedger":3,"ledgerRetentionWindow":100}`)
	c := rs.client()

	_, err := c.GetHealth(context.Background())
	require.NoError(t, err)
	_, err = c.GetLatestLedger(context.Background())
	require.NoError(t, err)

	assert.Equal(t, []string{http.MethodPost, http.MethodPost}, rs.verb,
		"JSON-RPC is POST-only; a GET would be rejected by every provider")
	assert.Equal(t, []string{"/", "/"}, rs.path, "the client must post to the endpoint URL itself")

	for i, env := range rs.envelopes {
		assert.Equal(t, "2.0", env.JSONRPC, "request %d", i)
		assert.Equal(t, int64(i+1), env.ID, "request %d must carry a fresh correlation id", i)
	}
	assert.Equal(t, "getHealth", rs.envelopes[0].Method)
	assert.Equal(t, "getLatestLedger", rs.envelopes[1].Method)

	// Methods without parameters must omit params, not send null: the
	// envelope field is `omitempty` on an interface value.
	assert.Empty(t, rs.envelopes[0].Params, "getHealth takes no params and must not send null")
	assert.Empty(t, rs.envelopes[1].Params, "getLatestLedger takes no params and must not send null")

	for i, h := range rs.headers {
		assert.Equal(t, "application/json", h.Get("Content-Type"), "request %d", i)
	}
}

// TestHTTPClient_RequestParamsRoundTrip checks that each method's params
// struct reaches the server with the RPC's own field names, including the
// fields only some methods use.
func TestHTTPClient_RequestParamsRoundTrip(t *testing.T) {
	t.Run("getEvents carries filters and pagination", func(t *testing.T) {
		rs := resultServer(t, `{"events":[],"latestLedger":10,"oldestLedger":1,"cursor":"c"}`)
		_, err := rs.client().GetEvents(context.Background(), GetEventsRequest{
			StartLedger: 4,
			EndLedger:   9,
			Filters:     []EventFilter{{ContractIDs: []string{"CAAA"}, Topics: [][]string{{"AAAA"}}}},
			Pagination:  &Pagination{Limit: 2},
		})
		require.NoError(t, err)

		var got GetEventsRequest
		require.NoError(t, json.Unmarshal(rs.last(t).Params, &got))
		assert.Equal(t, uint32(4), got.StartLedger)
		assert.Equal(t, uint32(9), got.EndLedger)
		require.Len(t, got.Filters, 1)
		assert.Equal(t, []string{"CAAA"}, got.Filters[0].ContractIDs)
		assert.Equal(t, [][]string{{"AAAA"}}, got.Filters[0].Topics)
		require.NotNil(t, got.Pagination)
		assert.Equal(t, uint(2), got.Pagination.Limit)
	})

	t.Run("getLedgerEntries sends the key list", func(t *testing.T) {
		rs := resultServer(t, `{"entries":[],"latestLedger":10}`)
		_, err := rs.client().GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{
			Keys: []string{"AAAA", "BBBB"},
		})
		require.NoError(t, err)

		var got GetLedgerEntriesRequest
		require.NoError(t, json.Unmarshal(rs.last(t).Params, &got))
		assert.Equal(t, []string{"AAAA", "BBBB"}, got.Keys)
	})

	t.Run("getTransactions sends the ledger range", func(t *testing.T) {
		rs := resultServer(t, `{"transactions":[],"latestLedger":10}`)
		_, err := rs.client().GetTransactions(context.Background(), GetTransactionsRequest{
			StartLedger: 100,
			EndLedger:   105,
		})
		require.NoError(t, err)

		var got GetTransactionsRequest
		require.NoError(t, json.Unmarshal(rs.last(t).Params, &got))
		assert.Equal(t, uint32(100), got.StartLedger)
		assert.Equal(t, uint32(105), got.EndLedger)
		assert.Nil(t, got.Pagination, "an unset pagination object must not be sent")
	})

	t.Run("simulateTransaction sends the encoded envelope", func(t *testing.T) {
		rs := resultServer(t, `{"cost":{"cpuInsns":"1","memBytes":"2"}}`)
		_, err := rs.client().SimulateTransaction(context.Background(), SimulateTransactionRequest{
			Transaction: "AAAAAg==",
		})
		require.NoError(t, err)

		var got SimulateTransactionRequest
		require.NoError(t, json.Unmarshal(rs.last(t).Params, &got))
		assert.Equal(t, "AAAAAg==", got.Transaction)
	})
}

// TestHTTPClient_ResponseParsing covers the decode side of every RPC method:
// each result shape must land in the typed response, including the optional
// fields a caller branches on.
func TestHTTPClient_ResponseParsing(t *testing.T) {
	t.Run("getEvents decodes both XDR and JSON topic forms", func(t *testing.T) {
		rs := resultServer(t, `{
			"events":[
				{"id":"0000000007-0000000001","type":"contract","ledger":7,
				 "contractId":"CDDD","txHash":"ABCD","pagingToken":"PT-1",
				 "topic":["AAAABQ=="],"value":"AAAADw==",
				 "topicJson":["transfer"],"valueJson":{"u64":"5"},
				 "inSuccessfulContractCall":true,"operationIndex":2,"transactionIndex":1},
				{"id":"0000000007-0000000002","type":"contract","ledger":7,
				 "contractId":"CEEE","txHash":"ABCE","pagingToken":"",
				 "topic":["AAAABQ=="],"inSuccessfulContractCall":false}
			],
			"latestLedger":7,"oldestLedger":3,"cursor":"TOID-7"
		}`)

		resp, err := rs.client().GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.NoError(t, err)
		assert.Equal(t, uint32(7), resp.LatestLedger)
		assert.Equal(t, uint32(3), resp.OldestLedger)
		assert.Equal(t, "TOID-7", resp.Cursor)

		require.Len(t, resp.Events, 2)
		first := resp.Events[0]
		assert.Equal(t, "PT-1", first.PagingToken)
		assert.Equal(t, []string{"AAAABQ=="}, first.Topic)
		assert.Equal(t, "AAAADw==", first.Value)
		assert.Equal(t, []json.RawMessage{json.RawMessage(`"transfer"`)}, first.TopicJSON)
		assert.JSONEq(t, `{"u64":"5"}`, string(first.ValueJSON))
		assert.True(t, first.InSuccessfulContractCall)
		assert.Equal(t, int32(2), first.OperationIndex)

		// The second event models a pre-protocol-22 server omitting the
		// per-event cursor, which is what CursorValue falls back for.
		assert.Empty(t, resp.Events[1].PagingToken)
		assert.Nil(t, resp.Events[1].TopicJSON)
	})

	t.Run("getLatestLedger", func(t *testing.T) {
		rs := resultServer(t, `{"id":"abc","protocolVersion":22,"sequence":1234}`)
		got, err := rs.client().GetLatestLedger(context.Background())
		require.NoError(t, err)
		assert.Equal(t, LatestLedger{ID: "abc", ProtocolVersion: 22, Sequence: 1234}, got)
	})

	t.Run("getLedgerEntries decodes entries and TTL", func(t *testing.T) {
		rs := resultServer(t, `{"entries":[
			{"key":"AAAA","xdr":"BBBB","lastModifiedLedgerSeq":9},
			{"key":"AAAA2","xdr":"BBBB2","lastModifiedLedgerSeq":10,"liveUntilLedgerSeq":500}
		],"latestLedger":10}`)

		got, err := rs.client().GetLedgerEntries(context.Background(), GetLedgerEntriesRequest{Keys: []string{"AAAA"}})
		require.NoError(t, err)
		require.Len(t, got.Entries, 2)
		assert.Equal(t, LedgerEntryResult{Key: "AAAA", XDR: "BBBB", LastModifiedLedgerSeq: 9}, got.Entries[0])
		assert.Nil(t, got.Entries[0].LiveUntilLedgerSeq, "an entry without a TTL must stay nil, not zero")
		require.NotNil(t, got.Entries[1].LiveUntilLedgerSeq)
		assert.Equal(t, uint32(500), *got.Entries[1].LiveUntilLedgerSeq)
	})

	t.Run("getTransactions decodes fee-bump context", func(t *testing.T) {
		rs := resultServer(t, `{"transactions":[
			{"txHash":"AA","sourceAccount":"GSRC","feeSource":"GFEE","fee":"100",
			 "memo":{"type":"text","value":"hi"},"ledger":42,"createdAt":"2026-01-01T00:00:00Z","feeBump":true},
			{"txHash":"AB","sourceAccount":"GOTHER","fee":"50","ledger":43,"createdAt":"2026-01-01T00:00:05Z"}
		],"latestLedger":43}`)

		got, err := rs.client().GetTransactions(context.Background(), GetTransactionsRequest{StartLedger: 42, EndLedger: 44})
		require.NoError(t, err)
		assert.Equal(t, uint32(43), got.LatestLedger)
		require.Len(t, got.Transactions, 2)

		bumped := got.Transactions[0]
		assert.Equal(t, "GSRC", bumped.SourceAccount, "sourceAccount is the semantic actor of a fee bump")
		assert.Equal(t, "GFEE", bumped.FeeSource)
		assert.True(t, bumped.FeeBump)
		require.NotNil(t, bumped.Memo)
		assert.Equal(t, Memo{Type: "text", Value: "hi"}, *bumped.Memo)

		assert.Nil(t, got.Transactions[1].Memo, "a transaction without a memo has no memo object")
		assert.Empty(t, got.Transactions[1].FeeSource)
		assert.False(t, got.Transactions[1].FeeBump)
	})

	t.Run("simulateTransaction decodes costs and error text", func(t *testing.T) {
		rs := resultServer(t, `{"transactionData":"AAAA","error":"HostError",
			"cost":{"cpuInsns":"1234567","memBytes":"8912"},"events":[{"id":"1","ledger":3}],
			"results":["AAAADw=="]}`)

		got, err := rs.client().SimulateTransaction(context.Background(), SimulateTransactionRequest{Transaction: "AAAA"})
		require.NoError(t, err)
		assert.Equal(t, "AAAA", got.TransactionData)
		assert.Equal(t, "HostError", got.Error)
		assert.Equal(t, uint64(1234567), got.Cost.CPUInstructions)
		assert.Equal(t, uint64(8912), got.Cost.MemoryBytes)
		require.Len(t, got.Events, 1)
		assert.Equal(t, []json.RawMessage{json.RawMessage(`"AAAADw=="`)}, got.Results)
	})
}

// TestHTTPClient_ErrorEnvelopes pins how each failure shape reaches the
// caller. The distinction matters to the layers above: a JSON-RPC *Error
// survives errors.As so IsLedgerOutOfRange can classify it, an HTTP 429
// becomes a RateLimitedError carrying the provider hint, and anything else
// at the transport level is a plain wrapped error.
func TestHTTPClient_ErrorEnvelopes(t *testing.T) {
	t.Run("json-rpc error object", func(t *testing.T) {
		rs := newRecordingServer(t, http.StatusOK,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"ledger range invalid","data":"must be within 100 - 200"}}`)

		_, err := rs.client().GetEvents(context.Background(), GetEventsRequest{StartLedger: 5})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "getEvents: rpc error -32001: ledger range invalid",
			"the method name and the RPC error must both reach the caller")

		var rpcErr *Error
		require.ErrorAs(t, err, &rpcErr, "call() must keep the server error reachable through wrapping")
		assert.Equal(t, -32001, rpcErr.Code)
		assert.Equal(t, "must be within 100 - 200", rpcErr.Data)
		assert.True(t, IsLedgerOutOfRange(err), "a retention-range error must be classifiable")
	})

	t.Run("http status other than 200 or 429", func(t *testing.T) {
		rs := newRecordingServer(t, http.StatusInternalServerError, "upstream bootstrap failure")
		_, err := rs.client().GetHealth(context.Background())
		require.Error(t, err)
		assert.Equal(t, "getHealth returned HTTP 500: upstream bootstrap failure", err.Error())
	})

	t.Run("error bodies are truncated", func(t *testing.T) {
		rs := newRecordingServer(t, http.StatusBadGateway, strings.Repeat("x", 400))
		_, err := rs.client().GetHealth(context.Background())
		require.Error(t, err)

		msg := err.Error()
		assert.Contains(t, msg, "...", "a truncated body is marked as truncated")
		assert.Less(t, strings.Count(msg, "x"), 400, "the whole body must not be echoed into the error")
		assert.Contains(t, msg, strings.Repeat("x", 200))
		assert.NotContains(t, msg, strings.Repeat("x", 201))
	})

	t.Run("malformed json", func(t *testing.T) {
		rs := newRecordingServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":`)
		_, err := rs.client().GetLatestLedger(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decoding getLatestLedger response")
	})

	t.Run("result that does not match the method", func(t *testing.T) {
		rs := resultServer(t, `{"latestLedger":"not a number"}`)
		_, err := rs.client().GetHealth(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decoding getHealth result")
	})

	t.Run("envelope with neither result nor error", func(t *testing.T) {
		rs := newRecordingServer(t, http.StatusOK, `{"jsonrpc":"2.0","id":1}`)
		_, err := rs.client().GetEvents(context.Background(), GetEventsRequest{StartLedger: 1})
		require.Error(t, err,
			"a response with no result must not decode into an empty page — ingestion would read it as 'no events' and advance")
		assert.Contains(t, err.Error(), "decoding getEvents result")
	})

	t.Run("transport failure", func(t *testing.T) {
		dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		url := dead.URL
		dead.Close()

		_, err := NewHTTPClient(url, WithMinRequestInterval(0)).GetHealth(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "calling getHealth",
			"a transport failure must name the method that failed")
		assert.True(t, isNetworkError(err) || isTransientHTTP(err),
			"a refused connection must be recognised as transient, got %v", err)
	})
}

// TestHTTPClientOptions covers the option functions that replace the
// transport itself: an injected http.Client, a timeout override, and the
// observer hook.
func TestHTTPClientOptions(t *testing.T) {
	t.Run("WithHTTPClient injects the transport", func(t *testing.T) {
		rs := resultServer(t, `{"status":"healthy"}`)
		injected := &http.Client{Timeout: 5 * time.Minute}
		c := NewHTTPClient(rs.srv.URL, WithMinRequestInterval(0), WithHTTPClient(injected))

		require.Same(t, injected, c.httpClient)
		_, err := c.GetHealth(context.Background())
		require.NoError(t, err, "the injected transport must be the one used")
	})

	t.Run("WithHTTPTimeout sets and skips", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, NewHTTPClient("http://localhost").httpClient.Timeout,
			"the documented default HTTP timeout")

		c := NewHTTPClient("http://localhost", WithHTTPTimeout(3*time.Second))
		assert.Equal(t, 3*time.Second, c.httpClient.Timeout)

		zero := NewHTTPClient("http://localhost", WithHTTPTimeout(0))
		assert.Equal(t, 30*time.Second, zero.httpClient.Timeout,
			"a non-positive timeout keeps the default instead of disabling it")
	})

	t.Run("WithRequestObserver stores the observer", func(t *testing.T) {
		obs := &countingObserver{}
		rs := resultServer(t, `{"status":"healthy"}`)
		c := NewHTTPClient(rs.srv.URL, WithMinRequestInterval(0), WithRequestObserver(obs))
		require.NotNil(t, c.requestObserver)

		_, err := c.GetHealth(context.Background())
		require.NoError(t, err)
		// Known gap, pinned deliberately: call() reports latency through the
		// Prometheus metrics but never invokes requestObserver, so an
		// observer configured this way sees no callbacks. Fixing it changes
		// production behaviour, which this issue scopes to a separate PR.
		assert.Zero(t, obs.calls,
			"requestObserver is currently dead weight; assert.Equal(t, 1, obs.calls) once the hook is wired up")
	})
}

type countingObserver struct {
	calls int
}

func (o *countingObserver) ObserveRPCRequest(string, error) { o.calls++ }

// TestRPCErrorsCarryTheirContext covers the error types the transport hands
// to the retry layer: their message text is what is matched by string-based
// classification in retry.go and failover.go, so the exact wording is part
// of the contract even though it reads as cosmetic.
func TestRPCErrorsCarryTheirContext(t *testing.T) {
	t.Run("rpc error rendering", func(t *testing.T) {
		tests := []struct {
			name string
			err  *Error
			want string
		}{
			{
				name: "code and message",
				err:  &Error{Code: -32601, Message: "Method not found"},
				want: "rpc error -32601: Method not found",
			},
			{
				name: "data is kept out of the message but stays on the struct",
				err:  &Error{Code: -32000, Message: "read Failed", Data: "ledger 5 out of range"},
				want: "rpc error -32000: read Failed",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.err.Error())
			})
		}
	})

	t.Run("rate limited error rendering", func(t *testing.T) {
		tests := []struct {
			name       string
			err        *RateLimitedError
			want       string
			wantPrefix string
		}{
			{
				name:       "with a provider hint",
				err:        &RateLimitedError{StatusCode: 429, RetryAfter: 30 * time.Second, Body: "slow down"},
				want:       "RPC endpoint returned HTTP 429 (rate limited, retry-after 30s): slow down",
				wantPrefix: "retry-after",
			},
			{
				name:       "without a hint",
				err:        &RateLimitedError{StatusCode: 429, Body: "slow down"},
				want:       "RPC endpoint returned HTTP 429 (rate limited): slow down",
				wantPrefix: "(rate limited)",
			},
			{
				name:       "with no body",
				err:        &RateLimitedError{StatusCode: 429},
				want:       "RPC endpoint returned HTTP 429 (rate limited): ",
				wantPrefix: "HTTP 429",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.Equal(t, tt.want, tt.err.Error())
				assert.Contains(t, tt.err.Error(), tt.wantPrefix)
			})
		}
	})
}

// TestFailoverClient_LabelsAndDiagnostics keeps the credential-stripping
// rule honest: a URL with basic auth must never reach a metric label, while
// the dial target and the operator diagnostics view still carry it.
func TestFailoverClient_LabelsAndDiagnostics(t *testing.T) {
	fc, _ := newFailoverTestClient(
		[]string{"https://user:secret@rpc-a.example:443/srpc?token=abc", "https://rpc-b.example"},
		WithFailoverLogger(testLogger()),
	)

	require.Len(t, fc.providers, 2)
	assert.Equal(t, "rpc-a.example", fc.providers[0].label)
	assert.Equal(t, "rpc-b.example", fc.providers[1].label)
	assert.Equal(t, "https://user:secret@rpc-a.example:443/srpc?token=abc", fc.providers[0].url,
		"the client still needs the credential-bearing URL to dial; only metric labels are sanitised")

	// ProviderStates is the operator-facing diagnostics view. It echoes the
	// configured URL (which may carry basic-auth credentials) rather than the
	// sanitised label, so it must never be wired into an HTTP response or a
	// log line without a deliberate decision. Pinned here because metrics
	// already use the label and a future caller would not see a difference.
	states := fc.ProviderStates()
	require.Len(t, states, 2)
	assert.Equal(t, fc.providers[0].url, states[0].URL)
	assert.Equal(t, StateActive, states[0].State, "providers start active")
	assert.Equal(t, StateActive, states[1].State)

	fc.setProviderState(fc.providers[1], StateDown)
	assert.Equal(t, StateDown, fc.ProviderStates()[1].State,
		"the snapshot must reflect live health, not construction-time state")
}
