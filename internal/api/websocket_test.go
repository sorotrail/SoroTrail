package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorotrail/internal/broadcast"
	"github.com/sorotrail/sorotrail/internal/store"
)

// wsTestEvent builds a populated store.Event suitable for transporting
// through the broadcaster and the WS handler. Fields omitted here
// (tx_hash, raw XDR, …) are not part of the filter logic or the JSON
// round-trip the tests care about.
func wsTestEvent(id, contractID string) store.Event {
	return store.Event{
		ID:         id,
		ContractID: contractID,
		Type:       "contract",
		Ledger:     1,
		Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		Value:      json.RawMessage(`"100"`),
		CreatedAt:  time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
	}
}

// wsBigPayloadEvent builds an event large enough that ~300 of them
// exceed any reasonable loopback TCP send buffer (Linux autotunes
// tcp_wmem to a few MiB per connection on lo). Each event carries
// ~10 KiB of artificial payload so the slow-consumer eviction test is
// deterministic across platforms.
func wsBigPayloadEvent(id, contractID string) store.Event {
	pad := strings.Repeat("x", 10*1024)
	return store.Event{
		ID:         id,
		ContractID: contractID,
		Type:       "contract",
		Ledger:     1,
		Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		// JSON string encodes to a large payload by construction; we don't
		// have to construct a valid JSON object that long.
		Value:     json.RawMessage(`"` + pad + `"`),
		CreatedAt: time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
	}
}

// dialWS opens a websocket client connected to the test server.
func dialWS(t *testing.T, srvURL, path string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srvURL, "http") + path
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	return c
}

// readOneEvent pulls a single WS message and decodes it as store.Event.
// It also asserts the message was a text frame — binary frames would mean
// the server is sending something other than JSON.
func readOneEvent(t *testing.T, c *websocket.Conn, timeout time.Duration) store.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	mt, data, err := c.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, mt)
	var ev store.Event
	require.NoError(t, json.Unmarshal(data, &ev))
	return ev
}

// waitForSubs blocks until the broadcaster's subscriber count equals
// `want`. websocket.Accept returns to the client as soon as the 101
// Switching Protocols response is flushed, but the handler calls
// Subscribe() *after* Accept — so publishing before this gate fires
// races against subscription registration and would silently drop the
// event. Calling with want==0 after teardown is the canonical way to
// assert a handler's deferred sub.Close() actually ran.
func waitForSubs(t *testing.T, b *broadcast.Broadcaster, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return b.SubscriberCount() == want },
		5*time.Second, 10*time.Millisecond,
		"broadcaster subscriber count: want %d", want)
}

// newServerWithBroadcaster builds an API server with a broadcaster of
// the requested buffer size.
func newServerWithBroadcaster(bufferSize int) (*Server, *broadcast.Broadcaster) {
	s := newTestServer(&stubStore{}, nil)
	b := broadcast.New(bufferSize)
	s.WithBroadcaster(b)
	return s, b
}

func TestEventStreamWS_DeliversMatchingEvents(t *testing.T) {
	s, b := newServerWithBroadcaster(broadcast.DefaultBufferSize)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	c := dialWS(t, srv.URL, "/events/ws?contract_id="+testContract)
	defer c.Close(websocket.StatusNormalClosure, "")

	waitForSubs(t, b, 1)

	b.Publish(context.Background(), []store.Event{
		wsTestEvent("keep-1", testContract),
		wsTestEvent("drop-1", "COTHER"),
		wsTestEvent("keep-2", testContract),
	})

	first := readOneEvent(t, c, 2*time.Second)
	assert.Equal(t, "keep-1", first.ID)
	second := readOneEvent(t, c, 2*time.Second)
	assert.Equal(t, "keep-2", second.ID)

	_ = c.Close(websocket.StatusNormalClosure, "")
	waitForSubs(t, b, 0)
}

func TestEventStreamWS_FilterByType(t *testing.T) {
	s, b := newServerWithBroadcaster(broadcast.DefaultBufferSize)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	c := dialWS(t, srv.URL, "/events/ws?type=system")
	defer c.Close(websocket.StatusNormalClosure, "")

	waitForSubs(t, b, 1)

	b.Publish(context.Background(), []store.Event{
		wsTestEvent("ctr", testContract), // Type "contract" is filtered out
		{
			ID: "sys", ContractID: testContract, Type: "system", Ledger: 1,
			Topics: json.RawMessage(`[]`), Value: json.RawMessage(`""`),
			CreatedAt: time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
		},
	})

	got := readOneEvent(t, c, 2*time.Second)
	assert.Equal(t, "sys", got.ID)

	_ = c.Close(websocket.StatusNormalClosure, "")
	waitForSubs(t, b, 0)
}

// TestEventStreamWS_FilterByTopic exercises the bare-word topic path:
// "topic=mint" is not valid JSON, so filterFromQuery marshals it as
// the JSON string "mint". The handler's filter then matches any event
// whose topics array contains that JSON string at any position (the
// same containment rule the query API uses). To make the test
// unambiguous the published event's Topics is the array ["mint"]
// rather than [{"symbol":"mint"}] — semantic JSON equality rejects
// strings vs. objects, so we use the shape the filter parses to.
func TestEventStreamWS_FilterByTopic(t *testing.T) {
	s, b := newServerWithBroadcaster(broadcast.DefaultBufferSize)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	c := dialWS(t, srv.URL, "/events/ws?topic=mint")
	defer c.Close(websocket.StatusNormalClosure, "")

	waitForSubs(t, b, 1)

	b.Publish(context.Background(), []store.Event{
		wsTestEvent("transfer-ev", testContract), // [transfer] won't match "mint"
		{
			ID: "mint-ev", ContractID: testContract, Type: "contract", Ledger: 2,
			Topics:    json.RawMessage(`["mint"]`),
			Value:     json.RawMessage(`"0"`),
			CreatedAt: time.Date(2026, 7, 24, 0, 0, 1, 0, time.UTC),
		},
	})

	got := readOneEvent(t, c, 2*time.Second)
	assert.Equal(t, "mint-ev", got.ID)

	_ = c.Close(websocket.StatusNormalClosure, "")
	waitForSubs(t, b, 0)
}

func TestEventStreamWS_NoBroadcasterReturns501(t *testing.T) {
	s := newTestServer(&stubStore{}, nil)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/events/ws"

	_, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.Error(t, err, "expected dial to fail when streaming is not configured")
	require.NotNil(t, resp, "expected the 501 response so callers can branch on it")
	assert.Equal(t, http.StatusNotImplemented, resp.StatusCode)
}

func TestEventStreamWS_BadFilterReturns400(t *testing.T) {
	s, _ := newServerWithBroadcaster(broadcast.DefaultBufferSize)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/events/ws?type=bogus"

	_, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestEventStreamWS_SlowConsumerEviction verifies the WS path of the
// broadcaster's slow-consumer policy. Mechanically: the broadcaster
// evicts on a full subscription channel; the handler's select loop
// then sees sub.Events() close (ok=false), returns, and the deferred
// sub.Close() removes the subscriber from the broadcaster.
//
// To make eviction deterministic across platforms:
//
//  1. The subscription channel buffer is shrunk to one slot.
//  2. Each event is padded to ~10 KiB so a few hundred of them exceed
//     any reasonable loopback TCP send buffer (Linux autotunes
//     tcp_wmem to a few MiB per connection). The handler's WS Write
//     then blocks long before the batch is drained, and a single
//     synchronous Publish call reliably hits the default branch in
//     broadcast.Publish and evicts.
//
// assertSubscriberDropped (subscriber count → 0) is the canonical
// lifecycle signal: the handler's deferred sub.Close() is the only
// path that can drive that count back to zero in this test, so
// reaching it proves the eviction path ran end-to-end.
func TestEventStreamWS_SlowConsumerEviction(t *testing.T) {
	s, b := newServerWithBroadcaster(1) // tiny per-subscriber buffer
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	c := dialWS(t, srv.URL, "/events/ws")
	defer c.Close(websocket.StatusNormalClosure, "")
	waitForSubs(t, b, 1)

	const n = 300
	events := make([]store.Event, n)
	for i := range events {
		events[i] = wsBigPayloadEvent(fmt.Sprintf("e%d", i), testContract)
	}
	b.Publish(context.Background(), events)

	waitForSubs(t, b, 0)
}

// TestEventStreamWS_ClientDisconnectExitsHandler asserts that when a
// client closes its WebSocket, the handler's deferred sub.Close()
// propagates through the broadcaster to drop the subscriber count.
// This is a much stronger signal than "subsequent dials work", because
// goroutine leaks can coexist with a perfectly responsive HTTP server.
func TestEventStreamWS_ClientDisconnectExitsHandler(t *testing.T) {
	s, b := newServerWithBroadcaster(broadcast.DefaultBufferSize)
	srv := httptest.NewServer(s.Router())
	defer srv.Close()

	c := dialWS(t, srv.URL, "/events/ws")
	waitForSubs(t, b, 1)
	require.NoError(t, c.Close(websocket.StatusNormalClosure, "bye"))
	waitForSubs(t, b, 0)
}
