# Live Subscriptions

SoroTrail provides real-time event streaming for connected clients. When new
contract events are ingested from the Stellar RPC, they can be streamed
immediately to consumers over a persistent WebSocket connection rather than
waiting for periodic polling cycles.

This document describes how to connect to the live subscription stream, the
filtering syntax, the delivery guarantees, backpressure handling, and how to
combine streaming with historical catch-up queries.

- [Connecting to the stream](#connecting-to-the-stream)
- [Available filters](#available-filters)
- [Delivery guarantee: at-most-once](#delivery-guarantee-at-most-once)
- [The catch-up pattern](#the-catch-up-pattern)
- [Reconnection guidance](#reconnection-guidance)
- [Backpressure and slow consumers](#backpressure-and-slow-consumers)
- [Authentication and multi-tenancy](#authentication-and-multi-tenancy)
- [Worked examples](#worked-examples)

## Connecting to the stream

The live subscription endpoint is served over WebSocket:

```http
GET /events/ws
```

A client opens a standard HTTP/1.1 connection with `Upgrade: websocket` headers.
The server accepts the connection and returns HTTP `101 Switching Protocols`.

From that point forward, the server pushes matching events as JSON text frames
down the WebSocket connection as soon as they are ingested by the worker pipeline.

The wire format of each delivered message is the JSON representation of an event:

```json
{
  "id": "0000000100-00001-00000",
  "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
  "ledger": 100,
  "type": "contract",
  "tx_hash": "a1b2c3d4...",
  "topics": ["transfer", "GA...", "GB..."],
  "value": "1000000",
  "created_at": "2026-07-24T12:00:00Z"
}
```

The payload mirrors the shape returned by `GET /events/{id}`.

## Available filters

Subscribers specify their event criteria as query parameters during the
WebSocket handshake. The server parses these parameters using the same filtering
engine as the REST API (`GET /events`):

| Parameter | Type | Description |
| --- | --- | --- |
| `contract_id` | string | Only receive events emitted by this Soroban contract address. |
| `type` | string | Filter by event type: `contract`, `system`, or `diagnostic`. |
| `topic` | string | Match any event whose JSON `topics` array contains this element. |
| `from_ledger` | integer | Lower ledger sequence bound (inclusive). |
| `to_ledger` | integer | Upper ledger sequence bound (inclusive). |
| `from_time` | string | Earliest event timestamp (RFC 3339). |
| `to_time` | string | Latest event timestamp (RFC 3339). |

For example, to listen only to transfer events from a specific token contract:

```
/events/ws?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&type=contract&topic=transfer
```

### Filter agreement with the SQL query path

The streaming filter logic evaluates incoming events in memory before they are
dispatched to subscribers. The evaluation rules are designed to agree with SQL
filtering on the storage backend:

- Topic filtering matches if the requested topic is contained within the event's
  topics array. Whitespace is normalized before comparison.
- Multiple constraints are combined conjunctively (`AND`).
- Type filtering matches any of the supplied comma-delimited types.

The in-process broker (`internal/api/broker.go`) applies every field of the
filter language the REST query path does — `network`, `contract_ids`,
`contract_id_prefix`, `tx_hash`, `topic0`–`topic3`, `topic_contains`,
`in_successful_call`, `tx_index`, `op_index` and `has_value` as well as the
parameters in the table above. That agreement is asserted, not merely intended:
one shared table of filter cases is run against the matcher and against a real
Postgres `GET /events` query, and the two must select exactly the same events
(`TestEventMatches_ParityWithSQLQueryPath`, integration-tagged).

The broadcaster used by the live `/events/ws` upgrade
(`internal/broadcast/broadcast.go`) currently consults a subset of those fields,
so a handshake that sets one of the others narrows the REST query without
narrowing the stream. Fixing it means the same table applied to that matcher,
which is not yet in place.

If an invalid filter parameter is supplied (such as an unknown event type or an
unparseable timestamp), the server rejects the WebSocket upgrade immediately
with HTTP `400 Bad Request`.

## Delivery guarantee: at-most-once

The live WebSocket subscription provides **best-effort, at-most-once delivery**.

The broker is an in-memory pub-sub dispatcher (`internal/api/broker.go` and
`internal/broadcast/`). It does not persist an event backlog or track per-client
offsets on disk. This design yields minimal latency for live feeds, but introduces
a critical operational property:

> **Events published while a client is disconnected are not queued.** They will
> never be delivered over the WebSocket stream once the client reconnects.

Any consumer that requires zero data loss (such as financial indexers, audit
ledgers, or accounting services) must implement the catch-up pattern.

## The catch-up pattern

Because disconnects can happen due to network blips, server restarts, or
consumer restarts, reliable consumers combine the live WebSocket stream with
paged REST reads:

```
 1. Connect to WebSocket (/events/ws)
 2. Buffer incoming live events in memory
 3. Query historical REST API (GET /events?from_ledger=...)
 4. Deduplicate historical results against buffered live events
 5. Drain buffer and transition to live processing
```

### Step-by-step catch-up procedure

1. **Connect and buffer**: Establish the WebSocket connection to `/events/ws`.
   As events arrive from the socket, push them into an in-memory buffer or queue
   without processing them yet. Note the earliest ledger number seen in the live
   stream ($L_{first}$).
2. **Query the gap**: Determine the last ledger sequence your system durably
   processed ($L_{last}$). Query the REST API:
   ```http
   GET /events?contract_id=<id>&from_ledger=<L_last>&limit=100
   ```
   Follow pagination (`Link` headers with `cursor`) until all historical events
   up to $L_{first}$ have been fetched and processed.
3. **Deduplicate**: Drain the in-memory queue of buffered live events. Skip any
   event whose `id` has already been recorded during the historical catch-up.
4. **Transition**: Process remaining events in the buffer and continue
   processing incoming WebSocket messages in real time.

This ensures no event is missed across disconnects or process restarts.

## Reconnection guidance

1. **Heartbeats and pings**: SoroTrail sends periodic WebSocket ping frames
   every 30 seconds. Clients should respond with pong frames (handled
   automatically by standard WebSocket libraries). If no pong is received within
   5 seconds, the server terminates the dead connection.
2. **Handling disconnects**: If the connection drops or an error occurs:
   - Close the existing socket cleanly.
   - Record the last successfully processed event `id` and ledger sequence.
   - Reconnect with exponential backoff (e.g. 500ms, 1s, 2s, up to 30s) to avoid
     thundering-herd issues on server restarts.
   - Run the [catch-up pattern](#the-catch-up-pattern) to backfill missed events.

Clients cannot resume from a position inside the WebSocket connection itself;
the position is maintained via your application's persistent state and the
REST query cursor.

### Cursor persistence across upgrades and restarts

If your consumer persists a REST pagination `cursor` across restarts, note the guarantees defined in the [Pagination Cursor Stability Contract](reference/cursors.md):

- Cursors survive SoroTrail process restarts, rolling upgrades across patch/minor versions (`v1` and `v2` envelopes), concurrent ingestion, and retention pruning against the same storage backend.
- Cursors are invalidated (returning HTTP `400 Bad Request` with `code: "INVALID_CURSOR"`) if the server's storage backend changes (`pg`, `sq`, `ch`), if the query's `order_by` parameter changes, or if the cursor is truncated/tampered with.
- Always persist the last processed `ledger` sequence (`lastProcessedLedger`) alongside any saved `cursor`. If a catch-up request fails with `400 Bad Request` (`INVALID_CURSOR`, detectable in the Go client via `client.IsStaleCursor(err)`), discard the saved cursor and restart the catch-up query from `from_ledger=<lastProcessedLedger>`.

## Backpressure and slow consumers

To prevent a slow consumer from blocking the ingestion pipeline or exhausting
server memory, SoroTrail enforces strict subscriber buffering:

- Each subscriber has a bounded in-memory channel buffer (default 64 events in
  `internal/broadcast/`, 256 in `internal/api/broker.go`).
- Event dispatches from the ingester to subscribers are non-blocking.
- **Eviction policy**: If a client's buffer fills up because it is consuming
  slower than events are arriving, the server drops the subscriber and closes
  the WebSocket connection immediately.

```
Ingester ──▶ Broadcaster ──[ buffer full ]──▶ Evict subscriber & Close WebSocket
```

### Writing fast consumers

To avoid eviction:
- Do not perform expensive tasks (such as database writes, synchronous external
  RPC calls, or file encoding) directly inside the message-reading loop.
- Read messages off the socket immediately into a local consumer queue or worker
  pool.

## Authentication and multi-tenancy

When authentication or multi-tenancy is active (`API_KEY_AUTH_ENABLED=true` or
`MULTI_TENANT=true`), WebSocket connections require authentication.

### Presenting credentials

Credentials may be passed during the WebSocket upgrade request in either of
two ways:

```http
Authorization: Bearer <key>
```
or
```http
X-API-Key: <key>
```

If the key is missing or invalid, the upgrade request is rejected with
HTTP `401 Unauthorized`.

### Multi-tenant scope enforcement

Under multi-tenancy:
- Non-wildcard tenants may only subscribe to contracts they are explicitly
  granted. Requesting an ungranted contract returns `403 Forbidden`.
- **Dynamic scope synchronization**: SoroTrail continuously synchronizes the
  tenant's authorization boundary with the database during the lifetime of the
  connection. If a contract is granted mid-stream, its events begin flowing
  without reconnecting. If a grant is revoked, events for that contract cease
  flowing immediately on the next sync interval.
- Total connection duration is metered and attributed to the tenant's streaming
  quota.

For full details on tenant isolation and quotas, see [Multi-tenancy](multi-tenancy.md).

## Worked examples

### Using `wscat`

To test the stream from the command line:

```sh
# Connect anonymously on an unauthenticated deployment
wscat -c "ws://localhost:8080/events/ws?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&type=contract"

# Connect with an API key
wscat -c "ws://localhost:8080/events/ws?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC" \
  -H "Authorization: Bearer sorotrail_0123456789abcdef_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```

### End-to-end client sketch (JavaScript / Node.js)

```javascript
import WebSocket from 'ws';

const BASE_URL = 'http://localhost:8080';
const WS_URL = 'ws://localhost:8080/events/ws';
const CONTRACT_ID = 'CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC';
const API_KEY = process.env.SOROTRAIL_API_KEY;

let lastProcessedLedger = 0;
const processedEventIds = new Set();

async function catchUpHistorical(fromLedger) {
  let url = `${BASE_URL}/events?contract_id=${CONTRACT_ID}&from_ledger=${fromLedger}&limit=100`;
  while (url) {
    const res = await fetch(url, {
      headers: API_KEY ? { 'Authorization': `Bearer ${API_KEY}` } : {}
    });
    if (!res.ok) throw new Error(`HTTP catchup failed: ${res.status}`);
    const data = await res.json();
    for (const event of data.events || []) {
      handleEvent(event);
    }
    // Parse Link header for rel="next"
    const linkHeader = res.headers.get('link');
    const match = linkHeader && linkHeader.match(/<([^>]+)>;\s*rel="next"/);
    url = match ? match[1] : null;
  }
}

function handleEvent(event) {
  if (processedEventIds.has(event.id)) {
    return; // Ignore duplicates
  }
  processedEventIds.add(event.id);
  lastProcessedLedger = Math.max(lastProcessedLedger, event.ledger);

  console.log(`[Event ${event.id}] Ledger ${event.ledger}: type=${event.type}`);
}

function connect() {
  const ws = new WebSocket(`${WS_URL}?contract_id=${CONTRACT_ID}`, {
    headers: API_KEY ? { 'Authorization': `Bearer ${API_KEY}` } : {}
  });

  const liveBuffer = [];
  let isCatchingUp = true;

  ws.on('open', async () => {
    console.log('Connected to SoroTrail event stream');
    try {
      await catchUpHistorical(lastProcessedLedger);
      // Flush buffered live events
      for (const ev of liveBuffer) {
        handleEvent(ev);
      }
      liveBuffer.length = 0;
      isCatchingUp = false;
    } catch (err) {
      console.error('Error during historical catch-up:', err);
      ws.close();
    }
  });

  ws.on('message', (data) => {
    const event = JSON.parse(data.toString());
    if (isCatchingUp) {
      liveBuffer.push(event);
    } else {
      handleEvent(event);
    }
  });

  ws.on('close', () => {
    console.warn('Stream disconnected. Reconnecting in 2 seconds...');
    setTimeout(connect, 2000);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
  });
}

connect();
```
