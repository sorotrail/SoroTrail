# SoroTrail

A contract event indexer for the Stellar/Soroban network.

Stellar RPC's `getEvents` method only retains contract events for roughly 24
hours to 7 days. Anyone who needs historical Soroban event data — dapp
dashboards, analytics, audits, notification services — must ingest and store
events themselves before the RPC drops them.

SoroTrail does exactly that: it polls a Stellar RPC endpoint, stores contract
events durably in Postgres, and serves them back through a queryable HTTP API
long after the RPC has forgotten them.

```
 Stellar RPC ──getEvents──▶ ingester ──▶ Postgres ◀── HTTP API ◀── you
```

## Quickstart

### Docker (one command)

```sh
docker compose up --build
```

This starts Postgres and the indexer against the public Stellar testnet RPC.
The API is on http://localhost:8080; watch the logs to see events flow in.

To watch specific contracts instead of everything:

```sh
WATCHED_CONTRACTS=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC docker compose up --build
```

### Bare metal

```sh
docker compose up -d postgres     # or bring your own Postgres
cp .env.example .env              # adjust as needed
set -a; source .env; set +a
make run
```

Migrations run automatically on startup.

## Configuration

All configuration comes from environment variables (see `.env.example`):

| Variable | Default | Description |
| --- | --- | --- |
| `RPC_URL` | `https://soroban-testnet.stellar.org` | Stellar RPC endpoint (JSON-RPC 2.0). Point at a provider URL for mainnet. |
| `DATABASE_URL` | — (required) | Postgres connection string. |
| `POLL_INTERVAL` | `5s` | Sleep between polls once caught up. |
| `HTTP_ADDR` | `:8080` | API listen address. |
| `WATCHED_CONTRACTS` | empty | Comma-separated contract IDs (`C...`). Empty = ingest **all** contract events. |
| `START_LEDGER` | unset | Force cold-start ingestion from this ledger. |
| `RETENTION_LEDGERS` | `17280` | Cold-start reach-back in ledgers (~24h at 5s/ledger). |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`. |
| `AUDIT_ENABLED` | `false` | Enable the background auditor. When unset/false the binary behaves exactly like the pre-audit build. |
| `AUDIT_POLL_INTERVAL` | `30s` | Sleep between audit passes. |
| `AUDIT_BATCH_LEDGERS` | `100` | Ledger range covered by one audit pass. |
| `AUDIT_LAG_THRESHOLD` | `200` | Auditor sleeps until ingest is at least this many ledgers past the verified mark. |
| `AUDIT_BUDGET_SHARE` | `0.10` | Fraction of the request budget the audit pool gets (rest goes to ingest). |
| `AUDIT_MAX_RPS` | `10` | Total request budget (split between ingest and audit). |
| `AUDIT_MAX_REPAIR_ATTEMPTS` | `3` | Repair iterations before a finding is kept open as `unrecoverable`. |
| `AUDIT_FINDING_MAX_LEDGERS` | `100` | Largest range a single finding is allowed to span. |
| `RATE_LIMIT_RPS` | unset | Per-client HTTP request rate limit (`requests/second`). Both `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST` must be set together; otherwise no rate limiting is applied. |
| `RATE_LIMIT_BURST` | unset | Maximum instantaneous burst size for the rate limiter. Pairs with `RATE_LIMIT_RPS`. |
| `RATE_LIMIT_TRUSTED_PROXY` | `false` | Honor `X-Forwarded-For` for client IP detection. Must only be enabled behind a proxy you trust to strip/rewrite the header — clients control `X-Forwarded-For` themselves, so enabling it on an Internet-facing surface lets any caller pick their own rate-limit key. |
| `CACHE_PRIVATE` | `false` | Flip cacheable responses from `Cache-Control: public` to `private`. Set this when the deployment serves per-user data behind an auth layer (see [Caching](#caching)). |

## Ingestion behavior

- **Cold start** (empty database): begins at `latest ledger − RETENTION_LEDGERS`
  (clamped to what the RPC still retains) so it captures as much recent history
  as possible, then follows the chain head. `START_LEDGER` overrides this.
- **Warm start**: resumes from the persisted cursor / last ingested ledger.
- Events are upserted idempotently by ID, so re-scans and restarts never
  duplicate rows.
- If the indexer is down long enough that its resume point falls out of the
  RPC's retention window, it logs a warning and skips ahead to the oldest
  retained ledger (the gap is unrecoverable from RPC — that's the problem this
  project exists to prevent).
- Requests are rate-limited (~10/s, matching public endpoint limits) and
  errors are retried with jittered exponential backoff.
- Topics/values are stored as JSON. When the RPC supports `xdrFormat: "json"`
  its decoding is used verbatim; otherwise the base64 XDR is decoded locally
  into shapes like `{"symbol":"transfer"}`, `{"u64":42}`, `{"i128":"-1000"}`,
  `{"address":"C..."}`.
- The raw base64 XDR is stored alongside the decoded JSON, so an improved
  decoder can be applied to already-indexed events — see
  [decoder replay](#decoder-replay).

## Decoder replay

Decoders improve over time. `sorotrail replay` re-runs the current decoder
over stored raw XDR and rewrites the decoded columns, so improvements apply
to everything already indexed instead of only to future events.

```sh
sorotrail replay --from-ledger 250000 --dry-run   # see what would change
sorotrail replay --from-ledger 250000             # rewrite it
```

It is batched, resumable (Ctrl-C and re-run picks up where it stopped),
idempotent, and safe to run against a live database while ingestion
continues; a Postgres advisory lock prevents two replays at once.

See [docs/replay.md](docs/replay.md) for flags, the summary output, the
advisory-lock strategy, and the derivation order for dependent tables.

## API reference

All responses are JSON. Errors look like `{"error": "message"}`.

### `GET /health`

Reports the API's view of its dependencies. `200` when both the database and
the RPC are reachable and healthy, `503` otherwise.

```sh
curl -s localhost:8080/health
```

```json
{"status":"ok","checks":{"database":"ok","rpc":"ok"}}
```

### `GET /version`

Returns the binary's version, commit, and build date.

```sh
curl -s localhost:8080/version
```

```json
{"version":"v0.1.0","commit":"3c691b4","date":"2026-07-24T12:00:00Z"}
```

### `GET /events`

Lists stored events in ascending (oldest-first) or descending (newest-first) order. Defaults to ascending.

Query parameters (all optional, combinable):

| Param | Example | Meaning |
| --- | --- | --- |
| `contract_id` | `CDLZ...CYSC` | Only events from this contract. |
| `type` | `contract` | `contract` \| `system` \| `diagnostic`. |
| `topic` | `{"symbol":"transfer"}` | Exact match against any topic position. A bare word is treated as a JSON string. |
| `topic0` | `{"symbol":"transfer"}` | Exact match against topic position 0. |
| `topic1` | `{"address":"G..."}` | Exact match against topic position 1. |
| `topic2` | `{"address":"G..."}` | Exact match against topic position 2. |
| `topic3` | `{"u64":7}` | Exact match against topic position 3. |
| `from_ledger` | `250000` | Inclusive lower ledger bound. |
| `to_ledger` | `260000` | Inclusive upper ledger bound. |
| `from_time` | `2026-07-21T00:00:00Z` | Inclusive lower `created_at` bound (RFC 3339). Sub-second precision and missing timezone are rejected. |
| `to_time` | `2026-07-22T00:00:00Z` | Inclusive upper `created_at` bound (RFC 3339). Sub-second precision and missing timezone are rejected. |
| `limit` | `50` | Page size, 1–200 (default 50). |
| `cursor` | `0001234...` | Opaque pagination cursor from a previous response. |
| `order` | `desc` | `asc` | `desc`, defaults to asc. Sort direction. |
| `decoded` | `true` | When `true`, enriches events with spec-driven named fields. Contracts without a spec return flagged raw data with `"decoded": false`. |

Topic filters may use `topic` for any-position matching, or `topic0`..`topic3` for position-specific matching. `topic` and positional topic filters cannot be combined.

```sh
curl -s 'localhost:8080/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&topic={"symbol":"transfer"}&limit=2'
```

```sh
curl -s 'localhost:8080/events?topic0={"symbol":"transfer"}&topic1={"address":"GABC..."}&topic2={"address":"GDEF..."}'
```

```json
{
  "events": [
    {
      "id": "0001099511627776-0000000001",
      "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "ledger": 256000,
      "type": "contract",
      "tx_hash": "9f5c...",
      "tx_index": 1,
      "op_index": 0,
      "in_successful_call": true,
      "topics": [{"symbol":"transfer"},{"address":"G..."},{"address":"G..."}],
      "value": {"i128":"10000000"},
      "created_at": "2026-07-16T12:00:00Z"
    }
  ],
  "cursor": "0001099511627776-0000000001"
}
```

`cursor` is present when more results exist; pass it back as `?cursor=` for
the next page.

Time filtering narrows results and does not change ordering (events remain in
ascending event-ID order, which agrees with `created_at` order because both
follow ledger sequence).

```sh
curl -s 'localhost:8080/events?from_time=2026-07-21T14:00:00Z&to_time=2026-07-21T15:00:00Z'
```

### `GET /events/{id}`

Fetch a single event by its ID (the TOID-based identifier from the RPC).
`404` if unknown.

```sh
curl -s localhost:8080/events/0001099511627776-0000000001
```

### `GET /contracts/{id}/events`

Convenience wrapper for `GET /events?contract_id={id}`; accepts the same
remaining query parameters.

```sh
curl -s localhost:8080/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events?limit=10
```

### Webhooks

Consumers can register callback URLs that receive matching events as they are
ingested. Delivery is asynchronous — it never blocks ingestion — and includes
HMAC-SHA256 signatures so subscribers can verify payload authenticity.

#### `POST /subscriptions`

Register a new webhook subscription.

```sh
curl -s -X POST localhost:8080/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/webhook",
    "secret": "whsec_z8eP5qL3vR2xK9yB4w",
    "filters": {
      "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
      "type": "contract",
      "topic": {"symbol":"transfer"}
    }
  }'
```

Response (`201 Created`):

```json
{
  "id": 1,
  "url": "https://example.com/webhook",
  "filters": {
    "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
    "type": "contract",
    "topic": {"symbol":"transfer"}
  },
  "secret": "whsec_z8eP5qL3vR2xK9yB4w",
  "enabled": true,
  "failure_count": 0,
  "created_at": "2026-07-24T12:00:00Z"
}
```

`filters` has the same shape as `GET /events` query parameters — an empty
object `{}` matches every event. `enabled` defaults to `true`.

#### `GET /subscriptions`

List all subscriptions.

```sh
curl -s localhost:8080/subscriptions
```

#### `GET /subscriptions/{id}`

Fetch a single subscription by ID. `404` if unknown.

```sh
curl -s localhost:8080/subscriptions/1
```

#### `PUT /subscriptions/{id}`

Update a subscription. Only fields present in the body are changed; omit a
field to leave it unchanged.

```sh
curl -s -X PUT localhost:8080/subscriptions/1 \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

#### `DELETE /subscriptions/{id}`

Delete a subscription and its delivery history. `204 No Content` on success.

```sh
curl -s -X DELETE localhost:8080/subscriptions/1
```

#### `GET /subscriptions/{id}/deliveries`

List delivery attempts for a subscription, newest first. Optional `?limit=`
(default 50, max 200).

```sh
curl -s localhost:8080/subscriptions/1/deliveries?limit=10
```

```json
[
  {
    "id": 42,
    "subscription_id": 1,
    "event_id": "0001099511627776-0000000001",
    "status": "success",
    "response_code": 200,
    "duration_ms": 87,
    "created_at": "2026-07-24T12:01:00Z"
  },
  {
    "id": 41,
    "subscription_id": 1,
    "event_id": "0001099511627776-0000000000",
    "status": "failed",
    "response_code": 500,
    "duration_ms": 1024,
    "error": "HTTP 500",
    "created_at": "2026-07-24T12:00:55Z"
  }
]
```

#### Webhook payload

When an ingested event matches a subscription's filters, SoroTrail POSTs the
event to the subscription's URL with this body:

```json
{
  "event": {
    "id": "0001099511627776-0000000001",
    "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
    "ledger": 256000,
    "type": "contract",
    "tx_hash": "9f5c...",
    "tx_index": 1,
    "op_index": 0,
    "in_successful_call": true,
    "topics": [{"symbol":"transfer"},{"address":"G..."},{"address":"G..."}],
    "value": {"i128":"10000000"},
    "created_at": "2026-07-24T12:00:00Z"
  }
}
```

Every request includes an `X-SoroTrail-Signature` header holding the
hex-encoded HMAC-SHA256 digest of the request body, keyed with the
subscription's secret. Subscribers **must verify** this signature to
confirm the payload came from SoroTrail and has not been tampered with.

**Verifying signatures — code samples:**

<details>
<summary>Go</summary>

```go
import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "io"
    "net/http"
)

func verifySignature(r *http.Request, secret string) ([]byte, bool) {
    body, _ := io.ReadAll(r.Body)
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write(body)
    expected := hex.EncodeToString(mac.Sum(nil))
    if !hmac.Equal([]byte(r.Header.Get("X-SoroTrail-Signature")), []byte(expected)) {
        return nil, false
    }
    return body, true
}
```

</details>

<details>
<summary>Python</summary>

```python
import hmac
import hashlib

def verify_signature(request_body: bytes, signature_header: str, secret: str) -> bool:
    expected = hmac.new(secret.encode(), request_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)

# Example Flask endpoint:
# @app.route("/webhook", methods=["POST"])
# def webhook():
#     body = request.get_data()
#     if not verify_signature(body, request.headers.get("X-SoroTrail-Signature", ""), SECRET):
#         return "bad signature", 401
#     payload = json.loads(body)
#     # process payload["event"]
```

</details>

<details>
<summary>JavaScript (Node.js)</summary>

```js
const crypto = require('crypto');

function verifySignature(body, signatureHeader, secret) {
  const expected = crypto.createHmac('sha256', secret).update(body).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signatureHeader));
}

// Example Express endpoint:
// app.post('/webhook', express.raw({ type: 'application/json' }), (req, res) => {
//   const sig = req.headers['x-sorotrail-signature'] || '';
//   if (!verifySignature(req.body, sig, SECRET)) return res.status(401).end();
//   const { event } = JSON.parse(req.body);
//   // process event
// });
```

</details>

<details>
<summary>TypeScript (Bun / Deno)</summary>

```ts
async function verifySignature(req: Request, secret: string): Promise<boolean> {
  const body = await req.arrayBuffer();
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const expected = await crypto.subtle.sign('HMAC', key, body);
  const expectedHex = Array.from(new Uint8Array(expected))
    .map(b => b.toString(16).padStart(2, '0')).join('');
  const sigHeader = req.headers.get('X-SoroTrail-Signature') || '';
  return crypto.subtle.timingSafeEqual(
    new TextEncoder().encode(expectedHex),
    new TextEncoder().encode(sigHeader)
  );
}
```

</details>

#### Delivery semantics

- Delivery is **at-least-once**: a subscriber may receive the same event more
  than once. Deduplicate by event `id`.
- Failed deliveries are retried up to **5 times** with exponential backoff
  (1s → 2s → 4s → 8s → 16s).
- After **5 consecutive failures** the subscription is **auto-disabled**. A
  successful delivery resets the failure counter to 0.
- Delivery attempts are recorded in `delivery_attempts` and queryable via
  `GET /subscriptions/{id}/deliveries`.
- Subscribers should return a **2xx status code** to acknowledge receipt.
  Non-2xx responses are treated as failures and retried.

### `GET /stats`

```sh
curl -s localhost:8080/stats
```

```json
{"total_events":18234,"last_ingested_ledger":260123,"verified_through_ledger":258900,"contract_count":57,"watched_contracts":0,"auditor":{"passes_run":87,"ledgers_checked":1200,"findings_opened":2,"findings_repaired":1,"findings_unverifiable":0,"findings_unrecoverable":1,"rpc_requests":340}}
```

`verified_through_ledger` is the inclusive highest ledger whose stored
events have been proven to match a fresh RPC fetch by the auditor. When
`AUDIT_ENABLED=false` it stays at `0`. See the Data integrity section
below for the contract the field implies.

### `GET /events/ws` (WebSocket live stream)

Pushes ingested events to the client over a single WebSocket connection
as soon as they are written to the store. There is no replay — the
stream starts at "now", and the client only sees events the indexer
ingests after it connects.

Query parameters share the same `EventFilter` shape as `GET /events`
(`contract_id`, `type`, `topic`, `from_ledger`, `to_ledger`,
`from_time`, `to_time`), so any filter that works against the query
API works against the stream.

Frame format:

- One `store.Event` per WebSocket text frame, JSON-encoded.
- Server-to-client only — clients do not send messages; the `nhooyr.io/websocket`
  library handles ping/pong internally.
- The server pings every 30s so proxies don't idle the connection.

Behavior:

- **Slow-consumer eviction**: each subscriber gets a bounded channel
  buffer (`broadcast.DefaultBufferSize` = 64). A subscriber whose
  channel fills is evicted silently: its `Events()` channel is closed,
  the handler returns, and the WebSocket is closed from the server side.
  This protects the indexer from one stuck client back-pressuring the
  broadcaster.
- **Broadcaster unwired**: returns `501 Not Implemented` (only happens
  if the binary was built without the broadcaster wired).
- **Bad filter**: returns `400 Bad Request` before the WebSocket
  upgrade, with the standard `{"error": "..."}` JSON body.

Example with [`websocat`](https://github.com/vi/websocat):

```sh
websocat 'ws://localhost:8080/events/ws?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&topic=mint'
{"id":"…","contract_id":"…","topics":["mint"], …}
{"id":"…","contract_id":"…","topics":["mint"], …}
```

## Data integrity

A background auditor walks recently-ingested ledger ranges (behind the
ingest frontier, inside the RPC's retention window) and re-fetches each
range with the same filter configuration the ingester uses, comparing
the stored event counts and IDs against the fresh response. Mismatches
are logged, recorded in the `audit_findings` table, and auto-repaired
by re-ingesting the affected range with `ReplaceEventsInRange` (which
deletes orphans and updates same-ID rows so topic/value drift on the
RPC side is corrected).

Each pass advances an audit-only `verified_through_ledger` high-water
mark past the clean prefix of the audited range; that field, exposed via
`GET /stats`, is the strongest trust signal SoroTrail can offer: it
names the highest ledger whose stored events have been *verified* against
the RPC, not merely *ingested*.

Audit behaviour:

- **Filter parity**: the auditor uses the ingester's exact filter batch
  (see `Ingester.BuildFilterBatches`), so events the RPC has for
  contracts you're not watching are intentionally not checked and never
  produce false findings.
- **Idempotency**: re-running the auditor over a clean range is a no-op;
  crashes mid-repair leave the finding open so the next pass can retry.
- **Budget**: the auditor shares the request-rate budget with the
  ingester via `rpc.Budget`; `AUDIT_BUDGET_SHARE` (default 10%) caps
  the audit pool while the ingest pool gets the remainder.
- **Lag pause**: if ingest hasn't moved at least `AUDIT_LAG_THRESHOLD`
  ledgers past `verified_through_ledger`, the auditor sleeps until it
  does — it never races ingestion.
- **Retention edges**: when a finding's range ages out of the RPC's
  retention window during repair, the auditor moves the finding to
  `status='unverifiable'` instead of crashing or false-alarming.
- **Self-disagreement**: if the RPC keeps returning different events
  for the same range across repair iterations, the auditor stops after
  `AUDIT_MAX_REPAIR_ATTEMPTS` attempts and keeps the finding visible
  with `status='unrecoverable'` — operators see it via `/stats`.

Set `AUDIT_ENABLED=false` (the default) to disable the auditor entirely;
the binary's behavior is identical to a pre-audit build.

## Caching

Stored events are immutable — a row written by ingest is never rewritten
in normal operation — so the API serves two distinct cache policies:

- **`GET /events/{id}`** carries a strong `ETag` equal to the event ID
  and `Cache-Control: public, max-age=31536000, immutable`. Clients
  and CDNs can cache the response for a year without revalidating.
  Conditional requests with a matching `If-None-Match` return
  `304 Not Modified` without re-serializing the row.
- **List endpoints** (`/events`, `/contracts/{id}/events`) split on a
  moving ingest frontier: a page whose upper bound (`to_ledger`) sits
  entirely below the last-ingested ledger cannot gain new rows, so the
  response is declared immutable with a strong ETag derived from the
  filter. Pages that are open-ended (`to_ledger` unset) or have bounds
  at/above the frontier are still growing, so they get
  `Cache-Control: no-cache` — a deliberate "when in doubt, don't
  cache" choice rather than a guess with a short max-age.
- **`GET /health`** and **`GET /stats`** are always
  `Cache-Control: no-store` so monitoring tooling, dashboards, and
  alerting see real state rather than a stale replica.

All cacheable responses set `Vary: Accept-Encoding` so a future
compression middleware (#25) can serve distinct encoded variants
without reconciling caches that warmed on a non-encoded version.

### Retention/pruning (#8)

Immutability is conditional on the row existing. When pruning deletes an
event that was previously cached by a client or CDN, that cache will
hold a stale copy until its `max-age` expires — clients can hit the
fresh `404` immediately by sending their `If-None-Match` validator, at
which point SoroTrail's `EventExists` probe correctly returns the
not-found status. We accept this self-healing delay rather than
arbitrarily shortening the immutable max-age, because for un-deleted
rows the long `max-age` is the whole point of the cache.

### Auth'd deployments (#17)

When authentication lands, the spec calls out that caching must "never
leak across keys". Today the API is unauthenticated, but the
`CACHE_PRIVATE` config flag flips every cacheable response from
`Cache-Control: public` to `Cache-Control: private` (with the same
`max-age` and `immutable` preset), so the same build can serve shared
caches in one deployment and per-user scenarios in another. Set
`CACHE_PRIVATE=true` as soon as auth (#17) is in front of the API.

## Development

```sh
make build        # compile to bin/sorotrail
make test         # unit tests (Postgres tests skip without a database)
make test-db      # full suite incl. Postgres integration tests
make lint         # golangci-lint
make migrate-up   # apply migrations manually (needs the migrate CLI)
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture notes and extension
points.

## Roadmap / future work

Deliberately out of scope for the MVP, with seams left for contributors:

- Per-standard event decoders (e.g. SEP-41 token transfers) on top of
  `decode.Decoder`.
- Support for more than 25 watched contracts per request chain is implemented
  via windowed sweeps; smarter scheduling (parallel sweeps, per-contract
  cursors) is welcome.
- GraphQL / websocket subscriptions.
- Metrics (Prometheus) and tracing.
- Alternative storage backends behind `store.Store`.

## License

[Apache-2.0](LICENSE)
