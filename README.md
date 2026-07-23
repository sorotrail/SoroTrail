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

### `GET /events`

Lists stored events in ascending event-ID (chronological) order.

Query parameters (all optional, combinable):

| Param | Example | Meaning |
| --- | --- | --- |
| `contract_id` | `C1&contract_id=C2` | Events for any of the listed contracts. Use repeated `contract_id` params; comma-separated lists are rejected. |
| `type` | `contract&type=system` | Events for any of the listed types. Use repeated `type` params; comma-separated lists are rejected. |
| `topic` | `{"symbol":"transfer"}` | Exact match against any topic position. A bare word is treated as a JSON string. |
| `from_ledger` | `250000` | Inclusive lower ledger bound. |
| `to_ledger` | `260000` | Inclusive upper ledger bound. |
| `limit` | `50` | Page size, 1–200 (default 50). |
| `cursor` | `0001234...` | Opaque pagination cursor from a previous response. |

```sh
curl -s 'localhost:8080/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&topic={"symbol":"transfer"}&limit=2'

```sh
curl -s 'localhost:8080/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&contract_id=CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&type=contract&type=system&limit=2'
```
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

### `GET /events/{id}`

Fetch a single event by its ID (the TOID-based identifier from the RPC).
`404` if unknown.

```sh
curl -s localhost:8080/events/0001099511627776-0000000001
```

### `GET /contracts/{id}/events`

Convenience wrapper for `GET /events?contract_id={id}`; accepts the same
remaining query parameters. It rejects an explicit `contract_id` query
parameter to avoid conflicting filters.

```sh
curl -s localhost:8080/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events?limit=10
```

### `GET /stats`

```sh
curl -s localhost:8080/stats
```

```json
{"total_events":18234,"last_ingested_ledger":260123,"contract_count":57,"watched_contracts":0}
```

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
