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

### `GET /events`

Lists stored events in ascending (oldest-first) or descending (newest-first) order. Defaults to ascending.

Query parameters (all optional, combinable):

| Param | Example | Meaning |
| --- | --- | --- |
| `contract_id` | `CDLZ...CYSC` | Only events from this contract. |
| `type` | `contract` | `contract` \| `system` \| `diagnostic`. |
| `topic` | `{"symbol":"transfer"}` | Exact match against any topic position. A bare word is treated as a JSON string. |
| `from_ledger` | `250000` | Inclusive lower ledger bound. |
| `to_ledger` | `260000` | Inclusive upper ledger bound. |
| `limit` | `50` | Page size, 1–200 (default 50). |
| `cursor` | `0001234...` | Opaque pagination cursor from a previous response. |
| `order` | `desc` | `asc` | `desc`, defaults to asc. Sort direction. |

```sh
curl -s 'localhost:8080/events?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&topic={"symbol":"transfer"}&limit=2'
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
remaining query parameters.

```sh
curl -s localhost:8080/contracts/CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC/events?limit=10
```

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

## Monitoring

SoroTrail exposes Prometheus metrics at `GET /metrics` in the standard
text format. The endpoint is read-only (no auth required) and powered by
`github.com/prometheus/client_golang`.

### Metrics

| Name | Type | Labels | Description |
| --- | --- | --- | --- |
| `sorotrail_events_ingested_total` | counter | — | Total number of contract events persisted. |
| `sorotrail_last_ingested_ledger` | gauge | — | Ledger sequence of the most recently ingested event. |
| `sorotrail_chain_head_ledger` | gauge | — | Latest ledger reported by the Stellar RPC. |
| `sorotrail_rpc_requests_total` | counter | `method`, `outcome` | RPC requests by JSON-RPC method and `success` / `error`. |
| `sorotrail_http_requests_total` | counter | `path`, `status` | HTTP requests by route pattern and status code. |
| `sorotrail_http_request_duration_seconds` | histogram | `path` | Request duration in seconds, bucketed with Prometheus defaults. |

### Example alert

Lag behind the chain head is the most important signal — it means the
ingester has fallen behind and the resume point risks ageing out of the
RPC retention window:

```yaml
# AlertManager rule
groups:
  - name: sorotrail
    rules:
      - alert: IngesterLagging
        expr: sorotrail_chain_head_ledger - sorotrail_last_ingested_ledger > 200
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "SoroTrail ingester is {{ $value }} ledgers behind the chain head"
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
