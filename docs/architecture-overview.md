# Architecture Overview

This is the fast-tour for new contributors. For the full component write-up
with data-flow diagrams, see [`docs/architecture.md`](architecture.md).

SoroTrail is a contract-event indexer for Stellar/Soroban. It polls a Stellar
RPC endpoint, decodes and stores events durably, and serves them back through
an HTTP API. The codebase is deliberately organized around **four seams** so
that each layer can be developed, tested, and replaced independently.

```
 Stellar RPC
     │  getEvents
     ▼
 ┌───────────┐   upsert    ┌──────────┐   query   ┌───────────┐
 │ ingester  │ ──────────▶ │  store   │ ◀──────── │    api    │
 └───────────┘             └──────────┘           └───────────┘
       │                        ▲                      │
       │                        │                      │ enrich
       │                  spec.Enricher ──────────────┘
       ▼
  decode.Decoder
```

## The four seams

### 1. `ingester` — the polling loop seam

`internal/ingester` drives ingestion. It owns the resume cursor, builds RPC
filter batches, pages through `getEvents`, decodes each event, and persists.
It depends on **interfaces**, never on concrete types:

| Dependency | Interface | Why it's a seam |
| --- | --- | --- |
| Stellar RPC | `rpc.Client` | swap/extend RPC methods without touching the loop |
| Decoding | `decode.Decoder` | richer ScVal handling, per-standard decoders |
| Persistence | `store.Store` | alternative backends (see `store.Store`) |
| Post-ingest | `ingester.EventNotifier` | webhooks, SSE, broadcast — no loop changes |

The high-level cycle (`internal/ingester/ingester.go`): resolve cursor →
build filter batches → `GetEvents` → decode → `UpsertEvents` → advance cursor
→ `EventNotifier`. Errors retry with jittered backoff; upserts are idempotent
so restarts and re-scans never duplicate rows.

### 2. `store` — the persistence seam

`internal/store` is the single source of truth for durability. Everything goes
through the `store.Store` interface so the ingester, API, auditor, replay, and
webhook layers never import a concrete database driver. Methods fall into a few
families:

- **Ingest**: `UpsertEvents`, `GetIngestionState`/`SaveIngestionState`
- **Query**: `QueryEvents` (ascending ID order required for cursor paging),
  `GetEvent`/`EventExists`, `Stats`, `Ping`
- **Subscriptions**: `CreateSubscription`/…, `ListEnabledSubscriptions`,
  `RecordDeliveryAttempt`
- **Audit/Replay**: `LedgerRangeCensus`, `ReplaceEventsInRange`,
  `GetReplayState`/`NextReplayBatch`/`CommitReplayBatch`
- **Spec**: `GetContractSpec`/`SetContractSpec`

The PostgreSQL implementation (`*Postgres`) is the production backend. The
`events` table is partitioned by ledger range and raw XDR is kept alongside
decoded JSON so decoder replay can re-write history. Adding a backend means
implementing `store.Store` in full.

### 3. `api` — the read surface seam

`internal/api` (chi handlers, wired in `internal/api/server.go`) is read-only
by default and depends only on `store.Store` plus the `spec.Enricher`. Endpoints:
`/health`, `/events`, `/events/{id}`, `/contracts/{id}/events`, `/events/ws`,
`/stats`, and the webhook subscription CRUD. New endpoints are added here;
keep them read-only unless you also add authentication. The OpenAPI source of
truth lives in `api/openapi.yaml` and is embedded at `/openapi.json` via
`make spec` (regenerated JSON copy is asserted by `pkg/docs`).

### 4. `spec` — the enrichment seam

`internal/spec` attaches human-readable field names to events by fetching and
parsing a contract's Wasm spec (`internal/spec/fetcher.go`), caching it
(`contract_specs` table + in-memory), and enriching events on `?decoded=true`.
It is opt-in and layered *on top of* stored JSON — it never changes what the
ingester writes, so it can be enabled/upgraded without re-ingestion.

## How the seams connect

```
cmd/sorotrail/main.go
   ├─ wires rpc.Client + decode.Decoder + store.Store + config
   ├─ starts ingester  ──────────────▶ store.Store
   ├─ starts api        ─────────────▶ store.Store + spec.Enricher
   ├─ starts webhook    (EventNotifier) ─▶ store.Store
   └─ (optional) starts audit ───────▶ store.Store + rpc.Client + ingester
```

Because every cross-layer call is an interface, a unit test can mock the store
or the RPC (see `internal/ingester/mocks_test.go`) and the integration suite
exercises each seam against a real Postgres via `internal/testdb`.

## Where to start

- Add a decoder type → `decode.Decoder` / `internal/decode/xdr.go`
- New storage backend → implement `store.Store`
- New RPC method → add to `rpc.Client` (only what ingester/API need)
- New endpoint → `internal/api/server.go`
- Schema change → new numbered pair in `internal/store/migrations/`
  (never edit an applied migration; update the migration test)
- Backfill a derived table → wire it through `store.ReplayBatch`

See [`docs/local-setup.md`](local-setup.md) to build, test, and run locally,
and [`docs/triage.md`](triage.md) for how changes get reviewed.
