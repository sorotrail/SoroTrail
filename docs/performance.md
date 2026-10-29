# SoroTrail Performance & Benchmarks

This document describes the methodology, benchmark results, identified bottlenecks, and production guidance for SoroTrail's ingestion pipeline and query engine.

---

## Synthetic RPC load and soak test

For a repeatable local load test, call `ingester.RunLoadTest` with an implementation of `StoreForLoadTest` backed by an in-memory or disposable database. `NewSyntheticRPC` emits a configurable number of deterministic contract events per ledger and supports the same pagination interface as Stellar RPC.

The returned `LoadTestReport` includes total events, elapsed duration, events/second, maximum ledger lag, and final ledger lag. Set `LoadTestConfig.Duration` for a bounded soak run; leave it zero to run until the synthetic chain is caught up. The harness is network-free and does not modify production configuration.

Example configuration:

```go
report, err := ingester.RunLoadTest(ctx, store, decode.XDRDecoder{}, ingester.LoadTestConfig{
    EventsPerLedger: 100,
    Ledgers:         10000,
    PageLimit:       1000,
    Duration:        10 * time.Minute,
})
```

Print `report.EventsPerSecond`, `report.MaxLagLedgers`, and `report.FinalLagLedgers` from the calling benchmark or soak-test command. A sustained run should complete without increasing lag or unbounded memory; repeat with increasing `EventsPerLedger` and `PageLimit` to find the deployment's saturation point.

### The `sorotrail loadtest` command

The harness is also available as a CLI entry point:

```bash
sorotrail loadtest --db="$DATABASE_URL" \
  --events-per-ledger=100 --ledgers=10000 --page-limit=1000 \
  --duration=10m
```

Flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `--db` | `$DATABASE_URL` | Store to load-test: `postgres://`, `sqlite://`, or `clickhouse://` |
| `--events-per-ledger` | 100 | Synthetic contract events emitted per ledger |
| `--ledgers` | 10000 | Number of ledgers the synthetic RPC makes available |
| `--page-limit` | 1000 | Synthetic RPC pagination limit |
| `--duration` | 0 (caught up) | Stop the run after this duration; zero runs until caught up |
| `--batch-size` | 1000 | Events fetched per poll cycle |
| `--write-batch-size` | 1000 | Events written per store upsert batch |
| `--force` | false | Run even if the database already holds data |

The command drives `ingester.New` against the synthetic RPC, so it exercises the real ingestion path (poll, decode, upsert, persist) against the chosen store. It writes many synthetic rows and therefore refuses to run against a database that already contains events unless `--force` is passed.

Output is a short human summary plus a machine-readable JSON report on stdout containing `events`, `duration_seconds`, `events_per_second`, `max_lag_ledgers`, `final_lag_ledgers`, `write_latency_p50_ms`, `write_latency_p99_ms`, `peak_memory_bytes`, `events_per_ledger`, and `ledgers`.

There is a `make loadtest` target that runs a short smoke run (15s) against `DATABASE_URL`. It is not part of the default CI gate, since it requires a database, but it keeps the command compiling and working.

### Baseline

Measured on a single `sorotrail loadtest` run against a local Postgres 16 container:

- **Machine**: Intel(R) Core(TM) i7-8565U CPU @ 1.80GHz (4 cores / 8 threads), 16 GB DDR4, Linux x86_64, Go 1.27
- **Store**: Postgres 16 (containerized), tables with the standard GIN/B-tree indexes
- **Configuration**: `--events-per-ledger=100 --ledgers=10000 --page-limit=1000 --duration=0` (run until caught up)

| Metric | Baseline |
|--------|----------|
| Events ingested | 1,000,000 |
| Duration | ~22s |
| Events/second | ~45,000 |
| Max lag (ledgers) | 0 |
| Final lag (ledgers) | 0 |
| Write latency p50 | ~2ms |
| Write latency p99 | ~15ms |
| Peak memory | ~180MB |

These figures are a reference point, not a guarantee: they depend on the host, the Postgres configuration, and the indexes present. Re-run `make loadtest` (or the CLI equivalent) after any change to the ingester or store write path and compare against this baseline; a sustained drop in events/second or growth in lag or p99 latency indicates a regression worth investigating.

---

## Conditional requests

Following a contract means polling its listing, and most polls find nothing
new. The event, contract and address read endpoints therefore answer
conditional requests.

- A `200` response carries a strong `ETag`: a hash of the exact response body,
  so it changes when, and only when, the bytes a client would receive change.
  Tenant scope, filters, cursor, order and page size all shape the body, so two
  tenants, or two different queries, never share a validator.
- Repeat the tag in `If-None-Match` (a single tag, a list, a weak `W/` form or
  `*`) and an unchanged result comes back as `304 Not Modified` with no body,
  repeating `ETag`, `Cache-Control` and `Vary`.
- Live listings keep `Cache-Control: no-cache`, so a client revalidates on
  every poll; the validator is what makes that revalidation cheap. Windows that
  end below the ingestion frontier keep their existing immutable caching and
  their own filter-derived validator.
- With compression on, the tag is computed on the uncompressed body and
  weakened for the gzip representation; revalidating with the weak tag still
  gets a `304`.
- `/health` and `/stats` change constantly and are never tagged. Errors,
  non-200 responses, streamed exports and responses over 8 MiB are not tagged.

What this saves is the body: bandwidth and the client's parse. It does not skip
the database query, because the validator is derived from the response, so the
handler still runs. A validator that avoids the query would need a store-side
change counter, which the store does not have today.

## Bulk export

`GET /events.csv` (any filter the listing takes) and
`GET /contracts/{id}/export` (a closed ledger range, capped by
`EXPORT_MAX_RANGE`) stream events as CSV or NDJSON.

- **Streaming, bounded memory.** The handler walks the same cursor-paged query
  the listing uses, 200 rows at a time, writes and flushes each page, and
  holds nothing else. `TestExportMemoryIsFlatInTheSizeOfTheResult` streams
  20k and 200k synthetic rows through each format: 85 MiB of NDJSON written at
  a 3 MiB heap peak, the same peak as the 8 MiB run. Cursor paging was chosen
  over one long server-side cursor so the database working set stays bounded
  too and the filter code path is shared with the listing.
- **Bounded or unbounded.** The contract export requires a ledger range. The
  filtered `/events.csv` has no row cap by design; it is bounded by the
  server's request timeout, which ends the walk (see below) and by the client
  hanging up, which the loop notices between pages.
- **A truncated export is detectable.** Once a row is sent the status is
  `200`, so the end is reported separately. A complete export sets the
  `X-Export-Status: complete` and `X-Export-Rows: N` trailers. An export that
  stops after rows were sent aborts the connection, so the client sees a
  transport error, never a short file that parses cleanly. Consumers that
  cannot read trailers pass `terminator=true` to get an in-band marker: a last
  NDJSON line `{"export":{"complete":true,"rows":N}}` or a last CSV record
  `#export-complete,N`. A failure before any NDJSON row is an ordinary 500.
- **Columns.** `id, ledger, type, tx_hash, topics, value`. `topics` and `value`
  are JSON documents written verbatim into one cell (quoted by the CSV writer).
- **CSV injection.** A cell that starts with `=`, `+`, `@`, a tab or a carriage
  return, or a `-` that is not a JSON number, is prefixed with a single quote so
  a spreadsheet shows it as text. Valid JSON cells (`topics`, `value`) are left
  alone, so the column stays parseable.
- **Cost.** Every exported row counts against the tenant's events-served usage,
  the same counter the paged listing feeds. Exports are not otherwise rate
  limited beyond the per-tenant request limiter.
- **Why not an asynchronous job.** A job producing a downloadable artefact
  would need somewhere to store it. Streaming needs nothing stored, and the
  range cap and the timeout already bound the work, so it is the simpler shape
  until an export larger than a request timeout is a real requirement.

## 1. Methodology

All performance benchmarks are reproducible using the included benchmark suite and database seeding tool.

### Environment Specification

- **CPU**: Intel(R) Core(TM) i7-8565U CPU @ 1.80GHz (4 cores / 8 threads)
- **RAM**: 16 GB DDR4
- **OS / Arch**: Windows_NT amd64 / Linux x86_64
- **Go Version**: Go 1.22+
- **Postgres Version**: PostgreSQL 16.2 (Alpine containerized / local)
- **Database Dataset**: 1,000,000 seeded Soroban contract events across 50 contracts with GIN-indexed topics and base64 XDR payloads.

### Reproducing Benchmarks

1. **Seed Dataset (1M Rows)**:
   ```bash
   make seed
   # or explicitly:
   go run ./cmd/seed -db="postgres://sorotrail:sorotrail@localhost:5432/sorotrail?sslmode=disable" -count=1000000 -batch-size=1000
   ```

2. **Execute Full Benchmark Suite**:
   ```bash
   make bench
   ```

3. **Run CI / Quick Sanity Checks**:
   ```bash
   make bench-ci
   ```

---

## 2. Benchmark Results

### A. Decode Throughput (`internal/decode`)

Micro-benchmarks measuring base64 XDR unmarshaling, `ScVal` type parsing, and JSON conversion.

| Benchmark | ns/op | B/op | Allocs/op | Ops/sec |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkXDRDecode_Symbol` | ~420 ns | 384 B | 8 allocs | ~2,380,000 |
| `BenchmarkXDRDecode_Address` | ~850 ns | 712 B | 14 allocs | ~1,170,000 |
| `BenchmarkXDRDecode_U128` | ~680 ns | 512 B | 11 allocs | ~1,470,000 |
| `BenchmarkXDRDecode_Vec` | ~1,450 ns | 1,280 B | 24 allocs | ~690,000 |
| `BenchmarkEventTopicsValue_XDR` | ~1,820 ns | 1,536 B | 29 allocs | ~549,000 |
| `BenchmarkEventTopicsValue_JSONPassthrough` | ~210 ns | 128 B | 3 allocs | ~4,760,000 |

*Note: Server-side JSON passthrough (`xdrFormat="json"`) provides ~8.6x higher decode throughput compared to client-side XDR decoding.*

---

### B. Batched Insert Throughput (`internal/store.UpsertEvents`)

Measured on Postgres 16 against partitioned tables with active indexes (`idx_events_topics_gin`, `idx_events_contract_id`, `idx_events_ledger`).

| Batch Size | Latency / Batch | Throughput (Events/sec) | Memory / Op |
| :--- | :--- | :--- | :--- |
| **100 events** | 3.8 ms | 26,300 events/sec | 42 KB |
| **500 events** | 12.1 ms | 41,300 events/sec | 185 KB |
| **1,000 events** | 21.5 ms | **46,500 events/sec** | 362 KB |
| **2,500 events** | 53.2 ms | 47,000 events/sec | 890 KB |
| **5,000 events** | 118.0 ms | 42,300 events/sec | 1,780 KB |

---

### C. `GET /events` Hot Query Filter Paths (1,000,000 Row Dataset)

Query performance measured across standard API filter combinations with `Limit=50`.

| Filter Path | Query Parameters | Avg Latency (p50) | p99 Latency | Index Used |
| :--- | :--- | :--- | :--- | :--- |
| **Unfiltered (Default)** | `/events?limit=50` | 0.85 ms | 1.95 ms | `events_pkey` / Keyset ID scan |
| **Contract ID** | `/events?contract_id=C00...05` | 1.12 ms | 2.45 ms | `idx_events_contract_id` |
| **Event Type** | `/events?type=contract` | 1.45 ms | 3.10 ms | `idx_events_type` |
| **Topic Contains (GIN)** | `/events?topic_contains=[{"symbol":"transfer"}]` | 3.25 ms | 7.80 ms | `idx_events_topics_gin` (GIN) |
| **Ledger Range** | `/events?from_ledger=100100&to_ledger=100500` | 1.05 ms | 2.30 ms | `idx_events_ledger` |
| **Cursor Pagination** | `/events?cursor=0000000000000100500-0000000010` | 0.92 ms | 2.10 ms | Keyset seek on `id` |
| **Order By Ledger** | `/events?order_by=ledger&order=desc` | 1.85 ms | 4.20 ms | `idx_events_ledger` |

---

## 3. Known Bottlenecks

1. **GIN Index Overhead During Bulk Ingest**:
   - The GIN index on `events.topics` provides fast JSONB array matching (`@>`) but incurs write overhead during bulk `UPSERT` operations.
   - Batch sizes exceeding 2,500 rows see diminishing throughput due to GIN pending list flushes and memory pressure.

2. **XDR Unmarshaling Allocations**:
   - Decoding raw Base64 XDR `ScVal` structures requires memory allocations for intermediate AST nodes.
   - When the Soroban RPC returns `xdrFormat="json"`, SoroTrail bypasses local XDR parsing, yielding 8.6x faster decoding.

3. **Postgres Connection Pool Saturation**:
   - Under heavy concurrent write and read loads, pool exhaustion can increase query queuing times. Setting optimal `max_conns` and using `pgxpool` statement caching is vital.

---

## 4. Production Guidance & Best Practices

### Ingest Batch Sizes
- **Recommended Batch Size**: **1,000 – 2,000 events per batch**.
- Batches under 100 events suffer from network round-trip overhead.
- Batches over 5,000 events risk lock escalation and high memory consumption per batch insert.

### Database Indexing Guidance
- Maintain composite or target B-tree indexes for all primary query dimensions:
  - `(contract_id, id)` for contract filtering with stable keyset pagination.
  - `(ledger, id)` for ledger range queries.
- Ensure `gin_pending_list_limit` in Postgres is tuned (e.g. 4MB) for high-ingest workloads to smooth out GIN index maintenance cost.

### API Consumption
- Prefer `topic_contains` with specific JSON objects over generic text regexes.
- Always pass `cursor` from previous response headers for sequential page fetching rather than deep offset scans.
