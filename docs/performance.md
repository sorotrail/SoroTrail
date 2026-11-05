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

### A. Decode & Ingestion Hot-Path Throughput (`internal/decode`, `internal/ingester`)

Micro-benchmarks and end-to-end batch benchmarks measuring base64 XDR unmarshaling, `ScVal` JSON serialization, `G...`/`C...` address extraction, and full 100-event batch ingestion (`BenchmarkIngestHotPath`, Apple M4 arm64, `go test -bench=... -benchmem`).

| Benchmark | Before (`ns/op`) | After (`ns/op`) | Speedup | Before (`B/op`) | After (`B/op`) | Bytes Saved | Before (`allocs/op`) | After (`allocs/op`) | Allocs Saved |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkXDRDecode_Symbol` | 605.0 ns | **103.7 ns** | **5.8x** | 2,897 B | **152 B** | **-94.8%** | 17 allocs | **5 allocs** | **-70.6%** |
| `BenchmarkXDRDecode_Address` | 786.0 ns | **227.4 ns** | **3.5x** | 3,097 B | **352 B** | **-88.6%** | 18 allocs | **6 allocs** | **-66.7%** |
| `BenchmarkXDRDecode_U128` | 735.3 ns | **199.6 ns** | **3.7x** | 3,098 B | **320 B** | **-89.7%** | 21 allocs | **8 allocs** | **-61.9%** |
| `BenchmarkXDRDecode_Vec` | 1,640 ns | **313.7 ns** | **5.2x** | 4,938 B | **848 B** | **-82.8%** | 43 allocs | **12 allocs** | **-72.1%** |
| `BenchmarkEventTopicsValue_XDR` | 2,051 ns | **315.1 ns** | **6.5x** | 8,776 B | **424 B** | **-95.2%** | 52 allocs | **12 allocs** | **-76.9%** |
| `BenchmarkEventTopicsValue_JSONPassthrough` | 357.8 ns | **277.1 ns** | **1.3x** | 120 B | **96 B** | **-20.0%** | 2 allocs | **1 alloc** | **-50.0%** |
| `BenchmarkExtractAddresses_Transfer` | 4,996 ns | **355.1 ns** | **14.1x** | 6,040 B | **320 B** | **-94.7%** | 87 allocs | **3 allocs** | **-96.6%** |
| `BenchmarkIngestHotPath` (100 events) | 929,545 ns | **109,827 ns** | **8.5x** | 1,895,155 B | **147,521 B** | **-92.2%** | 16,121 allocs | **1,906 allocs** | **-88.2%** |

#### Ingestion Hot-Path Allocation Profile (`pprof` Breakdown)

Profiling a 100-event transfer batch (`go test -bench=^BenchmarkIngestHotPath$ -memprofile=... ./internal/ingester`) identified three primary allocation sources before optimization:

1. **Address extraction (`decode.ExtractAddresses` / `decode.extractFromValue` — 54.6% of `alloc_objects`, 32.4% of `alloc_space`)**:
   - Previously, `extractFromValue` attempted up to three `json.Unmarshal` calls (`map[string]json.RawMessage`, `[]json.RawMessage`, and `string`) at every node of the JSON tree and allocated a `map[[2]string]bool` plus `strings.Builder` per topic role.
   - Replaced with a single-pass JSON byte scanner (`scanJSONValueEnd` / `scanJSONStringEnd`), precomputed `topic[0]`–`topic[15]` role strings, stack-backed deduplication for the common case (`≤8` unique `(address, role)` pairs per event), and `decode.AppendAddresses` so `ingester.indexEventAddresses` reuses one scratch slice across an entire batch.
2. **Base64 + XDR unmarshaling (`xdr.SafeUnmarshalBase64` — 43.4% of `alloc_space`)**:
   - `xdr.SafeUnmarshalBase64` allocated a streaming `base64.NewDecoder` (~2 KB read buffer), `strings.Reader`, `bytes.Buffer`, and `io.TeeReader` on every topic and value `ScVal`.
   - Replaced with `decodeScratchPool` (`sync.Pool`), which decodes single-line base64 directly into a pooled byte slice (`sc.rawBuf`), resets a pooled `bytes.Reader`, and unmarshals into a pooled `sc.val` (`xdr.ScVal`).
3. **Intermediate Go AST + reflection JSON marshaling (`decode.scValToGo` + `encoding/json.Marshal` — 22.4% of `alloc_objects`)**:
   - `scValToGo` previously allocated intermediate `map[string]any` wrappers, `[]any` slices, and heap `big.Int` values before passing them to reflection-driven `json.Marshal`, and `decodeXDROnlyEvent` marshaled each topic separately before re-marshaling `[]json.RawMessage`.
   - Replaced with direct byte-buffer serialization (`appendScValJSON`), 64-bit fast paths for `u128`/`i128`/`u256`/`i256` values whose upper words are zero/sign-extension, stack `big.Int` formatting for wide integers, and single-allocation topic array assembly in `decodeXDROnlyEvent`.

**Pool Lifetime Rule**: `decodeScratch` instances are acquired from `decodeScratchPool` at the entry of `DecodeScVal` or `decodeXDROnlyEvent` and returned via `defer` before returning. Every returned `json.RawMessage` is copied into a freshly allocated slice of exact length before `Put`, so pooled buffers never alias `store.Event` fields or cross goroutines.

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

---

## 5. Lag-Aware Backpressure & Catch-Up Mode

When the ingester falls behind the chain head by more than `LagWarnLedgers`, reporting the lag metric (`SetLagging(true)`) is combined with an opt-in lag-aware backpressure coordinator (`ingester.LagCoordinator` in `internal/ingester/backpressure.go`) that actively helps the ingester catch up while yielding competing background store I/O:

1. **Hysteresis Band (No Oscillation at the Threshold Boundary)**:
   - Entering the lagging state requires `lag > EnterLagLedgers` (`Options.LagWarnLedgers`).
   - Clearing the lagging state requires `lag <= RecoverLagLedgers` (`Options.LagRecoverLedgers`, defaulting to `75%` of `LagWarnLedgers`).
   - While lag fluctuates inside the hysteresis deadband (`RecoverLagLedgers < lag <= EnterLagLedgers`), the coordinator preserves its current state instead of toggling catch-up mode or background job deferral on every ledger.

2. **Ingester Catch-Up Acceleration & Closing-Rate Telemetry**:
   - While lagging with `LagBackpressureEnabled: true`, `effectiveSweepWindow()` scales `SweepWindow` by `CatchUpSweepMultiplier` (default `2x`) so each multi-contract sweep covers a wider ledger range per RPC round-trip.
   - `adjustPollInterval` keeps shrinking the poll interval toward `PollIntervalMin` until the hysteresis recovery watermark is crossed.
   - `LagCoordinator.ObserveLag` tracks the lag closing rate in `ledgers/sec` (`LagBackpressureStats.ClosingRateLedgersPerSec`) and logs structured transition events (`ingester lag backpressure engaged` / `ingester lag backpressure cleared`).

3. **Bounded Background Job Yielding (`pruner`, `archive`, `audit`)**:
   - Background jobs that compete with ingestion for database write IOPS and buffer cache (`pruner.Pruner`, `archive.Archiver`, and `audit.Auditor`) accept a `LagYieldController` (`ShouldYield(job string) (yield bool, reason string)`).
   - While the ingester is lagging, background passes and batch loops yield early (`reason="ingest_lagging"`) and increment per-component yield counters (`LagYields`).
   - To prevent indefinite starvation of retention pruning, cold-storage archiving, or audit-log verification during prolonged chain catch-up, each job has a configurable maximum deferral window (`LagMaxDeferral`, default `15m`). Once a job has been continuously deferred for `>= MaxDeferral`, `ShouldYield(job)` returns `(false, "max_deferral_reached")`, allowing one bounded maintenance pass to run and resetting that job's deferral timer.

---

## 6. Deep Pagination Over Wide Ledger Ranges

Walking hundreds of pages across wide ledger windows (`GET /events` and `GET /contracts/{id}/events`) stays flat in per-page latency (`O(limit)`) when two query-planner properties hold:

1. **Cursor-Driven Partition Pruning (`store.CursorLedger` & `buildQueryEventsSQL`)**:
   - The `events` table is range-partitioned by `ledger` (`PARTITION BY RANGE (ledger)` with `DefaultEventPartitionSpan = 120,960` ledgers per child partition).
   - Under `order_by=id` (default), canonical 64-bit Stellar SEP-0035 TOIDs (`<20-digit TOID>-<10-digit seq>`) encode the ledger sequence in their upper 32 bits (`toid >> 32`). `CursorLedger` extracts that ledger and `buildQueryEventsSQL` appends an inclusive scalar bound (`ledger >= $N` for `asc`, `ledger <= $N` for `desc`) alongside `id > $M` / `id < $M`.
   - Under `order_by=ledger`, PostgreSQL row-value comparisons `(ledger, id) > ($1::bigint, $2)` alone do not trigger range partition pruning. `buildQueryEventsSQL` therefore emits the scalar `ledger >= $1::bigint` (or `ledger <= $1::bigint` for `desc`) reusing the same bind parameter alongside `(ledger, id) > ($1::bigint, $2)`.
   - In both orderings, PostgreSQL prunes every child partition prior to (for `asc`) or after (for `desc`) the cursor's ledger and bounds the `(contract_id, ledger, id)` or `(ledger, id)` index scan without touching earlier partitions.

2. **Sparse Topic Filter Range Bounding (`store.ValidateSparseFilterRange`)**:
   - When a query filters on sparse JSONB topic predicates (`topic`, `topic_contains`, `topic0..topic3`) that match few or zero rows across millions of events, an unbounded cursor walk would otherwise scan every partition or intersect large GIN posting lists across the entire retention window before filling a single page.
   - When `MaxSparseFilterLedgerSpan` is enabled (`store.DefaultMaxSparseFilterLedgerSpan = 500,000` ledgers, ~4 partitions, via `store.Postgres.SetMaxSparseFilterLedgerSpan`, `api.SetMaxSparseFilterLedgerSpan`, or `EventFilter.MaxSparseFilterLedgerSpan`), sparse topic queries that omit `from_ledger`/`to_ledger` or span more than the configured ledger window fail fast with `store.ErrSparseFilterRangeTooWide` (`HTTP 400 Bad Request`, `code: "RANGE_TOO_WIDE"`, `field: "to_ledger"`).
   - **Consumer Guidance**: For sparse topic searches, bound `[from_ledger, to_ledger]` to `<= 500,000` ledgers per window and advance the window sequentially, or use the streaming bulk export endpoints (`GET /events.csv` and `GET /contracts/{id}/export`) which stream bounded 200-row cursor pages in flat heap memory.

### Deep Pagination Benchmark Results (`BenchmarkDeepPagination_WideRange`)

Measured via `go test -bench=BenchmarkDeepPagination_WideRange -benchmem ./internal/store` across a 6,000-event wide-range dataset spanning 500,000 ledgers (`100,000..599,750`, 3 events/ledger, 8 contracts, `Limit=50`, `ANALYZE` run after seed, Apple M4 `arm64`):

| Scenario | Page 1 (`ns/op`) | Page 20 (`ns/op`) | Page 50 (`ns/op`) | Page 100 (`ns/op`) | Page 20 → 100 Drift | Memory / Page (`B/op`) | Allocs / Page |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `OrderByID_Asc` | 79,954 ns (0.080 ms) | 88,944 ns (0.089 ms) | 88,755 ns (0.089 ms) | **90,183 ns (0.090 ms)** | **+1.4% (flat)** | 96,528 B | 1,381 allocs |
| `OrderByID_Desc` | 89,503 ns (0.090 ms) | 99,019 ns (0.099 ms) | 97,921 ns (0.098 ms) | **98,444 ns (0.098 ms)** | **-0.6% (flat)** | 96,520 B | 1,381 allocs |
| `OrderByLedger_Asc` | 81,296 ns (0.081 ms) | 93,606 ns (0.094 ms) | 93,889 ns (0.094 ms) | **93,613 ns (0.094 ms)** | **+0.0% (flat)** | 97,018 B | 1,392 allocs |
| `OrderByLedger_Desc` | 91,240 ns (0.091 ms) | 103,066 ns (0.103 ms) | 102,843 ns (0.103 ms) | **102,642 ns (0.103 ms)** | **-0.4% (flat)** | 97,020 B | 1,392 allocs |
| `ContractFiltered_OrderByID` | 93,653 ns (0.094 ms) | 105,583 ns (0.106 ms) | — | — | **flat** | 96,784 B | 1,385 allocs |
| `SparseTopic_BoundedLedgerRange` | 1,158,714 ns (1.16 ms) | — (page 3: 1.15 ms) | — | — | **-0.5% (flat)** | 56,984 B | 1,001 allocs |

---

## 7. Database Connection Pool Budget, Subsystem Isolation & Backpressure

To prevent a burst of slow read, GraphQL, or export queries from starving the ingester or exhausting the PostgreSQL connection pool, SoroTrail partitions pool capacity across logical subsystems via `store.PoolBudget` and `store.BudgetedStore` (`internal/store/pool_budget.go`) and applies fast-fail backpressure (#1231).

### A. Pool Sizing Formula & Defaults

For a PostgreSQL host with $C$ vCPUs and $R$ SoroTrail replicas sharing the database, choose per-replica `DB_MAX_CONNS` ($N$) using:

$$N = \left\lfloor \frac{\min(\text{postgres\_max\_connections} - \text{reserved\_admin\_conns},\; 4 \times C)}{R} \right\rfloor$$

When `DB_MAX_CONNS` is unset (`0`), `store.ParsePoolConfigWithTLS` and `config.Config.ResolvePoolSizing()` apply sensible production defaults for $N = 20$:

| Parameter | Environment Variable | Default ($N = 20$) | Sizing Formula |
|---|---|---|---|
| Total pool ceiling | `DB_MAX_CONNS` | `20` | $N$ |
| Warm minimum idle conns | `DB_MIN_CONNS` | `5` | $\max(2, \lfloor 0.25 \times N \rfloor)$, clamped to $\le N$ |
| Max connection lifetime | `DB_MAX_CONN_LIFETIME` | `30m` | `30m` |
| Max connection idle time | `DB_MAX_CONN_IDLE_TIME` | `5m` | `5m` |
| Ingester reserved floor | `DB_INGESTER_RESERVED_CONNS` | `5` | $F_{\text{ingest}} = \max(2, \lfloor 0.25 \times N \rfloor)$ (`< N`) |
| Shared non-ingester cap | *(derived)* | `15` | $S_{\text{shared}} = N - F_{\text{ingest}}$ |
| HTTP read API cap | `DB_API_READ_MAX_CONNS` | `10` | $\min(S_{\text{shared}}, \max(1, \lfloor 0.50 \times N \rfloor))$ |
| GraphQL cap | `DB_GRAPHQL_MAX_CONNS` | `6` | $\min(S_{\text{shared}}, \max(1, \lfloor 0.30 \times N \rfloor))$ |
| Subscription / webhook cap | `DB_SUBSCRIPTION_MAX_CONNS` | `4` | $\min(S_{\text{shared}}, \max(1, \lfloor 0.20 \times N \rfloor))$ |
| Background maintenance cap | `DB_MAINTENANCE_MAX_CONNS` | `3` | $\min(S_{\text{shared}}, \max(1, \lfloor 0.15 \times N \rfloor))$ |
| Non-ingester acquire timeout | `DB_POOL_ACQUIRE_TIMEOUT` | `50ms` | Bounded fast-fail wait before returning `503` |
| Maintenance pressure threshold | `DB_POOL_PRESSURE_THRESHOLD` | `0.75` | Shared or total pool utilization ratio (`0.0–1.0`) |

### B. Reserved Ingester Floor & Two-Level Semaphore Isolation

`store.PoolBudget` enforces a strict two-level semaphore invariant:

1. Every non-ingester subsystem (`api_read`, `graphql`, `subscription`, `maintenance`) must acquire both:
   - a permit from its per-subsystem semaphore (`subsystemSem[sub]`), and
   - a permit from the shared non-ingester semaphore (`sharedSem`, capacity $S_{\text{shared}} = N - F_{\text{ingest}}$), before acquiring a slot in `totalSem` ($N$).
2. `SubsystemIngester` acquires directly from `totalSem` ($N$). Because non-ingester subsystems combined can hold at most $N - F_{\text{ingest}}$ permits in `totalSem`, at least $F_{\text{ingest}}$ connections are **mathematically guaranteed** to remain immediately available to the ingester even when all read, GraphQL, subscription, and maintenance queues are 100% saturated.
3. Context-level permit tracking makes `PoolBudget.Acquire` re-entrant on the same request context so nested store calls never deadlock or double-count permits.

### C. Fast-Fail Backpressure (`503 Service Unavailable` + `Retry-After`)

When a non-ingester subsystem exhausts either its own cap (`reason="subsystem_cap"`) or the shared non-ingester ceiling (`reason="shared_cap"`) and cannot acquire a permit within `DB_POOL_ACQUIRE_TIMEOUT` (`50ms` default):

- `PoolBudget.Acquire` fails fast with `*store.PoolBudgetExhaustedError` (`errors.Is(err, store.ErrPoolBudgetExhausted)`).
- The HTTP API (`writePoolBudgetExhausted` in `internal/api/handlers.go`) and GraphQL handler (`runQuery` in `internal/api/graphql/server.go`) translate `store.ErrPoolBudgetExhausted` into a retriable `503 Service Unavailable` response with `Retry-After: 1` instead of queueing callers until `API_QUERY_TIMEOUT` (`25s`).

### D. Background Maintenance Yielding Under Pool Pressure

`PoolBudget.ShouldYield(job)` implements the maintenance yield controller interface consumed by `pruner.Pruner`, `archive.Archiver`, and `audit.Auditor`:

- Returns `(true, "pool_pressure")` whenever the ingester has waiting acquisitions (`ingester_waiters > 0`), shared non-ingester utilization reaches `DB_POOL_PRESSURE_THRESHOLD` (`0.75`), total pool utilization reaches `0.75`, or live `pgxpool.Stat` utilization reaches `0.75`.
- Enforces `MaxMaintenanceDeferral` (`15m` default) so a continuously deferred job is granted one bounded pass (`(false, "max_deferral_reached")`) and cannot starve indefinitely.

### E. Pool Budget Observability Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `sorotrail_db_pool_budget_in_use` | GaugeVec | `subsystem` | Permits currently held by each subsystem (`ingester`, `api_read`, `graphql`, `subscription`, `maintenance`) |
| `sorotrail_db_pool_budget_limit` | GaugeVec | `subsystem` | Configured maximum permits per subsystem |
| `sorotrail_db_pool_budget_utilization_ratio` | GaugeVec | `subsystem` | Current utilization ratio (`in_use / limit`) per subsystem |
| `sorotrail_db_pool_budget_wait_duration_seconds` | HistogramVec | `subsystem`, `outcome` | Time spent waiting for a pool budget permit (`acquired`, `rejected`, `canceled`) |
| `sorotrail_db_pool_budget_rejections_total` | CounterVec | `subsystem`, `reason` | Total fast-fail rejections due to budget exhaustion (`subsystem_cap`, `shared_cap`) |
| `sorotrail_db_pool_maintenance_yields_total` | CounterVec | `job`, `reason` | Total times background maintenance yielded due to pool pressure |
| `sorotrail_db_pool_connections` | GaugeVec | `state` | Live connection counts (`acquired`, `idle`, `total`, `max`, `ingester_reserved`, `shared_cap`) |

---

## 8. Database Query Cost Attribution (`#1278`)

SoroTrail attributes every database operation to its originating HTTP route pattern or background worker (`ingester`, `pruner`, `auditor`, `archiver`, `reconciler`, `webhook`) and an opaque tenant bucket (`default`, `t_<8hex>`, or `other`, capped at `MaxTrackedTenants = 64`):

1. **Prometheus Cost & Query Counters (`internal/metrics/metrics.go`)**:
   - `sorotrail_db_attributed_cost_seconds_total{originator, operation, tenant}`: cumulative database wall-clock execution time in seconds.
   - `sorotrail_db_attributed_queries_total{originator, operation, tenant, outcome}`: total store queries by originator, operation, opaque tenant bucket, and outcome (`success` | `error`).
2. **PostgreSQL Statement & Session Attribution (`internal/store/attribution.go`, `internal/store/postgres.go`)**:
   - Every guarded transaction sets a transaction-local `application_name` (`<originator>:<tenant>`) prefixed with a SQL comment `/*originator=...,tenant=...,op=...*/` so `pg_stat_activity` and `pg_stat_statements` join database load back to the application view without leaking raw contract IDs or tenant secrets.
3. **Slow-Query Log Attribution (`internal/store/guarded_store.go`)**:
   - Slow queries crossing `SlowQueryThreshold` log `originator`, `tenant`, `query`, `duration`, and `request_id`.
4. **Measured Overhead**:
   - Context attribution lookup + opaque tenant hashing + Prometheus counter update adds **< 250 ns/op** (`< 0.03%` of a ~1 ms database query).



