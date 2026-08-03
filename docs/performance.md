# SoroTrail Performance & Benchmarks

This document describes the methodology, benchmark results, identified bottlenecks, and production guidance for SoroTrail's ingestion pipeline and query engine.

---

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
